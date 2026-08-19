// Package sender implements background streaming and batching of wagon records to S3.
package sender

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/segmentio/ksuid"
	"github.com/udhos/consist/wagon"
)

// Result signals the outcome of a batch write to storage.
// LastSeq is the highest sequence number included in the batch.
// All sequence numbers up to LastSeq are confirmed saved if Err is nil.
type Result struct {
	LastSeq uint64
	Err     error
}

// S3Client abstracts the minimal AWS S3 API methods required by Sender.
type S3Client interface {
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// Options configures the Sender instance, target S3 storage, and batch limits.
type Options struct {
	Client           S3Client      // Minimal S3 client interface (required)
	Bucket           string        // Target S3 bucket name (required)
	Prefix           string        // Target key prefix (optional)
	MaxBatchBytes    int           // Max bytes per batch before flush (default: 100MB)
	MaxBatchTime     time.Duration // Max duration a batch can remain open before flush (default: 1s)
	MaxClientSilence time.Duration // Max inactivity duration after Send() before flush (default: 500ms)
	MinPartBytes     int           // Min bytes per S3 part upload (default: 10MB)
}

// Sender batches and streams messages to storage in the background.
type Sender struct {
	options     Options
	results     chan Result
	seq         uint64
	batchBuf    *bytes.Buffer
	bufPool     sync.Pool
	readBuf     bytes.Buffer
	encoder     *wagon.Encoder
	batchStart  time.Time
	lastSend    time.Time
	timerStop   chan struct{}
	timerWake   chan struct{}
	timerDone   chan struct{}
	uploadID    string
	key         string
	completed   []types.CompletedPart
	partNum     int32
	totalBatchB int
	uploadWG    sync.WaitGroup
	uploadGate  sync.RWMutex
	uploadErr   error
	stateMu     sync.Mutex
	flushing    bool
	flushDone   chan struct{}
	closeOnce   sync.Once
	closeErr    error
	closed      chan struct{}
}

// newBuffer returns a pooled batch buffer, avoiding a fresh allocation for
// every part. Freshly created buffers are pre-sized to MinPartBytes so they
// grow once instead of repeatedly doubling as messages are encoded into them.
func (s *Sender) newBuffer() *bytes.Buffer {
	if v := s.bufPool.Get(); v != nil {
		return v.(*bytes.Buffer)
	}
	buf := &bytes.Buffer{}
	buf.Grow(s.options.MinPartBytes)
	return buf
}

// releaseBuffer returns a drained batch buffer to the pool for reuse.
func (s *Sender) releaseBuffer(buf *bytes.Buffer) {
	buf.Reset()
	s.bufPool.Put(buf)
}

// readerLen reports the number of unread bytes remaining in r, for reader
// types that can report it without consuming data. Used to avoid buffering
// a copy of the payload when the length is already known upfront.
func readerLen(r io.Reader) (int, bool) {
	switch v := r.(type) {
	case *bytes.Reader:
		return v.Len(), true
	case *bytes.Buffer:
		return v.Len(), true
	case *strings.Reader:
		return v.Len(), true
	default:
		return 0, false
	}
}

// NewSender creates a new Sender instance with the given configuration options.
func NewSender(opts Options) (*Sender, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("missing required S3Client")
	}
	if opts.Bucket == "" {
		return nil, fmt.Errorf("missing required Bucket")
	}
	if opts.MaxBatchBytes <= 0 {
		opts.MaxBatchBytes = 100 * 1024 * 1024 // 100 MB default
	}
	if opts.MaxBatchTime <= 0 {
		opts.MaxBatchTime = 1 * time.Second // 1s default
	}
	if opts.MaxClientSilence <= 0 {
		opts.MaxClientSilence = 500 * time.Millisecond // 500ms default
	}
	if opts.MinPartBytes <= 0 {
		opts.MinPartBytes = 10 * 1024 * 1024 // 10 MB default multipart trigger size: smaller parts sustain more concurrent in-flight uploads and roughly double measured throughput versus 25MB
	}

	s := &Sender{
		options:   opts,
		results:   make(chan Result, 100),
		timerStop: make(chan struct{}),
		timerWake: make(chan struct{}, 1),
		timerDone: make(chan struct{}),
		closed:    make(chan struct{}),
	}

	s.batchBuf = s.newBuffer()
	enc, err := wagon.NewEncoder(s.batchBuf)
	if err != nil {
		return nil, fmt.Errorf("create wagon encoder: %w", err)
	}
	s.encoder = enc

	go s.timerLoop()

	return s, nil
}

func (s *Sender) timerLoop() {
	defer close(s.timerDone)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		s.stateMu.Lock()
		var timerC <-chan time.Time
		if s.batchBuf.Len() > 0 {
			deadline := s.lastSend.Add(s.options.MaxClientSilence)
			batchDeadline := s.batchStart.Add(s.options.MaxBatchTime)
			if !s.batchStart.IsZero() && batchDeadline.Before(deadline) {
				deadline = batchDeadline
			}
			remaining := max(time.Until(deadline), 0)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(remaining)
			timerC = timer.C
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		s.stateMu.Unlock()

		select {
		case <-s.timerStop:
			return
		case <-s.timerWake:
		case <-timerC:
			s.stateMu.Lock()
			now := time.Now()
			if s.batchBuf.Len() > 0 {
				timeExceeded := !s.batchStart.IsZero() && now.Sub(s.batchStart) >= s.options.MaxBatchTime
				silenceExceeded := !s.lastSend.IsZero() && now.Sub(s.lastSend) >= s.options.MaxClientSilence

				if timeExceeded || silenceExceeded {
					s.flush()
				}
			}
			s.stateMu.Unlock()
		}
	}
}

// Send reads payload from r, encodes it as a wagon record into the batch buffer,
// and returns its assigned sequence ID.
// Send is safe to call concurrently; encoding and batch state are serialized,
// while independent multipart uploads may proceed concurrently.
func (s *Sender) Send(r io.Reader) (uint64, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	for s.flushing {
		done := s.flushDone
		s.stateMu.Unlock()
		<-done
		s.stateMu.Lock()
	}
	select {
	case <-s.closed:
		return 0, fmt.Errorf("sender is closed")
	default:
	}

	// Fast path: readers with a cheaply-known remaining length (e.g.
	// *bytes.Reader, *bytes.Buffer, *strings.Reader) are streamed directly
	// into the batch buffer, skipping the readBuf copy used by the
	// general io.Reader path below.
	if dataLen, ok := readerLen(r); ok {
		now := time.Now()
		if s.totalBatchB == 0 && s.batchBuf.Len() == 0 {
			s.batchStart = now
		}
		s.lastSend = now

		s.seq++

		if err := s.encoder.EncodeReader(wagon.Message{}, r, dataLen); err != nil {
			return 0, fmt.Errorf("encode wagon record: %w", err)
		}
	} else {
		s.readBuf.Reset()
		if _, err := io.Copy(&s.readBuf, r); err != nil {
			return 0, fmt.Errorf("read payload: %w", err)
		}

		now := time.Now()
		if s.totalBatchB == 0 && s.batchBuf.Len() == 0 {
			s.batchStart = now
		}
		s.lastSend = now

		s.seq++
		msg := wagon.Message{
			Data: s.readBuf.Bytes(),
		}

		if err := s.encoder.Encode(msg); err != nil {
			return 0, fmt.Errorf("encode wagon record: %w", err)
		}
	}

	// If current buffer reaches MinPartBytes, upload an S3 part
	if s.batchBuf.Len() >= s.options.MinPartBytes {
		if err := s.uploadCurrentPart(context.Background()); err != nil {
			return s.seq, fmt.Errorf("upload part: %w", err)
		}
	}

	// If total accumulated batch size reaches MaxBatchBytes, flush batch
	if s.totalBatchB+s.batchBuf.Len() >= s.options.MaxBatchBytes {
		s.flush()
	}
	select {
	case s.timerWake <- struct{}{}:
	default:
	}

	return s.seq, nil
}

func (s *Sender) ensureMultipartStarted(ctx context.Context) error {
	if s.uploadID != "" {
		return nil
	}
	s.key = batchObjectKey(s.options.Prefix, time.Now())
	out, err := s.options.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &s.options.Bucket,
		Key:    &s.key,
	})
	if err != nil {
		return err
	}
	if out.UploadId != nil {
		s.uploadID = *out.UploadId
	}
	return nil
}

func batchObjectKey(prefix string, now time.Time) string {
	now = now.UTC()
	base := strings.Trim(prefix, "/")
	if base == "" {
		return fmt.Sprintf("%s/%s/%s/%s/%s.batch",
			now.Format("2006-01"),
			now.Format("02"),
			now.Format("15"),
			now.Format("04"),
			randomBatchSuffix(),
		)
	}

	return fmt.Sprintf("%s/%s/%s/%s/%s/%s.batch",
		base,
		now.Format("2006-01"),
		now.Format("02"),
		now.Format("15"),
		now.Format("04"),
		randomBatchSuffix(),
	)
}

func randomBatchSuffix() string {
	return ksuid.New().String()
}

// uploadCurrentPart hands the accumulated batch buffer off for upload and
// returns immediately without waiting for the S3 round trip. The filled
// buffer is swapped out for a pooled one instead of being copied, so this
// no longer does an O(part size) memcpy while stateMu is held. The actual
// network call runs in a separate goroutine (see uploadPartAsync) so the
// producer that filled the part never blocks on network I/O; upload
// concurrency is therefore not limited by the number of producer goroutines.
// Callers must hold stateMu; it stays held for the caller's remaining work.
func (s *Sender) uploadCurrentPart(ctx context.Context) error {
	if s.batchBuf.Len() == 0 {
		return nil
	}
	if err := s.ensureMultipartStarted(ctx); err != nil {
		return err
	}

	s.partNum++
	partNum := s.partNum
	filled := s.batchBuf
	s.totalBatchB += filled.Len()
	s.batchBuf = s.newBuffer()
	s.encoder.ResetWriter(s.batchBuf)
	s.uploadGate.RLock()
	s.uploadWG.Add(1)

	go s.uploadPartAsync(ctx, partNum, filled)

	return nil
}

// uploadPartAsync performs the S3 UploadPart call and records its outcome.
// Runs without stateMu held except while updating shared completion state.
// buf is returned to the pool once the SDK is done reading it.
func (s *Sender) uploadPartAsync(ctx context.Context, partNum int32, buf *bytes.Buffer) {
	defer s.uploadGate.RUnlock()
	defer s.uploadWG.Done()

	out, err := s.options.Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     &s.options.Bucket,
		Key:        &s.key,
		UploadId:   &s.uploadID,
		PartNumber: &partNum,
		Body:       bytes.NewReader(buf.Bytes()),
	})

	s.releaseBuffer(buf)

	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if err != nil {
		if s.uploadErr == nil {
			s.uploadErr = err
		}
		return
	}

	s.completed = append(s.completed, types.CompletedPart{
		ETag:       out.ETag,
		PartNumber: &partNum,
	})
}

func (s *Sender) waitForUploads() {
	s.stateMu.Unlock()
	s.uploadGate.Lock()
	s.uploadWG.Wait()
	s.uploadGate.Unlock()
	s.stateMu.Lock()
}

func (s *Sender) flush() {
	if s.flushing {
		return
	}
	_ = s.flushWithContext(context.Background())
}

// Results returns a read-only channel delivering batch processing outcomes.
func (s *Sender) Results() <-chan Result {
	return s.results
}

// Close flushes any remaining buffered messages and cleans up resources.
func (s *Sender) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		close(s.timerStop)
		<-s.timerDone
		s.stateMu.Lock()
		defer s.stateMu.Unlock()
		if s.totalBatchB > 0 || s.batchBuf.Len() > 0 {
			s.closeErr = s.flushWithContext(ctx)
		}
		close(s.results)
		close(s.closed)
	})
	return s.closeErr
}

func (s *Sender) flushWithContext(ctx context.Context) error {
	if s.totalBatchB == 0 && s.batchBuf.Len() == 0 {
		return nil
	}
	s.flushing = true
	s.flushDone = make(chan struct{})
	defer func() {
		s.flushing = false
		close(s.flushDone)
		s.flushDone = nil
	}()

	var uploadErr error
	if s.batchBuf.Len() > 0 {
		uploadErr = s.uploadCurrentPart(ctx)
	}
	s.waitForUploads()
	if uploadErr == nil {
		uploadErr = s.uploadErr
	}
	if uploadErr == nil && s.uploadID != "" {
		sort.Slice(s.completed, func(i, j int) bool {
			return *s.completed[i].PartNumber < *s.completed[j].PartNumber
		})
		_, uploadErr = s.options.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   &s.options.Bucket,
			Key:      &s.key,
			UploadId: &s.uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: s.completed,
			},
		})
	}
	if uploadErr != nil && s.uploadID != "" {
		_, _ = s.options.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &s.options.Bucket,
			Key:      &s.key,
			UploadId: &s.uploadID,
		})
	}

	s.results <- Result{LastSeq: s.seq, Err: uploadErr}

	s.batchBuf.Reset()
	s.encoder.Reset(s.batchBuf)
	s.uploadID = ""
	s.key = ""
	s.completed = nil
	s.partNum = 0
	s.totalBatchB = 0
	s.uploadErr = nil
	s.batchStart = time.Time{}
	s.lastSend = time.Time{}
	return uploadErr
}
