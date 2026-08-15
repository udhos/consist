// Package sender implements background streaming and batching of wagon records to S3.
package sender

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	batchBuf    bytes.Buffer
	readBuf     bytes.Buffer
	encoder     *wagon.Encoder
	batchStart  time.Time
	lastSend    time.Time
	timerStop   chan struct{}
	timerDone   chan struct{}
	uploadID    string
	key         string
	completed   []types.CompletedPart
	partNum     int32
	totalBatchB int
	closeOnce   sync.Once
	closed      chan struct{}
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
		opts.MinPartBytes = 10 * 1024 * 1024 // 10 MB default multipart trigger size
	}

	s := &Sender{
		options:   opts,
		results:   make(chan Result, 100),
		timerStop: make(chan struct{}),
		timerDone: make(chan struct{}),
		closed:    make(chan struct{}),
	}

	enc, err := wagon.NewEncoder(&s.batchBuf)
	if err != nil {
		return nil, fmt.Errorf("create wagon encoder: %w", err)
	}
	s.encoder = enc

	go s.timerLoop()

	return s, nil
}

func (s *Sender) timerLoop() {
	defer close(s.timerDone)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.timerStop:
			return
		case <-ticker.C:
			now := time.Now()
			if s.batchBuf.Len() > 0 {
				timeExceeded := !s.batchStart.IsZero() && now.Sub(s.batchStart) >= s.options.MaxBatchTime
				silenceExceeded := !s.lastSend.IsZero() && now.Sub(s.lastSend) >= s.options.MaxClientSilence

				if timeExceeded || silenceExceeded {
					s.flush()
				}
			}
		}
	}
}

// Send reads payload from r, encodes it as a wagon record into the batch buffer,
// and returns its assigned sequence ID.
// Note: Sender is not concurrency-safe and must be used by a single goroutine.
func (s *Sender) Send(r io.Reader) (uint64, error) {
	s.readBuf.Reset()
	if _, err := io.Copy(&s.readBuf, r); err != nil {
		return 0, fmt.Errorf("read payload: %w", err)
	}
	data := s.readBuf.Bytes()

	now := time.Now()
	if s.totalBatchB == 0 && s.batchBuf.Len() == 0 {
		s.batchStart = now
	}
	s.lastSend = now

	s.seq++
	msg := wagon.Message{
		Data: data,
	}

	if err := s.encoder.Encode(msg); err != nil {
		return 0, fmt.Errorf("encode wagon record: %w", err)
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

	return s.seq, nil
}

func (s *Sender) ensureMultipartStarted(ctx context.Context) error {
	if s.uploadID != "" {
		return nil
	}
	s.key = fmt.Sprintf("%s/%d.batch", s.options.Prefix, time.Now().UnixNano())
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

func (s *Sender) uploadCurrentPart(ctx context.Context) error {
	if s.batchBuf.Len() == 0 {
		return nil
	}
	if err := s.ensureMultipartStarted(ctx); err != nil {
		return err
	}

	s.partNum++
	partLen := s.batchBuf.Len()
	payload := s.batchBuf.Bytes()

	out, err := s.options.Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     &s.options.Bucket,
		Key:        &s.key,
		UploadId:   &s.uploadID,
		PartNumber: &s.partNum,
		Body:       bytes.NewReader(payload),
	})
	if err != nil {
		return err
	}

	s.completed = append(s.completed, types.CompletedPart{
		ETag:       out.ETag,
		PartNumber: &s.partNum,
	})

	s.totalBatchB += partLen
	s.batchBuf.Reset()
	s.encoder.Reset(&s.batchBuf)

	return nil
}

func (s *Sender) flush() {
	if s.totalBatchB == 0 && s.batchBuf.Len() == 0 {
		return
	}

	ctx := context.Background()
	var uploadErr error

	// Upload remaining buffer as final part if needed
	if s.batchBuf.Len() > 0 {
		uploadErr = s.uploadCurrentPart(ctx)
	}

	// Complete multipart upload if active
	if uploadErr == nil && s.uploadID != "" {
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

	s.results <- Result{
		LastSeq: s.seq,
		Err:     uploadErr,
	}

	s.batchBuf.Reset()
	s.encoder.Reset(&s.batchBuf)
	s.uploadID = ""
	s.key = ""
	s.completed = nil
	s.partNum = 0
	s.totalBatchB = 0
	s.batchStart = time.Time{}
	s.lastSend = time.Time{}
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
		if s.totalBatchB > 0 || s.batchBuf.Len() > 0 {
			s.flushWithContext(ctx)
		}
		close(s.results)
		close(s.closed)
	})
	return nil
}

func (s *Sender) flushWithContext(ctx context.Context) {
	if s.totalBatchB == 0 && s.batchBuf.Len() == 0 {
		return
	}

	var uploadErr error
	if s.batchBuf.Len() > 0 {
		uploadErr = s.uploadCurrentPart(ctx)
	}
	if uploadErr == nil && s.uploadID != "" {
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
	s.encoder.Reset(&s.batchBuf)
	s.uploadID = ""
	s.key = ""
	s.completed = nil
	s.partNum = 0
	s.totalBatchB = 0
	s.batchStart = time.Time{}
	s.lastSend = time.Time{}
}
