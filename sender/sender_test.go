package sender_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/udhos/consist/sender"
	"github.com/udhos/consist/wagon"
)

// mockS3Client provides a simple mock implementation of sender.S3Client for testing.
type mockS3Client struct {
	sender.S3Client
	uploadedParts        [][]byte
	uploadedPartBytes    [][]byte
	uploadedBytes        [][]byte
	injectError          error
	keys                 []string
	completedPartNumbers [][]int32
}

type blockingMultipartClient struct {
	sender.S3Client
	partStarted  chan int32
	releasePart1 chan struct{}
}

type flushRaceMultipartClient struct {
	sender.S3Client
	partStarted  chan int32
	releasePart1 chan struct{}
	releasePart2 chan struct{}
}

func (m *blockingMultipartClient) CreateMultipartUpload(_ context.Context, _ *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	uploadID := "blocking-upload-id"
	return &s3.CreateMultipartUploadOutput{UploadId: &uploadID}, nil
}

func (m *blockingMultipartClient) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	partNumber := *params.PartNumber
	m.partStarted <- partNumber
	if partNumber == 1 {
		<-m.releasePart1
	}
	etag := "mock-etag"
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (m *blockingMultipartClient) CompleteMultipartUpload(_ context.Context, _ *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *blockingMultipartClient) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (m *flushRaceMultipartClient) CreateMultipartUpload(_ context.Context, _ *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	uploadID := "flush-race-upload-id"
	return &s3.CreateMultipartUploadOutput{UploadId: &uploadID}, nil
}

func (m *flushRaceMultipartClient) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	partNumber := *params.PartNumber
	m.partStarted <- partNumber
	if partNumber == 1 {
		<-m.releasePart1
	}
	if partNumber == 2 {
		<-m.releasePart2
	}
	etag := "flush-race-etag"
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (m *flushRaceMultipartClient) CompleteMultipartUpload(_ context.Context, _ *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *flushRaceMultipartClient) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (m *mockS3Client) CreateMultipartUpload(_ context.Context, params *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if m.injectError != nil {
		return nil, m.injectError
	}
	if params != nil && params.Key != nil {
		m.keys = append(m.keys, *params.Key)
	}
	uploadID := "mock-upload-id"
	m.uploadedParts = nil
	return &s3.CreateMultipartUploadOutput{UploadId: &uploadID}, nil
}

func (m *mockS3Client) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if m.injectError != nil {
		return nil, m.injectError
	}
	etag := "mock-etag"
	if params.Body != nil {
		buf, _ := io.ReadAll(params.Body)
		m.uploadedParts = append(m.uploadedParts, buf)
		m.uploadedPartBytes = append(m.uploadedPartBytes, buf)
	}
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (m *mockS3Client) CompleteMultipartUpload(_ context.Context, params *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if m.injectError != nil {
		return nil, m.injectError
	}
	var partNumbers []int32
	if params.MultipartUpload != nil {
		for _, part := range params.MultipartUpload.Parts {
			if part.PartNumber != nil {
				partNumbers = append(partNumbers, *part.PartNumber)
			}
		}
	}
	m.completedPartNumbers = append(m.completedPartNumbers, partNumbers)
	var fullFile []byte
	for _, part := range m.uploadedParts {
		fullFile = append(fullFile, part...)
	}
	m.uploadedBytes = append(m.uploadedBytes, fullFile)
	m.uploadedParts = nil
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func TestSender_CompletedPartsAreOrdered(t *testing.T) {
	mockClient := &mockS3Client{}
	s, err := sender.NewSender(sender.Options{
		Client:        mockClient,
		Bucket:        "my-bucket",
		MinPartBytes:  20,
		MaxBatchBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	for range 3 {
		if _, err := s.Send(bytes.NewReader(bytes.Repeat([]byte("x"), 100))); err != nil {
			t.Fatalf("unexpected send error: %v", err)
		}
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if len(mockClient.completedPartNumbers) != 1 {
		t.Fatalf("expected one completed multipart upload, got %d", len(mockClient.completedPartNumbers))
	}
	if len(mockClient.completedPartNumbers[0]) != 3 {
		t.Fatalf("expected three completed parts, got %d", len(mockClient.completedPartNumbers[0]))
	}
	for i, partNumber := range mockClient.completedPartNumbers[0] {
		expected := int32(i + 1)
		if partNumber != expected {
			t.Fatalf("expected part %d at position %d, got %d", expected, i, partNumber)
		}
	}
}

func TestSender_UploadsMultipartPartsConcurrently(t *testing.T) {
	mockClient := &blockingMultipartClient{
		partStarted:  make(chan int32, 2),
		releasePart1: make(chan struct{}),
	}
	s, err := sender.NewSender(sender.Options{
		Client:        mockClient,
		Bucket:        "my-bucket",
		MinPartBytes:  1,
		MaxBatchBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, sendErr := s.Send(bytes.NewReader([]byte("first")))
		firstDone <- sendErr
	}()

	select {
	case partNumber := <-mockClient.partStarted:
		if partNumber != 1 {
			t.Fatalf("expected first upload to be part 1, got %d", partNumber)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first part upload")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, sendErr := s.Send(bytes.NewReader([]byte("second")))
		secondDone <- sendErr
	}()

	select {
	case partNumber := <-mockClient.partStarted:
		if partNumber != 2 {
			t.Errorf("expected concurrent upload to be part 2, got %d", partNumber)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("part 2 did not start while part 1 was blocked; uploads are sequential")
	}

	close(mockClient.releasePart1)
	if err := <-firstDone; err != nil {
		t.Errorf("first send failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Errorf("second send failed: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestSender_DoesNotAppendPartDuringFinalization(t *testing.T) {
	mockClient := &flushRaceMultipartClient{
		partStarted:  make(chan int32, 3),
		releasePart1: make(chan struct{}),
		releasePart2: make(chan struct{}),
	}
	s, err := sender.NewSender(sender.Options{
		Client:           mockClient,
		Bucket:           "my-bucket",
		MinPartBytes:     100,
		MaxBatchBytes:    1024 * 1024,
		MaxBatchTime:     time.Hour,
		MaxClientSilence: time.Hour,
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, sendErr := s.Send(bytes.NewReader(bytes.Repeat([]byte("a"), 200)))
		firstDone <- sendErr
	}()
	if partNumber := <-mockClient.partStarted; partNumber != 1 {
		t.Fatalf("expected first part, got %d", partNumber)
	}

	if _, err := s.Send(bytes.NewReader([]byte("short"))); err != nil {
		t.Fatalf("second send: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.Close(context.Background())
	}()
	if partNumber := <-mockClient.partStarted; partNumber != 2 {
		t.Fatalf("expected short final part 2, got %d", partNumber)
	}

	thirdDone := make(chan error, 1)
	go func() {
		_, sendErr := s.Send(bytes.NewReader(bytes.Repeat([]byte("c"), 200)))
		thirdDone <- sendErr
	}()
	select {
	case partNumber := <-mockClient.partStarted:
		t.Fatalf("created part %d while finalization was in progress", partNumber)
	case <-time.After(100 * time.Millisecond):
	}

	close(mockClient.releasePart2)
	close(mockClient.releasePart1)
	if err := <-firstDone; err != nil {
		t.Fatalf("first send: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := <-thirdDone; err == nil {
		t.Fatal("expected concurrent send to be rejected after Close")
	}
}

func TestSender_MultipartPrefixOnlyOnFirstPart(t *testing.T) {
	mockClient := &mockS3Client{}
	s, err := sender.NewSender(sender.Options{
		Client:        mockClient,
		Bucket:        "my-bucket",
		MinPartBytes:  20,
		MaxBatchBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	for range 3 {
		if _, err := s.Send(bytes.NewReader(bytes.Repeat([]byte("x"), 100))); err != nil {
			t.Fatalf("unexpected send error: %v", err)
		}
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if len(mockClient.uploadedPartBytes) != 3 {
		t.Fatalf("expected three uploaded parts, got %d", len(mockClient.uploadedPartBytes))
	}
	if !bytes.HasPrefix(mockClient.uploadedPartBytes[0], []byte("w1:")) {
		t.Fatalf("expected first part to start with w1:, got %q", mockClient.uploadedPartBytes[0][:min(3, len(mockClient.uploadedPartBytes[0]))])
	}
	for i, part := range mockClient.uploadedPartBytes[1:] {
		if bytes.HasPrefix(part, []byte("w1:")) {
			t.Errorf("part %d unexpectedly starts with w1:", i+2)
		}
	}
}

func (m *mockS3Client) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	m.uploadedParts = nil
	return &s3.AbortMultipartUploadOutput{}, nil
}

func TestSender_UsesDocumentedBatchKeyStructure(t *testing.T) {
	mockClient := &mockS3Client{}
	s, err := sender.NewSender(sender.Options{
		Client: mockClient,
		Bucket: "my-bucket",
		Prefix: "my-prefix",
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	if _, err := s.Send(bytes.NewReader([]byte("hello world"))); err != nil {
		t.Fatalf("unexpected error sending payload: %v", err)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if len(mockClient.keys) == 0 {
		t.Fatal("expected at least one S3 key to be created")
	}

	pattern := regexp.MustCompile(`^my-prefix/\d{4}-\d{2}/\d{2}/\d{2}/\d{2}/[A-Za-z0-9]+\.batch$`)
	if !pattern.MatchString(mockClient.keys[0]) {
		t.Fatalf("expected S3 key to match documented batch structure, got %q", mockClient.keys[0])
	}
}

func TestSender_BasicUsage(t *testing.T) {
	mockClient := &mockS3Client{}
	s, err := sender.NewSender(sender.Options{
		Client: mockClient,
		Bucket: "my-bucket",
		Prefix: "my-prefix",
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	// 1. Send first message payload
	msg1 := bytes.NewReader([]byte("hello world"))
	seq1, err := s.Send(msg1)
	if err != nil {
		t.Fatalf("unexpected error sending msg1: %v", err)
	}
	if seq1 == 0 {
		t.Errorf("expected non-zero sequence ID for first message, got %d", seq1)
	}

	// 2. Send second message payload
	msg2 := bytes.NewReader([]byte("foo bar"))
	seq2, err := s.Send(msg2)
	if err != nil {
		t.Fatalf("unexpected error sending msg2: %v", err)
	}
	if seq2 != seq1+1 {
		t.Errorf("expected monotonic sequence ID %d, got %d", seq1+1, seq2)
	}

	// 3. Clean up sender
	ctx := context.Background()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("unexpected error closing sender: %v", err)
	}
}

func TestSender_AsyncResults(t *testing.T) {
	mockClient := &mockS3Client{}
	s, err := sender.NewSender(sender.Options{
		Client: mockClient,
		Bucket: "my-bucket",
		Prefix: "my-prefix",
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	const totalMsgs = 50
	var lastAckSeq uint64

	// Goroutine consuming asynchronous batch ACK results
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for res := range s.Results() {
			if res.Err != nil {
				t.Errorf("unexpected batch error: %v", res.Err)
				return
			}
			lastAckSeq = res.LastSeq
		}
	}()

	// Producer loop sending messages
	for i := 1; i <= totalMsgs; i++ {
		payload := bytes.NewReader([]byte("message payload"))
		seq, err := s.Send(payload)
		if err != nil {
			t.Fatalf("unexpected send error on msg %d: %v", i, err)
		}
		if seq != uint64(i) {
			t.Fatalf("expected sequence %d, got %d", i, seq)
		}
	}

	// Close sender to flush final batch and close results channel
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	// Wait for ack reader loop to complete
	<-ackDone

	if lastAckSeq != totalMsgs {
		t.Errorf("expected last acknowledged sequence %d, got %d", totalMsgs, lastAckSeq)
	}
}

func TestSender_BatchSizeLimit(t *testing.T) {
	mockClient := &mockS3Client{}

	s, err := sender.NewSender(sender.Options{
		Client:        mockClient,
		Bucket:        "my-bucket",
		Prefix:        "my-prefix",
		MaxBatchBytes: 15,
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	// Channel to receive flushed results synchronously in test
	resultsCh := make(chan sender.Result, 10)

	// Collect results from s.Results()
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for res := range s.Results() {
			resultsCh <- res
		}
	}()

	payload1 := bytes.NewReader([]byte("payload one 12345"))
	seq1, err := s.Send(payload1)
	if err != nil {
		t.Fatalf("send 1: %v", err)
	}

	// Payload 1 (~25 bytes encoded) crosses 15 byte threshold -> triggers immediate flush!
	res1 := <-resultsCh
	if res1.LastSeq != seq1 {
		t.Errorf("expected flushed LastSeq %d, got %d", seq1, res1.LastSeq)
	}

	payload2 := bytes.NewReader([]byte("payload two 67890"))
	seq2, err := s.Send(payload2)
	if err != nil {
		t.Fatalf("send 2: %v", err)
	}

	res2 := <-resultsCh
	if res2.LastSeq != seq2 {
		t.Errorf("expected flushed LastSeq %d, got %d", seq2, res2.LastSeq)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	<-ackDone
}

func TestSender_MaxBatchTime(t *testing.T) {
	mockClient := &mockS3Client{}

	s, err := sender.NewSender(sender.Options{
		Client:           mockClient,
		Bucket:           "my-bucket",
		Prefix:           "my-prefix",
		MaxBatchBytes:    1024 * 1024,
		MaxBatchTime:     100 * time.Millisecond,
		MaxClientSilence: 10 * time.Second, // disable silence check for this test
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	resultsCh := make(chan sender.Result, 10)
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for res := range s.Results() {
			resultsCh <- res
		}
	}()

	// Send single message (well under MaxBatchBytes)
	payload := bytes.NewReader([]byte("time test payload"))
	seq, err := s.Send(payload)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Verify flush happens automatically via timer (within 300ms) without calling Close() or exceeding size
	select {
	case res := <-resultsCh:
		if res.LastSeq != seq {
			t.Errorf("expected LastSeq %d, got %d", seq, res.LastSeq)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch flush on MaxBatchTime")
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	<-ackDone
}

func TestSender_MaxClientSilence(t *testing.T) {
	mockClient := &mockS3Client{}

	// Set short MaxClientSilence (100ms) and long MaxBatchTime (10s)
	s, err := sender.NewSender(sender.Options{
		Client:           mockClient,
		Bucket:           "my-bucket",
		Prefix:           "my-prefix",
		MaxBatchBytes:    1024 * 1024,
		MaxBatchTime:     10 * time.Second, // disable batch time trigger
		MaxClientSilence: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	resultsCh := make(chan sender.Result, 10)
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for res := range s.Results() {
			resultsCh <- res
		}
	}()

	// Send a payload
	payload := bytes.NewReader([]byte("silence test payload"))
	seq, err := s.Send(payload)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Verify flush happens automatically due to client inactivity (within ~300ms)
	select {
	case res := <-resultsCh:
		if res.LastSeq != seq {
			t.Errorf("expected LastSeq %d, got %d", seq, res.LastSeq)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch flush on MaxClientSilence")
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	<-ackDone
}

func TestSender_MultipartPartUploadDoesNotAckBatch(t *testing.T) {
	mockClient := &mockS3Client{}
	s, err := sender.NewSender(sender.Options{
		Client:           mockClient,
		Bucket:           "my-bucket",
		Prefix:           "my-prefix",
		MaxBatchBytes:    1024 * 1024,
		MaxBatchTime:     10 * time.Second,
		MaxClientSilence: 10 * time.Second,
		MinPartBytes:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	resultsCh := make(chan sender.Result, 10)
	go func() {
		for res := range s.Results() {
			resultsCh <- res
		}
	}()

	seq, err := s.Send(bytes.NewReader([]byte("hello world")))
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if len(mockClient.keys) != 1 {
		t.Fatalf("expected multipart upload to start once, got %d keys", len(mockClient.keys))
	}
	if len(mockClient.uploadedParts) != 1 {
		t.Fatalf("expected UploadPart to be called once, got %d uploaded parts in buffer", len(mockClient.uploadedParts))
	}
	if len(mockClient.completedPartNumbers) != 0 {
		t.Fatalf("expected no complete multipart upload before final batch flush, got %d completed uploads", len(mockClient.completedPartNumbers))
	}

	select {
	case res := <-resultsCh:
		t.Fatalf("unexpected batch ack from multipart upload; got LastSeq=%d Err=%v", res.LastSeq, res.Err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case res := <-resultsCh:
		if res.LastSeq != seq {
			t.Fatalf("expected final ack LastSeq %d, got %d", seq, res.LastSeq)
		}
		if res.Err != nil {
			t.Fatalf("unexpected final batch error: %v", res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for final batch ack after Close")
	}
}

func TestSender_ResultsOnlyOnBatchFinalizationConditions(t *testing.T) {
	t.Run("max size", func(t *testing.T) {
		mockClient := &mockS3Client{}
		s, err := sender.NewSender(sender.Options{
			Client:        mockClient,
			Bucket:        "my-bucket",
			Prefix:        "my-prefix",
			MaxBatchBytes: 8,
			MaxBatchTime:  10 * time.Second,
		})
		if err != nil {
			t.Fatalf("unexpected error creating sender: %v", err)
		}
		resultsCh := make(chan sender.Result, 10)
		go func() {
			for res := range s.Results() {
				resultsCh <- res
			}
		}()

		seq, err := s.Send(bytes.NewReader(bytes.Repeat([]byte("x"), 64)))
		if err != nil {
			t.Fatalf("send: %v", err)
		}

		select {
		case res := <-resultsCh:
			if res.LastSeq != seq {
				t.Fatalf("expected LastSeq %d, got %d", seq, res.LastSeq)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for max-size batch ack")
		}
	})

	t.Run("max time", func(t *testing.T) {
		mockClient := &mockS3Client{}
		s, err := sender.NewSender(sender.Options{
			Client:           mockClient,
			Bucket:           "my-bucket",
			Prefix:           "my-prefix",
			MaxBatchBytes:    1024 * 1024,
			MaxBatchTime:     50 * time.Millisecond,
			MaxClientSilence: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("unexpected error creating sender: %v", err)
		}
		resultsCh := make(chan sender.Result, 10)
		go func() {
			for res := range s.Results() {
				resultsCh <- res
			}
		}()

		seq, err := s.Send(bytes.NewReader([]byte("payload")))
		if err != nil {
			t.Fatalf("send: %v", err)
		}

		select {
		case res := <-resultsCh:
			if res.LastSeq != seq {
				t.Fatalf("expected LastSeq %d, got %d", seq, res.LastSeq)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for max-time batch ack")
		}
	})

	t.Run("silence", func(t *testing.T) {
		mockClient := &mockS3Client{}
		s, err := sender.NewSender(sender.Options{
			Client:           mockClient,
			Bucket:           "my-bucket",
			Prefix:           "my-prefix",
			MaxBatchBytes:    1024 * 1024,
			MaxBatchTime:     10 * time.Second,
			MaxClientSilence: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("unexpected error creating sender: %v", err)
		}
		resultsCh := make(chan sender.Result, 10)
		go func() {
			for res := range s.Results() {
				resultsCh <- res
			}
		}()

		seq, err := s.Send(bytes.NewReader([]byte("payload")))
		if err != nil {
			t.Fatalf("send: %v", err)
		}

		select {
		case res := <-resultsCh:
			if res.LastSeq != seq {
				t.Fatalf("expected LastSeq %d, got %d", seq, res.LastSeq)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for silence-triggered batch ack")
		}
	})

	t.Run("close", func(t *testing.T) {
		mockClient := &mockS3Client{}
		s, err := sender.NewSender(sender.Options{
			Client:        mockClient,
			Bucket:        "my-bucket",
			Prefix:        "my-prefix",
			MaxBatchBytes: 1024 * 1024,
			MaxBatchTime:  10 * time.Second,
		})
		if err != nil {
			t.Fatalf("unexpected error creating sender: %v", err)
		}
		resultsCh := make(chan sender.Result, 10)
		go func() {
			for res := range s.Results() {
				resultsCh <- res
			}
		}()

		seq, err := s.Send(bytes.NewReader([]byte("payload")))
		if err != nil {
			t.Fatalf("send: %v", err)
		}

		if err := s.Close(context.Background()); err != nil {
			t.Fatalf("close: %v", err)
		}

		select {
		case res := <-resultsCh:
			if res.LastSeq != seq {
				t.Fatalf("expected LastSeq %d after Close, got %d", seq, res.LastSeq)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for final Close ack")
		}
	})
}

func TestSender_CloseIsIdempotent(t *testing.T) {
	mockClient := &mockS3Client{}
	s, err := sender.NewSender(sender.Options{
		Client: mockClient,
		Bucket: "my-bucket",
		Prefix: "my-prefix",
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	if _, err := s.Send(bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("unexpected send error: %v", err)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("second close should be a no-op: %v", err)
	}

	if _, err := s.Send(bytes.NewReader([]byte("after close"))); err == nil {
		t.Fatal("expected Send after Close to fail")
	}
}

func TestSender_CloseReturnsFinalFlushError(t *testing.T) {
	closeErr := io.ErrClosedPipe
	mockClient := &mockS3Client{injectError: closeErr}
	s, err := sender.NewSender(sender.Options{
		Client: mockClient,
		Bucket: "my-bucket",
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	seq, err := s.Send(bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatalf("unexpected send error: %v", err)
	}

	if err := s.Close(context.Background()); err != closeErr {
		t.Fatalf("expected Close error %v, got %v", closeErr, err)
	}

	result := <-s.Results()
	if result.LastSeq != seq {
		t.Fatalf("expected result sequence %d, got %d", seq, result.LastSeq)
	}
	if result.Err != closeErr {
		t.Fatalf("expected result error %v, got %v", closeErr, result.Err)
	}
}

func TestSender_ValidWagonFormat(t *testing.T) {
	mockClient := &mockS3Client{}

	s, err := sender.NewSender(sender.Options{
		Client: mockClient,
		Bucket: "my-bucket",
		Prefix: "my-prefix",
	})
	if err != nil {
		t.Fatalf("unexpected error creating sender: %v", err)
	}

	messages := [][]byte{
		[]byte("first test payload"),
		[]byte("second test payload"),
		[]byte("third test payload"),
	}

	for _, msg := range messages {
		if _, err := s.Send(bytes.NewReader(msg)); err != nil {
			t.Fatalf("unexpected send error: %v", err)
		}
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if len(mockClient.uploadedBytes) != 1 {
		t.Fatalf("expected 1 uploaded batch file, got %d", len(mockClient.uploadedBytes))
	}

	uploadedData := mockClient.uploadedBytes[0]

	// Parse uploaded bytes using wagon.NewDecoder
	dec, err := wagon.NewDecoder(bytes.NewReader(uploadedData))
	if err != nil {
		t.Fatalf("failed to decode uploaded wagon format: %v", err)
	}

	var decodedMsgs []wagon.Message
	for {
		var msg wagon.Message
		err := dec.Decode(&msg)
		if err != nil {
			if io.EOF == err || errorsIsEOF(err) {
				break
			}
			t.Fatalf("error decoding message: %v", err)
		}
		decodedMsgs = append(decodedMsgs, msg)
	}

	if len(decodedMsgs) != len(messages) {
		t.Fatalf("expected %d decoded messages, got %d", len(messages), len(decodedMsgs))
	}

	for i, expected := range messages {
		if !bytes.Equal(decodedMsgs[i].Data, expected) {
			t.Errorf("msg %d mismatch: expected %q, got %q", i, string(expected), string(decodedMsgs[i].Data))
		}
	}
}

func errorsIsEOF(err error) bool {
	return err != nil && err.Error() == "EOF"
}

/*
$ go test -bench='^BenchmarkSender_Send_10k_10KB$' -benchmem -count=5 ./sender
goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: 13th Gen Intel(R) Core(TM) i7-1360P
BenchmarkSender_Send_10k_10KB-16    	       8	 148058207 ns/op	 675.41 MB/s	847424720 B/op	   10442 allocs/op
BenchmarkSender_Send_10k_10KB-16    	       8	 141890268 ns/op	 704.77 MB/s	847424752 B/op	   10441 allocs/op
BenchmarkSender_Send_10k_10KB-16    	       9	 151272966 ns/op	 661.06 MB/s	847424613 B/op	   10441 allocs/op
BenchmarkSender_Send_10k_10KB-16    	       8	 147615572 ns/op	 677.44 MB/s	847424682 B/op	   10441 allocs/op
BenchmarkSender_Send_10k_10KB-16    	      10	 156430167 ns/op	 639.26 MB/s	847424333 B/op	   10441 allocs/op
PASS
ok  	github.com/udhos/consist/sender	12.409s
*/
func BenchmarkSender_Send_10k_10KB(b *testing.B) {
	mockClient := &mockS3Client{}
	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s, err := sender.NewSender(sender.Options{
			Client:        mockClient,
			Bucket:        "bench-bucket",
			Prefix:        "bench-prefix",
			MaxBatchBytes: 100 * 1024 * 1024, // 100MB
		})
		if err != nil {
			b.Fatalf("create sender: %v", err)
		}

		// Drain results goroutine
		go func() {
			for res := range s.Results() {
				_ = res
			}
		}()

		r := bytes.NewReader(payload)
		for range 10000 {
			r.Reset(payload)
			if _, err := s.Send(r); err != nil {
				b.Fatalf("send: %v", err)
			}
		}

		_ = s.Close(context.Background())
	}

	b.SetBytes(int64(10000 * 10000))
}

/*
$ AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname go test -v -run='^$' -bench='^BenchmarkSender_Send_10k_10KB_AWS$' -benchmem -count=3 ./sender
goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz
BenchmarkSender_Send_10k_10KB_AWS
BenchmarkSender_Send_10k_10KB_AWS-8            1        1326268281 ns/op          75.40 MB/s    141035440 B/op     79700 allocs/op
BenchmarkSender_Send_10k_10KB_AWS-8            1        1358256926 ns/op          73.62 MB/s    133649856 B/op     18004 allocs/op
BenchmarkSender_Send_10k_10KB_AWS-8            1        1415666697 ns/op          70.64 MB/s    143862256 B/op     18576 allocs/op
PASS
ok      github.com/udhos/consist/sender 4.112s
*/
func BenchmarkSender_Send_10k_10KB_AWS(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET to run the real AWS benchmark")
	}

	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		b.Fatalf("load AWS config: %v", err)
	}
	client := s3.NewFromConfig(awsConfig)
	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		s, err := sender.NewSender(sender.Options{
			Client:        client,
			Bucket:        bucket,
			Prefix:        "consist",
			MaxBatchBytes: 100 * 1024 * 1024,
		})
		if err != nil {
			b.Fatalf("create sender: %v", err)
		}

		go func() {
			for res := range s.Results() {
				if res.Err != nil {
					b.Errorf("upload batch: %v", res.Err)
				}
			}
		}()

		r := bytes.NewReader(payload)
		for range 10000 {
			r.Reset(payload)
			if _, err := s.Send(r); err != nil {
				b.Fatalf("send: %v", err)
			}
		}

		if err := s.Close(context.Background()); err != nil {
			b.Fatalf("close sender: %v", err)
		}
	}

	b.SetBytes(int64(10000 * 10000))
}

/*
	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_Send_10k_10KB_AWS(|_PartSizes)$' \
		  -benchtime=5s \
		  -count=3 \
		  -benchmem
*/
func BenchmarkSender_Send_10k_10KB_AWS_PartSizes(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET to run the real AWS benchmark")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatalf("load AWS config: %v", err)
	}
	client := s3.NewFromConfig(awsConfig)
	payload := make([]byte, 10000)
	partSizes := []int{
		5 * 1024 * 1024,
		10 * 1024 * 1024,
		25 * 1024 * 1024,
		50 * 1024 * 1024,
	}

	for _, partSize := range partSizes {
		b.Run(fmt.Sprintf("part_%dMiB", partSize/(1024*1024)), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/part-size",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  partSize,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						b.Errorf("batch upload: %v", result.Err)
					}
				}
			}()

			r := bytes.NewReader(payload)
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Reset(payload)
				if _, err := s.Send(r); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
		})
	}
}

/*
	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		-run='^$' \
		-bench='^BenchmarkSender_SustainedAWS$' \
		-benchtime=30s \
		-count=3 \
		-benchmem

goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz
BenchmarkSender_SustainedAWS-8            240770            139434 ns/op          73.44 MB/s       10471 B/op          1 allocs/op
BenchmarkSender_SustainedAWS-8            259803            142263 ns/op          71.98 MB/s       10478 B/op          1 allocs/op
BenchmarkSender_SustainedAWS-8            247593            140744 ns/op          72.76 MB/s       10498 B/op          1 allocs/op
PASS
ok      github.com/udhos/consist/sender 112.582s
*/
func BenchmarkSender_SustainedAWS(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatal(err)
	}

	s, err := sender.NewSender(sender.Options{
		Client:        s3.NewFromConfig(awsConfig),
		Bucket:        bucket,
		Prefix:        "consist-bench",
		MaxBatchBytes: 100 * 1024 * 1024,
		MinPartBytes:  10 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}

	resultsDone := make(chan struct{})
	go func() {
		defer close(resultsDone)
		for result := range s.Results() {
			if result.Err != nil {
				b.Errorf("batch upload: %v", result.Err)
			}
		}
	}()

	payload := make([]byte, 10*1024)
	reader := bytes.NewReader(payload)

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader.Reset(payload)
		if _, err := s.Send(reader); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	if err := s.Close(ctx); err != nil {
		b.Fatal(err)
	}
	<-resultsDone
}
