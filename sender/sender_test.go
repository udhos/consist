package sender_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/udhos/consist/sender"
	"github.com/udhos/consist/wagon"
)

// mockS3Client provides a simple mock implementation of sender.S3Client for testing.
// mu guards the fields below since parts now upload concurrently from separate goroutines.
type mockS3Client struct {
	sender.S3Client
	mu                   sync.Mutex
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
	m.mu.Lock()
	defer m.mu.Unlock()
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
		var partNumber int32 = 1
		if params.PartNumber != nil {
			partNumber = *params.PartNumber
		}
		idx := int(partNumber - 1)
		m.mu.Lock()
		// Parts upload concurrently and may complete out of order, so store
		// each part at its part-number slot rather than in completion order.
		for len(m.uploadedParts) <= idx {
			m.uploadedParts = append(m.uploadedParts, nil)
			m.uploadedPartBytes = append(m.uploadedPartBytes, nil)
		}
		m.uploadedParts[idx] = buf
		m.uploadedPartBytes[idx] = buf
		m.mu.Unlock()
	}
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (m *mockS3Client) CompleteMultipartUpload(_ context.Context, params *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadedParts = nil
	return &s3.AbortMultipartUploadOutput{}, nil
}

// uploadedPartsCount safely reads the number of parts uploaded so far. Parts
// now upload in a background goroutine, so callers that need to observe an
// upload must poll this instead of reading the field directly right after
// Send() returns.
func (m *mockS3Client) uploadedPartsCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploadedParts)
}

// waitForCondition polls cond until it returns true or timeout elapses.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not met within timeout")
		}
		time.Sleep(time.Millisecond)
	}
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
	// UploadPart now runs in a background goroutine, so wait for it instead
	// of asserting immediately after Send() returns.
	waitForCondition(t, time.Second, func() bool { return mockClient.uploadedPartsCount() == 1 })
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

// benchS3Client is a minimal S3 mock for benchmarking. Unlike mockS3Client, it
// discards uploaded data instead of retaining it in ever-growing slices, so
// memory/CPU cost stays constant across b.Loop() iterations.
type benchS3Client struct{}

func (*benchS3Client) CreateMultipartUpload(_ context.Context, _ *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	uploadID := "bench-upload-id"
	return &s3.CreateMultipartUploadOutput{UploadId: &uploadID}, nil
}

func (*benchS3Client) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if params.Body != nil {
		_, _ = io.Copy(io.Discard, params.Body)
	}
	etag := "bench-etag"
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (*benchS3Client) CompleteMultipartUpload(_ context.Context, _ *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (*benchS3Client) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

/*
go test -bench='^BenchmarkSender_Send_10k_10KB$' -benchmem ./sender
goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: 13th Gen Intel(R) Core(TM) i7-1360P
BenchmarkSender_Send_10k_10KB-16    	      21	  50836591 ns/op	1967.09 MB/s	184082517 B/op	   10094 allocs/op
PASS
ok  	github.com/udhos/consist/sender	1.696s
*/
func BenchmarkSender_Send_10k_10KB(b *testing.B) {
	mockClient := &benchS3Client{}
	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ReportAllocs()

	for b.Loop() {
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
		  -bench='^BenchmarkSender_Send_10k_10KB_AWS_PartSizes$' \
		  -benchtime=5s \
		  -count=3 \
		  -benchmem

goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_5MiB-8                    32200            184473 ns/op          54.21 MB/s       10855 B/op 2 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_5MiB-8                    33002            190577 ns/op          52.47 MB/s       10824 B/op 2 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_5MiB-8                    37064            184278 ns/op          54.27 MB/s       10763 B/op 2 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_10MiB-8                   42808            136639 ns/op          73.19 MB/s       11129 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_10MiB-8                   45877            129164 ns/op          77.42 MB/s       10883 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_10MiB-8                   44618            133209 ns/op          75.07 MB/s       11010 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_25MiB-8                   64500            115410 ns/op          86.65 MB/s       11118 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_25MiB-8                   63386            116427 ns/op          85.89 MB/s       11310 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_25MiB-8                   65076            112760 ns/op          88.68 MB/s       11015 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_50MiB-8                   87399            112604 ns/op          88.81 MB/s       11553 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_50MiB-8                   86336            114266 ns/op          87.52 MB/s       11696 B/op 1 allocs/op
BenchmarkSender_Send_10k_10KB_AWS_PartSizes/part_50MiB-8                   82276            109479 ns/op          91.34 MB/s       11634 B/op 1 allocs/op
PASS
ok      github.com/udhos/consist/sender 114.869s
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

/*
	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_SustainedAWS_Producers_25MBPart$' \
		  -benchtime=120s \
		  -count=1 \
		  -benchmem

goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_1-8               63614            119392 ns/op          85.77 MB/s       11532 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_1-8               63090            121022 ns/op          84.61 MB/s       11629 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_1-8               64915            118681 ns/op          86.28 MB/s       11706 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_2-8               91594             71061 ns/op         144.10 MB/s       11174 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_2-8               91218             73232 ns/op         139.83 MB/s       11219 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_2-8               90337             70611 ns/op         145.02 MB/s       11330 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_4-8               92848             61016 ns/op         167.82 MB/s       11307 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_4-8               92224             55142 ns/op         185.70 MB/s       11381 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_4-8               92606             58450 ns/op         175.19 MB/s       11335 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_8-8              143473             48463 ns/op         211.29 MB/s       10987 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_8-8              138381             50578 ns/op         202.46 MB/s       11014 B/op          1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_8-8              141328             48006 ns/op         213.31 MB/s       10968 B/op          1 allocs/op
PASS
ok      github.com/udhos/consist/sender 100.163s

# ------------

	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=pulsix-br \
		go test ./sender \
			-run='^$' \
			-bench='^BenchmarkSender_SustainedAWS_Producers_25MBPart$' \
			-benchtime=20s \
			-count=1 \
			-benchmem

goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_8-8            1000000             42613 ns/op         240.30 MB/s         725 B/op 1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_16-8           1000000             42492 ns/op         240.98 MB/s         804 B/op 1 allocs/op
PASS
ok      github.com/udhos/consist/sender 88.087s
*/
func BenchmarkSender_SustainedAWS_Producers_25MBPart(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig)
	payload := make([]byte, 10*1024)

	for _, producers := range []int{8, 16} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/producers",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  25 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultErr := make(chan error, 1)
			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						select {
						case resultErr <- result.Err:
						default:
						}
					}
				}
			}()

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			var wg sync.WaitGroup
			producerErr := make(chan error, producers)
			for producer := range producers {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					reader := bytes.NewReader(payload)
					for i := producer; i < b.N; i += producers {
						reader.Reset(payload)
						if _, err := s.Send(reader); err != nil {
							producerErr <- err
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-producerErr:
				b.Fatal(err)
			default:
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
			select {
			case err := <-resultErr:
				b.Fatal(err)
			default:
			}
		})
	}
}

/*
	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
	    go test ./sender \
	        -run='^$' \
	        -bench='^BenchmarkSender_SustainedAWS_Producers_25MBPart' \
	        -benchtime=20s \
	        -count=1 \
	        -benchmem

goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_8-8             522032             48695 ns/op         210.29 MB/s       10491 B/op 1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_16-8            551214             47637 ns/op         214.96 MB/s       10462 B/op 1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart_CustomTransport/producers_8-8             547948             48074 ns/op         213.00 MB/s       10480 B/op        1 allocs/op
BenchmarkSender_SustainedAWS_Producers_25MBPart_CustomTransport/producers_16-8            540452             47619 ns/op         215.04 MB/s       10492 B/op        1 allocs/op
PASS
ok      github.com/udhos/consist/sender 109.914s
*/
func BenchmarkSender_SustainedAWS_Producers_25MBPart_CustomTransport(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = 1_024
	transport.MaxIdleConnsPerHost = 1_024
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ExpectContinueTimeout = time.Second
	transport.ReadBufferSize = 1 * 1024 * 1024
	transport.WriteBufferSize = 1 * 1024 * 1024

	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithHTTPClient(&http.Client{
		Transport: transport,
	}))
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig)
	payload := make([]byte, 10*1024)

	for _, producers := range []int{8, 16} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/producers",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  25 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultErr := make(chan error, 1)
			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						select {
						case resultErr <- result.Err:
						default:
						}
					}
				}
			}()

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			var wg sync.WaitGroup
			producerErr := make(chan error, producers)
			for producer := range producers {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					reader := bytes.NewReader(payload)
					for i := producer; i < b.N; i += producers {
						reader.Reset(payload)
						if _, err := s.Send(reader); err != nil {
							producerErr <- err
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-producerErr:
				b.Fatal(err)
			default:
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
			select {
			case err := <-resultErr:
				b.Fatal(err)
			default:
			}
		})
	}
}

/*
The AWS SDK v2 defaults RequestChecksumCalculation to WhenSupported, which
makes it compute a checksum (e.g. CRC32) over every request body,
including each UploadPart call, regardless of anything the sender package
does. WhenRequired skips that extra pass over the data unless the
operation truly needs it, so this variant isolates whether that
SDK-level checksum work is limiting sustained AWS throughput.

	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_SustainedAWS_Producers_25MBPart_NoChecksum$' \
		  -benchtime=20s \
		  -count=1 \
		  -benchmem
*/
func BenchmarkSender_SustainedAWS_Producers_25MBPart_NoChecksum(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	payload := make([]byte, 10*1024)

	for _, producers := range []int{8, 16} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/producers",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  25 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultErr := make(chan error, 1)
			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						select {
						case resultErr <- result.Err:
						default:
						}
					}
				}
			}()

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			var wg sync.WaitGroup
			producerErr := make(chan error, producers)
			for producer := range producers {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					reader := bytes.NewReader(payload)
					for i := producer; i < b.N; i += producers {
						reader.Reset(payload)
						if _, err := s.Send(reader); err != nil {
							producerErr <- err
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-producerErr:
				b.Fatal(err)
			default:
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
			select {
			case err := <-resultErr:
				b.Fatal(err)
			default:
			}
		})
	}
}

/*
SigV4 signing computes a full SHA256 hash of every request body (this is
separate from the RequestChecksumCalculation CRC32 feature tested above),
competing for CPU with encoding on the same cores. UNSIGNED-PAYLOAD skips
that hash since S3 already gets payload integrity from TLS. This variant
isolates whether that SHA256 pass is the throughput-limiting factor.

	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_SustainedAWS_Producers_25MBPart_UnsignedPayload$' \
		  -benchtime=20s \
		  -count=1 \
		  -benchmem
*/
func BenchmarkSender_SustainedAWS_Producers_25MBPart_UnsignedPayload(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.APIOptions = append(o.APIOptions, v4signer.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	})
	payload := make([]byte, 10*1024)

	for _, producers := range []int{8, 16} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/producers",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  25 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultErr := make(chan error, 1)
			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						select {
						case resultErr <- result.Err:
						default:
						}
					}
				}
			}()

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			var wg sync.WaitGroup
			producerErr := make(chan error, producers)
			for producer := range producers {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					reader := bytes.NewReader(payload)
					for i := producer; i < b.N; i += producers {
						reader.Reset(payload)
						if _, err := s.Send(reader); err != nil {
							producerErr <- err
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-producerErr:
				b.Fatal(err)
			default:
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
			select {
			case err := <-resultErr:
				b.Fatal(err)
			default:
			}
		})
	}
}

/*
	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_SustainedAWS_Producers_10MBPart$' \
		  -benchtime=20s \
		  -count=2 \
		  -benchmem
*/
func BenchmarkSender_SustainedAWS_Producers_10MBPart(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig)
	payload := make([]byte, 10*1024*1024)

	for _, producers := range []int{8} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/producers",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  25 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultErr := make(chan error, 1)
			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						select {
						case resultErr <- result.Err:
						default:
						}
					}
				}
			}()

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			var wg sync.WaitGroup
			producerErr := make(chan error, producers)
			for producer := range producers {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					reader := bytes.NewReader(payload)
					for i := producer; i < b.N; i += producers {
						reader.Reset(payload)
						if _, err := s.Send(reader); err != nil {
							producerErr <- err
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-producerErr:
				b.Fatal(err)
			default:
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
			select {
			case err := <-resultErr:
				b.Fatal(err)
			default:
			}
		})
	}
}

/*
The SDK's default transport sets ForceAttemptHTTP2, which lets Go's
http.Transport multiplex many concurrent requests over a small number of
TCP connections instead of opening one per request. S3 can effectively
rate-limit per connection, so fewer connections can mean less aggregate
throughput regardless of producer/goroutine count. This variant disables
HTTP/2 negotiation (TLSNextProto) to force one TCP connection per
concurrent request. Result: no measurable difference from HTTP/2 (both
settled around 6-7 concurrent connections and ~235 MB/s), ruling out
HTTP/2 multiplexing as the throughput-limiting factor.

	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_SustainedAWS_Producers_25MBPart_HTTP1$' \
		  -benchtime=20s \
		  -count=1 \
		  -benchmem
*/
func BenchmarkSender_SustainedAWS_Producers_25MBPart_HTTP1(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = false
	transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	transport.MaxIdleConnsPerHost = 64
	transport.MaxConnsPerHost = 0

	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithHTTPClient(&http.Client{
		Transport: transport,
	}))
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.APIOptions = append(o.APIOptions, v4signer.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	})
	payload := make([]byte, 10*1024)

	for _, producers := range []int{8, 16} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/producers",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  25 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultErr := make(chan error, 1)
			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						select {
						case resultErr <- result.Err:
						default:
						}
					}
				}
			}()

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			var wg sync.WaitGroup
			producerErr := make(chan error, producers)
			for producer := range producers {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					reader := bytes.NewReader(payload)
					for i := producer; i < b.N; i += producers {
						reader.Reset(payload)
						if _, err := s.Send(reader); err != nil {
							producerErr <- err
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-producerErr:
				b.Fatal(err)
			default:
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
			select {
			case err := <-resultErr:
				b.Fatal(err)
			default:
			}
		})
	}
}

/*
Tests whether the ~6-7 concurrent connection ceiling observed at 8/16
producers is a natural equilibrium that grows with more offered load, or
a hard limit. Result: 64 producers still settled around 7-8 connections
and ~239 MB/s, no better than 8 or 16 producers - ruling out client-side
concurrency/goroutine count as the limiting factor.

	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_SustainedAWS_Producers_25MBPart_HighConcurrency$' \
		  -benchtime=20s \
		  -count=1 \
		  -benchmem
*/
func BenchmarkSender_SustainedAWS_Producers_25MBPart_HighConcurrency(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.APIOptions = append(o.APIOptions, v4signer.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	})
	payload := make([]byte, 10*1024)

	for _, producers := range []int{64} {
		b.Run(fmt.Sprintf("producers_%d", producers), func(b *testing.B) {
			s, err := sender.NewSender(sender.Options{
				Client:        client,
				Bucket:        bucket,
				Prefix:        "consist-bench/producers",
				MaxBatchBytes: 100 * 1024 * 1024,
				MinPartBytes:  25 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			resultErr := make(chan error, 1)
			resultsDone := make(chan struct{})
			go func() {
				defer close(resultsDone)
				for result := range s.Results() {
					if result.Err != nil {
						select {
						case resultErr <- result.Err:
						default:
						}
					}
				}
			}()

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			var wg sync.WaitGroup
			producerErr := make(chan error, producers)
			for producer := range producers {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					reader := bytes.NewReader(payload)
					for i := producer; i < b.N; i += producers {
						reader.Reset(payload)
						if _, err := s.Send(reader); err != nil {
							producerErr <- err
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-producerErr:
				b.Fatal(err)
			default:
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
			<-resultsDone
			select {
			case err := <-resultErr:
				b.Fatal(err)
			default:
			}
		})
	}
}

/*
BenchmarkSender_SustainedAWS_SmallParts tests smaller MinPartBytes values
(closer to aws-cli's 8MB default chunksize) across higher producer counts,
to see if more numerous, smaller, independent part uploads achieve better
aggregate concurrency/throughput to S3 than fewer large 25MB parts.
Result: 8-10 MiB parts roughly double throughput versus 25MB parts,
confirming S3 throughput scales with concurrent request count rather
than request size. This informed the package's new default MinPartBytes.

	AWS_REGION=sa-east-1 CONSIST_BENCH_BUCKET=bucketname \
		go test ./sender \
		  -run='^$' \
		  -bench='^BenchmarkSender_SustainedAWS_SmallParts$' \
		  -benchtime=10s \
		  -count=1 \
		  -benchmem
*/
func BenchmarkSender_SustainedAWS_SmallParts(b *testing.B) {
	bucket := os.Getenv("CONSIST_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set CONSIST_BENCH_BUCKET")
	}

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		b.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.APIOptions = append(o.APIOptions, v4signer.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	})
	payload := make([]byte, 10*1024)

	partSizes := []struct {
		name  string
		bytes int
	}{
		{"part_8MiB", 8 * 1024 * 1024},
		{"part_10MiB", 10 * 1024 * 1024},
	}
	producerCounts := []int{16, 32, 64}

	for _, ps := range partSizes {
		for _, producers := range producerCounts {
			b.Run(fmt.Sprintf("%s/producers_%d", ps.name, producers), func(b *testing.B) {
				s, err := sender.NewSender(sender.Options{
					Client:        client,
					Bucket:        bucket,
					Prefix:        "consist-bench/smallparts",
					MaxBatchBytes: 200 * 1024 * 1024,
					MinPartBytes:  ps.bytes,
				})
				if err != nil {
					b.Fatal(err)
				}

				resultErr := make(chan error, 1)
				resultsDone := make(chan struct{})
				go func() {
					defer close(resultsDone)
					for result := range s.Results() {
						if result.Err != nil {
							select {
							case resultErr <- result.Err:
							default:
							}
						}
					}
				}()

				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				var wg sync.WaitGroup
				producerErr := make(chan error, producers)
				for producer := range producers {
					wg.Add(1)
					go func(producer int) {
						defer wg.Done()
						reader := bytes.NewReader(payload)
						for i := producer; i < b.N; i += producers {
							reader.Reset(payload)
							if _, err := s.Send(reader); err != nil {
								producerErr <- err
								return
							}
						}
					}(producer)
				}
				wg.Wait()
				b.StopTimer()

				select {
				case err := <-producerErr:
					b.Fatal(err)
				default:
				}
				if err := s.Close(ctx); err != nil {
					b.Fatal(err)
				}
				<-resultsDone
				select {
				case err := <-resultErr:
					b.Fatal(err)
				default:
				}
			})
		}
	}
}
