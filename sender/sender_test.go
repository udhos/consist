package sender_test

import (
	"bytes"
	"context"
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
