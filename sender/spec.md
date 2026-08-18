# Sender Specification

This specification defines the behavior and API design for the `consist` `sender` component.

## Objective

The `sender` package streams payloads formatted in the `wagon` container format (`w1`) to AWS S3 (or compatible blob storage) in background batches.

Key design goals:
- **Streamable**: Accepts payload streams (`io.Reader`).
- **Monotonic Sequence IDs**: Assigns incremental sequence numbers (`1, 2, 3...`) per message.
- **Batch Acknowledgments**: Reports durable batch commits via a single `Result` channel.
- **Concurrent Multipart Uploads**: Shared batch state is serialized while independent S3 part uploads may proceed concurrently.
- **Mockable Storage Interface**: Interacts with S3 through a minimal `S3Client` interface.

## API Specification

### 1. Types

#### `Result`
Represents the processing outcome for a batch of messages.

```go
type Result struct {
    LastSeq uint64
    Err     error
}
```
- `LastSeq`: The sequence ID of the last message in the completed batch. If `Err` is `nil`, all sequence numbers up to `LastSeq` are confirmed safely stored on S3.
- `Err`: Contains any storage/upload error encountered while persisting the batch.
- If `Err != nil`, the batch is not guaranteed durable and the sender should treat all sequence IDs after the last confirmed successful batch as retryable.
- A successful `Close(ctx)` still emits a final `Result` for any pending buffered batch before the result channel is closed.
- Once the sender is closed, no further `Result` values are emitted and no further `Send()` calls are valid.

#### `Options`
Configures the `Sender` instance, target storage, and batch limits.

```go
type Options struct {
    Client           S3Client      // Minimal S3 client interface (required)
    Bucket           string        // Target S3 bucket name (required)
    Prefix           string        // Target key prefix (optional)
    MaxBatchBytes    int           // Max bytes per batch before flush (default: 100MB)
    MaxBatchTime     time.Duration // Max duration a batch can remain open before flush (default: 1s)
    MaxClientSilence time.Duration // Max inactivity duration after Send() before flush (default: 500ms)
    MinPartBytes     int           // Minimum accumulated buffer size before uploading a multipart part (default: 25MB)
}
```

Default values applied when unspecified or zero:
- `MaxBatchBytes`: `100 * 1024 * 1024` (100 MB)
- `MaxBatchTime`: `1 * time.Second` (1 second)
- `MaxClientSilence`: `500 * time.Millisecond` (500 milliseconds)
- `MinPartBytes`: `25 * 1024 * 1024` (25 MB)

#### `S3Client`
Minimal S3 interface used by `Sender` for upload operations.

```go
type S3Client interface {
    CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
    UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
    CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
    AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}
```

### 2. Core Methods

#### `NewSender`
```go
func NewSender(opts Options) (*Sender, error)
```
Instantiates a `Sender` with the provided configuration options.

#### `Send`
```go
func (s *Sender) Send(r io.Reader) (uint64, error)
```
- Accepts an `io.Reader` payload.
- Increments and returns the next monotonic sequence number (`uint64`).
- May be called concurrently; sequence assignment and batch encoding remain serialized.

#### `Results`
```go
func (s *Sender) Results() <-chan Result
```
- Returns a read-only channel delivering batch processing results (`Result`).
- Callers consume from this channel to handle cumulative acknowledgments or errors.

#### `Close`
```go
func (s *Sender) Close(ctx context.Context) error
```
- Flushes any pending buffered messages and forces the current batch to finalize.
- Completes active S3 uploads using the provided context.
- Emits the final `Result` for the remaining buffered sequences if any are pending.
- Closes internal channels only after the final flush result has been emitted.
- `Close(ctx)` is a terminal operation: after it returns, the sender is closed and no further `Send()` calls are valid.

## Batch Flush Triggers

A batch is finalized and uploaded when any of the following conditions are met:
1. **Max Size**: Cumulative batch size reaches configured byte limit.
2. **Max Time**: Batch open duration exceeds time limit.
3. **Client Silence**: Inactivity duration since last `Send()` exceeds silence limit.
4. **Explicit Close**: `Close(ctx)` is called to force a final flush of any remaining buffered records.

The first three cases are automatic batch flush conditions. `Close(ctx)` is a controlled shutdown/finalization operation that emits the last result for the remaining buffer before the sender is closed.

# Best benchmark so far

- EC2 instance type: c5.2xlarge (8 vCPU, 16 GiB RAM, up to 10 Gbps network)
- Producers: 8 or 16
- Multipart part size: 25 MB

```bash
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
BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_8-8

	3036224             46892 ns/op         218.37 MB/s       10337 B/op          1 allocs/op

BenchmarkSender_SustainedAWS_Producers_25MBPart/producers_16-8

	3030388             47047 ns/op         217.65 MB/s       10335 B/op          1 allocs/op

PASS
ok      github.com/udhos/consist/sender 383.536s
```
