# consist

[consist](https://github.com/udhos/consist) is a high-volume cost-effective streaming framework built over reliable serverless primitives like S3 as Blob Storage and on SQS as Reliable Notification Service.

# Design

The system is intentionally split into a durable data plane and a signaling plane:

- The sender batches records and writes complete wagon payloads to Amazon S3.
- S3 stores the durable batch object and emits an object-created notification when the upload is complete.
- That S3 event is routed to an SQS queue, which acts as the reliable wake-up and coordination channel.
- A receiver consumes the SQS message, then reads the corresponding object from S3 and processes the batch.

This gives us a serverless-friendly pattern:

- S3 is the source of truth for the data itself.
- SQS is the source of truth for availability signals and work scheduling.
- The receiver does not need to be tightly coupled to the sender; it reacts to the notification and fetches the objects.

In other words, the design is built around AWS as the machine:

- large immutable objects in S3
- reliable asynchronous wake-ups in SQS
- object completion defines the durable commit point
- downstream consumers can scale independently from producers

This is the intended model for `consist`: durable data in S3, reliable signaling in SQS, and batch-oriented processing with clear separation between storage and notification.

# Wagon file format

See `wagon` file format in [./wagon/spec.md](./wagon/spec.md).

# Test sender against AWS S3

```bash
export AWS_REGION=sa-east-1

CONSIST_BENCH_BUCKET=pulsix-br \
go test ./sender \
  -run '^$' \
  -bench 'BenchmarkSender_Send_10k_10KB_AWS$' \
  -benchmem

goos: linux
goarch: amd64
pkg: github.com/udhos/consist/sender
cpu: 13th Gen Intel(R) Core(TM) i7-1360P
BenchmarkSender_Send_10k_10KB_AWS-16    	       1	19393860312 ns/op	   5.16 MB/s	47980624 B/op	   62646 allocs/op
PASS
ok  	github.com/udhos/consist/sender	19.407s
```