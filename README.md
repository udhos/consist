# consist

[consist](https://github.com/udhos/consist) is a high-volume cost-effective streaming framework built over reliable serverless primitives like S3 as Blob Storage and on SQS as Reliable Notification Service.

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