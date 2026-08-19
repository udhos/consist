package wagon

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestEncode(t *testing.T) {
	msg := Message{
		Attributes: map[string]string{
			"a": "b",
		},
		Data: []byte("hello"),
	}

	var buf bytes.Buffer
	enc, err := NewEncoder(&buf)
	if err != nil {
		t.Fatalf("unexpected NewEncoder error: %v", err)
	}

	if err := enc.Encode(msg); err != nil {
		t.Fatalf("unexpected Encode error: %v", err)
	}

	// Expected per wagon/spec.md lines 113-125:
	// File Prefix: "w1:"
	// Record Prefix: "25:"
	// TLV 1 (Attributes): "a:8:k:1:a1:b" (12 bytes)
	// TLV 2 (Data): "d:5:hello" (9 bytes)
	// Total record length = 12 + 9 = 21
	expected := "w1:21:a:8:k:1:a1:bd:5:hello"
	if got := buf.String(); got != expected {
		t.Errorf("got %q, want %q", got, expected)
	}
}

func TestEncoderOptionsAddMessageID(t *testing.T) {
	msg := Message{Data: []byte("hello")}

	// Test default (AddMessageID = false) -> no 'm' metadata TLV
	var buf1 bytes.Buffer
	enc1, _ := NewEncoder(&buf1)
	_ = enc1.Encode(msg)
	expectedDefault := "w1:9:d:5:hello"
	if got := buf1.String(); got != expectedDefault {
		t.Errorf("default encoder got %q, want %q", got, expectedDefault)
	}

	// Test with AddMessageID = true -> generates 'm' metadata TLV with a non-empty ID
	var buf2 bytes.Buffer
	enc2, _ := NewEncoder(&buf2, Options{AddMessageID: true})
	_ = enc2.Encode(msg)
	wire := buf2.String()
	if !bytes.HasPrefix(buf2.Bytes(), []byte("w1:")) {
		t.Errorf("AddMessageID encoder output missing magic header, got %q", wire)
	}
	if !bytes.Contains(buf2.Bytes(), []byte("m:")) {
		t.Errorf("AddMessageID encoder output missing 'm' TLV, got %q", wire)
	}
}

// TestAddMessageIDFalse verifies that when AddMessageID is false (the default),
// no 'm' TLV is written to the stream and the decoded MessageID is empty.
func TestAddMessageIDFalse(t *testing.T) {
	msg := Message{Data: []byte("payload")}

	// Encode with default options (AddMessageID = false)
	var buf bytes.Buffer
	enc, err := NewEncoder(&buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if err := enc.Encode(msg); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	wire := buf.Bytes()

	// The raw wire bytes must not contain an 'm:' TLV marker.
	if bytes.Contains(wire, []byte("m:")) {
		t.Errorf("AddMessageID=false: unexpected 'm:' TLV in wire bytes: %q", wire)
	}

	// Decode and confirm MessageID is empty.
	dec, err := NewDecoder(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	var got Message
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Metadata.MessageID != "" {
		t.Errorf("AddMessageID=false: expected empty MessageID, got %q", got.Metadata.MessageID)
	}
}

// TestAddMessageIDTrue verifies that when AddMessageID is true, the encoder
// automatically generates a non-empty, random MessageID that:
//   - is present in the 'm' TLV of the encoded bytes;
//   - round-trips correctly through the decoder;
//   - is unique across independent Encode calls.
func TestAddMessageIDTrue(t *testing.T) {
	msg := Message{Data: []byte("payload")}

	enc, err := NewEncoder(nil, Options{AddMessageID: true})
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}

	var ids []string
	const rounds = 5
	for i := range rounds {
		var buf bytes.Buffer
		enc.Reset(&buf)
		if err := enc.Encode(msg); err != nil {
			t.Fatalf("round %d: Encode: %v", i, err)
		}

		wire := buf.Bytes()

		// Wire bytes must contain an 'm:' TLV marker.
		if !bytes.Contains(wire, []byte("m:")) {
			t.Errorf("round %d: AddMessageID=true: missing 'm:' TLV in wire bytes: %q", i, wire)
		}

		// Decode and confirm the MessageID is non-empty.
		dec, err := NewDecoder(bytes.NewReader(wire))
		if err != nil {
			t.Fatalf("round %d: NewDecoder: %v", i, err)
		}
		var got Message
		if err := dec.Decode(&got); err != nil {
			t.Fatalf("round %d: Decode: %v", i, err)
		}
		if got.Metadata.MessageID == "" {
			t.Errorf("round %d: AddMessageID=true: decoded MessageID is empty", i)
		}
		ids = append(ids, got.Metadata.MessageID)
	}

	// All generated IDs must be unique.
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			t.Errorf("AddMessageID=true: duplicate MessageID generated: %q", id)
		}
		seen[id] = struct{}{}
	}
}

// TestAddMessageIDTrueEncodeReader is the EncodeReader counterpart of
// TestAddMessageIDTrue, exercising the streaming path.
func TestAddMessageIDTrueEncodeReader(t *testing.T) {
	data := []byte("payload")
	msg := Message{} // no explicit MessageID

	enc, err := NewEncoder(nil, Options{AddMessageID: true})
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}

	var ids []string
	const rounds = 5
	for i := range rounds {
		var buf bytes.Buffer
		enc.Reset(&buf)
		if err := enc.EncodeReader(msg, bytes.NewReader(data), len(data)); err != nil {
			t.Fatalf("round %d: EncodeReader: %v", i, err)
		}

		wire := buf.Bytes()

		if !bytes.Contains(wire, []byte("m:")) {
			t.Errorf("round %d: AddMessageID=true (EncodeReader): missing 'm:' TLV: %q", i, wire)
		}

		dec, err := NewDecoder(bytes.NewReader(wire))
		if err != nil {
			t.Fatalf("round %d: NewDecoder: %v", i, err)
		}
		var got Message
		if err := dec.Decode(&got); err != nil {
			t.Fatalf("round %d: Decode: %v", i, err)
		}
		if got.Metadata.MessageID == "" {
			t.Errorf("round %d: AddMessageID=true (EncodeReader): decoded MessageID is empty", i)
		}
		ids = append(ids, got.Metadata.MessageID)
	}

	// Uniqueness check.
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			t.Errorf("AddMessageID=true (EncodeReader): duplicate MessageID: %q", id)
		}
		seen[id] = struct{}{}
	}
}

// TestAddMessageIDFalseEncodeReader is the EncodeReader counterpart of
// TestAddMessageIDFalse, verifying no 'm' TLV when option is disabled.
func TestAddMessageIDFalseEncodeReader(t *testing.T) {
	data := []byte("payload")
	msg := Message{}

	var buf bytes.Buffer
	enc, err := NewEncoder(&buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if err := enc.EncodeReader(msg, bytes.NewReader(data), len(data)); err != nil {
		t.Fatalf("EncodeReader: %v", err)
	}

	wire := buf.Bytes()

	if bytes.Contains(wire, []byte("m:")) {
		t.Errorf("AddMessageID=false (EncodeReader): unexpected 'm:' TLV in wire bytes: %q", wire)
	}

	dec, err := NewDecoder(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	var got Message
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Metadata.MessageID != "" {
		t.Errorf("AddMessageID=false (EncodeReader): expected empty MessageID, got %q", got.Metadata.MessageID)
	}
}

func TestDecode(t *testing.T) {

	// Encode reference message to get exact wire bytes:

	refMsg := Message{
		Metadata:   Metadata{MessageID: "101"},
		Attributes: map[string]string{"a": "b"},
		Data:       []byte("hello"),
	}
	var refBuf bytes.Buffer
	enc, err := NewEncoder(&refBuf)
	if err != nil {
		t.Fatalf("unexpected NewEncoder error: %v", err)
	}
	if err := enc.Encode(refMsg); err != nil {
		t.Fatalf("unexpected Encode error: %v", err)
	}

	dec, err := NewDecoder(&refBuf)
	if err != nil {
		t.Fatalf("unexpected NewDecoder error: %v", err)
	}

	var got Message
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("unexpected Decode error: %v", err)
	}

	if got.Metadata.MessageID != "101" {
		t.Errorf("Metadata.MessageID got %q, want %q", got.Metadata.MessageID, "101")
	}
	if got.Attributes["a"] != "b" {
		t.Errorf("Attributes[\"a\"] got %q, want %q", got.Attributes["a"], "b")
	}
	if string(got.Data) != "hello" {
		t.Errorf("Data got %q, want %q", string(got.Data), "hello")
	}
}

func TestDecodeMultipleMessages(t *testing.T) {
	// w1 prefix followed by two records:
	// record 1: 9:d:5:hello
	// record 2: 9:d:5:world
	input := "w1:9:d:5:hello9:d:5:world"
	buf := bytes.NewBufferString(input)

	dec, err := NewDecoder(buf)
	if err != nil {
		t.Fatalf("unexpected NewDecoder error: %v", err)
	}

	var msg1, msg2 Message
	if err := dec.Decode(&msg1); err != nil {
		t.Fatalf("decode record 1 error: %v", err)
	}
	if string(msg1.Data) != "hello" {
		t.Errorf("msg1 data got %q, want %q", string(msg1.Data), "hello")
	}

	if err := dec.Decode(&msg2); err != nil {
		t.Fatalf("decode record 2 error: %v", err)
	}
	if string(msg2.Data) != "world" {
		t.Errorf("msg2 data got %q, want %q", string(msg2.Data), "world")
	}

	// 3rd read should return io.EOF
	var msg3 Message
	if err := dec.Decode(&msg3); !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF on end of stream, got: %v", err)
	}
}

func TestDecodeKVEncoding(t *testing.T) {
	// Spec line 109: a:23:k:3:key5:value2:kk3:vvv (28 bytes)
	// Overhead of record prefix 28: is 28 bytes total
	input := "w1:28:a:23:k:3:key5:value2:kk3:vvv"

	buf := bytes.NewBufferString(input)

	dec, err := NewDecoder(buf)
	if err != nil {
		t.Fatalf("unexpected NewDecoder error: %v", err)
	}

	var got Message
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("unexpected Decode error: %v", err)
	}

	wantAttrs := map[string]string{
		"key": "value",
		"kk":  "vvv",
	}
	if !reflect.DeepEqual(got.Attributes, wantAttrs) {
		t.Errorf("Attributes got %v, want %v", got.Attributes, wantAttrs)
	}
}

func TestDecodeBinaryData(t *testing.T) {
	// Binary opaque data payload: 5 bytes [0x00, 0xFF, 0x0A, 0x0D, 0x3A]
	// TLV: d:5:<binary> (9 bytes total)
	input := []byte("w1:9:d:5:\x00\xFF\x0A\x0D\x3A")
	buf := bytes.NewBuffer(input)

	dec, err := NewDecoder(buf)
	if err != nil {
		t.Fatalf("unexpected NewDecoder error: %v", err)
	}

	var got Message
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("unexpected Decode error: %v", err)
	}

	wantBytes := []byte{0x00, 0xFF, 0x0A, 0x0D, 0x3A}
	if !bytes.Equal(got.Data, wantBytes) {
		t.Errorf("Data got %v, want %v", got.Data, wantBytes)
	}
}

func TestDecodeInvalidHeader(t *testing.T) {
	input := "w2:9:d:5:hello"
	buf := bytes.NewBufferString(input)

	_, err := NewDecoder(buf)
	if err == nil {
		t.Error("expected error for invalid header prefix w2:, got nil")
	}
}

func TestDecodeTruncatedStream(t *testing.T) {
	// Declares 20 bytes total record length, but ends abruptly
	input := "w1:20:d:5:hel"
	buf := bytes.NewBufferString(input)

	dec, err := NewDecoder(buf)
	if err != nil {
		t.Fatalf("unexpected NewDecoder error: %v", err)
	}

	var got Message
	if err := dec.Decode(&got); err == nil {
		t.Error("expected error for truncated stream, got nil")
	}
}

/*
go test -bench='^BenchmarkEncode10kRecords10kBytes$' -benchmem ./wagon
goos: linux
goarch: amd64
pkg: github.com/udhos/consist/wagon
cpu: 13th Gen Intel(R) Core(TM) i7-1360P
BenchmarkEncode10kRecords10kBytes-16    	      34	  34687733 ns/op	2882.86 MB/s	301543689 B/op	   10008 allocs/op
PASS
ok  	github.com/udhos/consist/wagon	2.492s
*/
func BenchmarkEncode10kRecords10kBytes(b *testing.B) {
	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = 'a'
	}
	msg := Message{
		//Metadata:   Metadata{MessageID: "msg-1234567890"},
		Attributes: map[string]string{"env": "production", "region": "us-east-1"},
		Data:       payload,
	}

	b.SetBytes(int64(10000 * len(payload)))

	for b.Loop() {
		var buf bytes.Buffer
		buf.Grow(10000 * 10050) // pre-allocate ~100MB buffer
		enc, err := NewEncoder(&buf)
		if err != nil {
			b.Fatalf("NewEncoder error: %v", err)
		}
		for range 10000 {
			if err := enc.Encode(msg); err != nil {
				b.Fatalf("Encode error: %v", err)
			}
		}
	}
}

func BenchmarkDecode10kRecords10kBytes(b *testing.B) {
	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = 'a'
	}
	msg := Message{
		Metadata:   Metadata{MessageID: "msg-1234567890"},
		Attributes: map[string]string{"env": "production", "region": "us-east-1"},
		Data:       payload,
	}

	var buf bytes.Buffer
	enc, err := NewEncoder(&buf)
	if err != nil {
		b.Fatalf("NewEncoder error: %v", err)
	}
	for range 10000 {
		if err := enc.Encode(msg); err != nil {
			b.Fatalf("Encode error: %v", err)
		}
	}
	encodedBytes := buf.Bytes()

	b.ResetTimer()
	b.SetBytes(int64(10000 * len(payload)))

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(encodedBytes)
		dec, err := NewDecoder(reader)
		if err != nil {
			b.Fatalf("NewDecoder error: %v", err)
		}
		var record Message
		for {
			err := dec.Decode(&record)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatalf("Decode error: %v", err)
			}
		}
	}
}
