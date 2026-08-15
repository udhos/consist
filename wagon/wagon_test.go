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

	// Test with AddMessageID = true -> generates 'm' metadata TLV
	var buf2 bytes.Buffer
	enc2, _ := NewEncoder(&buf2, Options{AddMessageID: true})
	_ = enc2.Encode(msg)
	expectedIDOption := "w1:26:m:12:k:2:id4:autod:5:hello"
	if got := buf2.String(); got != expectedIDOption {
		t.Errorf("AddMessageID encoder got %q, want %q", got, expectedIDOption)
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

func BenchmarkEncode10kRecords10kBytes(b *testing.B) {
	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = 'a'
	}
	msg := Message{
		Metadata:   Metadata{MessageID: "msg-1234567890"},
		Attributes: map[string]string{"env": "production", "region": "us-east-1"},
		Data:       payload,
	}

	b.ResetTimer()
	b.SetBytes(int64(10000 * len(payload)))

	for i := 0; i < b.N; i++ {
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
