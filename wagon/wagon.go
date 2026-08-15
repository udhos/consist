// Package wagon implements encoding and decoding for the Wagon v1 (w1) file format,
// the streaming message batch container format used by the consist framework.
//
// Wagon is an append-only, streamable, ASCII-debuggable binary format designed for
// high-throughput processing. A Wagon stream begins with a version prefix ("w1:") followed
// by a sequence of records framed by ASCII length prefixes and TLV (Type-Length-Value)
// fields for metadata ('m'), user attributes ('a'), and opaque payload data ('d').
package wagon

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const magicHeader = "w1:"

// Metadata contains system-level internal metadata for a record ('m' TLV).
type Metadata struct {
	MessageID string
}

// Message represents a single wagon record.
type Message struct {
	Metadata   Metadata          // Internal metadata ('m' TLV)
	Attributes map[string]string // User-defined key-value attributes ('a' TLV)
	Data       []byte            // Opaque user message payload ('d' TLV)
}

// Options controls encoding configuration options.
type Options struct {
	AddMessageID bool // If true, automatically generates and includes a MessageID if not set. Default is false.
}

// Encoder writes Wagon formatted batches to an underlying io.Writer.
type Encoder struct {
	w           io.Writer
	options     Options
	headerWrote bool
	tlvBuf      bytes.Buffer
	kvBuf       bytes.Buffer
	numBuf      [32]byte
}

// NewEncoder creates a new Wagon Encoder with optional configuration.
func NewEncoder(w io.Writer, opts ...Options) (*Encoder, error) {
	var options Options
	if len(opts) > 0 {
		options = opts[0]
	}
	return &Encoder{w: w, options: options}, nil
}

// Encode writes a single Message record to the stream.
func (e *Encoder) Encode(msg Message) error {
	if !e.headerWrote {
		if _, err := e.w.Write([]byte(magicHeader)); err != nil {
			return err
		}
		e.headerWrote = true
	}

	e.tlvBuf.Reset()

	msgID := msg.Metadata.MessageID
	if msgID == "" && e.options.AddMessageID {
		// Example auto-generated message ID logic when enabled
		msgID = "auto"
	}

	// Metadata ('m' TLV) - Encoded as 'k' format: "id:<msgID>"
	if msgID != "" {
		e.kvBuf.Reset()
		e.appendKV(&e.kvBuf, "id", msgID)

		payloadLen := 2 + e.kvBuf.Len() // "k:" + KV data
		e.tlvBuf.WriteString("m:")
		e.appendInt(payloadLen)
		e.tlvBuf.WriteString(":k:")
		e.tlvBuf.Write(e.kvBuf.Bytes())
	}

	// Attributes ('a' TLV) - Encoded as 'k' format
	if len(msg.Attributes) > 0 {
		e.kvBuf.Reset()
		for k, v := range msg.Attributes {
			e.appendKV(&e.kvBuf, k, v)
		}

		payloadLen := 2 + e.kvBuf.Len() // "k:" + KV data
		e.tlvBuf.WriteString("a:")
		e.appendInt(payloadLen)
		e.tlvBuf.WriteString(":k:")
		e.tlvBuf.Write(e.kvBuf.Bytes())
	}

	// Data ('d' TLV)
	e.tlvBuf.WriteString("d:")
	e.appendInt(len(msg.Data))
	e.tlvBuf.WriteByte(':')
	e.tlvBuf.Write(msg.Data)

	recordLen := e.tlvBuf.Len()

	// Write record header "<recordLen>:" without allocation
	numSlice := strconv.AppendInt(e.numBuf[:0], int64(recordLen), 10)
	if _, err := e.w.Write(numSlice); err != nil {
		return err
	}
	if _, err := e.w.Write([]byte{':'}); err != nil {
		return err
	}
	_, err := e.w.Write(e.tlvBuf.Bytes())
	return err
}

func (e *Encoder) appendInt(v int) {
	b := strconv.AppendInt(e.numBuf[:0], int64(v), 10)
	e.tlvBuf.Write(b)
}

func (e *Encoder) appendKV(buf *bytes.Buffer, k, v string) {
	bK := strconv.AppendInt(e.numBuf[:0], int64(len(k)), 10)
	buf.Write(bK)
	buf.WriteByte(':')
	buf.WriteString(k)

	bV := strconv.AppendInt(e.numBuf[:0], int64(len(v)), 10)
	buf.Write(bV)
	buf.WriteByte(':')
	buf.WriteString(v)
}

// Decoder reads Wagon formatted batches from an underlying io.Reader.
type Decoder struct {
	r             *bufio.Reader
	headerChecked bool
	recordBuf     []byte
	numBuf        []byte
}

// NewDecoder creates a new Wagon Decoder and verifies the header.
func NewDecoder(r io.Reader) (*Decoder, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	hdr := make([]byte, len(magicHeader))
	if _, err := io.ReadFull(br, hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if string(hdr) != magicHeader {
		return nil, fmt.Errorf("invalid wagon header: %q", string(hdr))
	}
	return &Decoder{
		r:             br,
		headerChecked: true,
		numBuf:        make([]byte, 0, 32),
	}, nil
}

// Decode reads the next Message record from the stream.
func (d *Decoder) Decode(msg *Message) error {
	d.numBuf = d.numBuf[:0]
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(d.numBuf) == 0 {
				return io.EOF
			}
			return fmt.Errorf("read record length: %w", err)
		}
		if b == ':' {
			break
		}
		d.numBuf = append(d.numBuf, b)
	}

	recordLen, err := parseIntBytes(d.numBuf)
	if err != nil {
		return fmt.Errorf("invalid record length %q: %w", string(d.numBuf), err)
	}

	if cap(d.recordBuf) < recordLen {
		d.recordBuf = make([]byte, recordLen)
	} else {
		d.recordBuf = d.recordBuf[:recordLen]
	}

	if _, err := io.ReadFull(d.r, d.recordBuf); err != nil {
		return fmt.Errorf("read record payload: %w", err)
	}

	*msg = Message{}
	buf := d.recordBuf

	for len(buf) > 0 {
		if len(buf) < 2 || buf[1] != ':' {
			return errors.New("malformed TLV header")
		}
		tlvType := buf[0]
		buf = buf[2:]

		// Parse TLV length
		colonIdx := bytes.IndexByte(buf, ':')
		if colonIdx == -1 {
			return errors.New("malformed TLV length delimiter")
		}
		tlvLen, err := parseIntBytes(buf[:colonIdx])
		if err != nil {
			return fmt.Errorf("invalid TLV length: %w", err)
		}
		buf = buf[colonIdx+1:]

		if len(buf) < tlvLen {
			return errors.New("truncated TLV payload")
		}
		payload := buf[:tlvLen]
		buf = buf[tlvLen:]

		switch tlvType {
		case 'm':
			metaKV := make(map[string]string)
			if err := parseKVTLV(payload, metaKV); err != nil {
				return fmt.Errorf("parse metadata tlv: %w", err)
			}
			if id, ok := metaKV["id"]; ok {
				msg.Metadata.MessageID = id
			}
		case 'a':
			if msg.Attributes == nil {
				msg.Attributes = make(map[string]string)
			}
			if err := parseKVTLV(payload, msg.Attributes); err != nil {
				return fmt.Errorf("parse attributes tlv: %w", err)
			}
		case 'd':
			msg.Data = make([]byte, len(payload))
			copy(msg.Data, payload)
		default:
			// Ignore unknown TLV types for forward compatibility
		}
	}

	return nil
}

func parseIntBytes(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty integer bytes")
	}
	var res int
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, errors.New("non-digit character")
		}
		res = res*10 + int(c-'0')
	}
	return res, nil
}

func parseKVTLV(payload []byte, target map[string]string) error {
	if len(payload) < 2 || payload[1] != ':' {
		return errors.New("invalid encoding prefix")
	}
	enc := payload[0]
	if enc != 'k' {
		return fmt.Errorf("unsupported encoding %q, expected 'k'", enc)
	}
	val := payload[2:]

	buf := val
	for len(buf) > 0 {
		colonIdx := bytes.IndexByte(buf, ':')
		if colonIdx == -1 {
			return errors.New("invalid kv key len delimiter")
		}
		keyLen, err := parseIntBytes(buf[:colonIdx])
		if err != nil {
			return fmt.Errorf("invalid kv key len: %w", err)
		}
		buf = buf[colonIdx+1:]

		if len(buf) < keyLen {
			return errors.New("truncated kv key data")
		}
		key := string(buf[:keyLen])
		buf = buf[keyLen:]

		colonIdx = bytes.IndexByte(buf, ':')
		if colonIdx == -1 {
			return errors.New("invalid kv val len delimiter")
		}
		valLen, err := parseIntBytes(buf[:colonIdx])
		if err != nil {
			return fmt.Errorf("invalid kv val len: %w", err)
		}
		buf = buf[colonIdx+1:]

		if len(buf) < valLen {
			return errors.New("truncated kv val data")
		}
		valStr := string(buf[:valLen])
		buf = buf[valLen:]

		target[key] = valStr
	}
	return nil
}
