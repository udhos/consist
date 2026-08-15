# Spec

This specification defines the `wagon` format.

# Wagon Format Objective

`wagon` is the file format for the `consist` streaming framework.

The `wagon` format version 1:

- It is streamable.
- It is append only.
- It has good parsing performance.
- It is self-describing.
- It is relatively simple.
- It is ascii debugable in many cases.
- It supports 8-bit clean opaque user data.
- It is extensible with TLVs.

# Wagon Format Specification

1 - One file stores one batch of messages.

Batch object key in S3 has the following structure:

    <prefix>/YYYY-MM/DD/HH/MM/<random_ksuid>.batch

2 - A file begins with a single version prefix and then contains a sequence of records.

3 - A file has this format:

```bash
<2-bytes version>:<record><record>...<record>
```

4 - The first version is `w1` (wagon version 1). So every w1 file starts with `0x70 0x31 0x3a` (ASCII `w1:`). w1 aims to provide a balance of nice  properties: streamable, parsing performance, self-describing, simplicity, ascii debugability possible without tools in many cases, support for 8bit clean opaque user data, some extensibility with TLVs.

5 - w1 record is defined as:

```bash
<total_record_length>:<tlv1><tlv2>...<tlvn>
```

A w1 record holds a single message.

`<total_record_length>` is the total length in ascii decimal, like "1234".

`<total_record_length>` is always surrounded by `:`.

The total_record_length accounts exactly the full number of bytes AFTER the `:` that follows the total_record_length, and up to-and-including the last TLV byte of the record.

That is to say the total_record_length is the byte-length of the list of TLVs, excluding the file version prefix and the `<total_record_length>:` field itself.

tlv is defined as:

Each TLV field holds a piece of the message.

```bash
<type>:<length>:<value>
```

`<type>` is 1 byte. w1 defines 3 types that are ascii friendly:

- Type 'm' means internal metadata.
- Type 'a' means user defined attributes.
- Type 'd' means the actual user message data.

For `m` and `a`, the value encoding is an explicit single-byte marker.

```bash
m:<length>:k:<value>
a:<length>:k:<value>
```

- `k` stands for key-value encoding.

**Encoding support status:**
Support for `k` is mandatory for `m` and `a` for both producer and consumer.

Length is the length of the value in ascii decimal, like "1234".
Length is always surrounded by `:`.
Similar to total_record_length, the length field accounts exactly the byte-length of the TLV payload field.
For `m` and `a`, this payload is `k:<value>`, so length includes the `k:` marker.
For `d`, this payload is `<value>`.

The `k` encoding is a sequence of key-value pairs encoded as `<key-length>:<key-data><value-length>:<value-data>`.

Example:

- encoding: `k`
- encoding prefix: `k:` (2 bytes)
- attribute1: key=value => `3:key5:value` (12 bytes)
- attribute2: kk=vvv => `2:kk3:vvv` (9 bytes)

Total size: 2 (encoding prefix) + 12 (attribute1) + 9 (attribute2) = 23 bytes

Then the attribute TLV would be: `a:23:k:3:key5:value2:kk3:vvv`

### Storage Format Example

**Input Data:**
- User Attributes: `a=b` (`1:a1:b` = 6 bytes)
- User Data: `hello` (5 bytes)

**Breakdown:**
**File Prefix:** `w1:`
- **Record Prefix:** `21:` (The `21` represents the sum of all TLV bytes following this colon)
- **TLV 1 (Attributes):** `a:8:k:1:a1:b` (4 bytes of overhead + 8 bytes prefix/payload = 12 bytes total)
- **TLV 2 (Data):** `d:5:hello` (4 bytes of overhead + 5 bytes value = 9 bytes total)

**Final Wire File With One Message:**
`w1:21:a:8:k:1:a1:bd:5:hello`

**Multiple Messages:**
A record transports a single message.
If a producer batches two identical messages:
`w1:21:a:8:k:1:a1:bd:5:hello21:a:8:k:1:a1:bd:5:hello`



THE END
