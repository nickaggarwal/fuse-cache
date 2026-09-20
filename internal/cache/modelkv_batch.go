package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// The on-the-wire format for a coalesced KV batch.
//
// A batch is the durable unit: one cloud object holding many KV blocks. The
// format is deliberately self-describing and checksummed per block, because
// the batch is read back by a *different* process than wrote it — usually on a
// different node, after the writer died. There is no index to consult and no
// writer to ask, so everything needed to extract a block must be in the object
// itself.
//
// Layout:
//
//	magic   [4]byte  "FKVB"
//	version uint32   format version
//	count   uint32   number of blocks
//	repeated count times:
//	  keyLen   uint32
//	  key      keyLen bytes (UTF-8)
//	  dataLen  uint64
//	  crc32c   uint32   (Castagnoli, over data only)
//	  data     dataLen bytes
//
// All integers little-endian. Blocks appear in the order given, which the
// flusher sorts, so an identical set of blocks produces an identical object.
const (
	kvBatchMagic   = "FKVB"
	kvBatchVersion = uint32(1)
	// kvBatchMaxBlock bounds a single block at decode time so a corrupt length
	// field cannot drive a multi-gigabyte allocation. A KV chunk at LMCache's
	// 256-token default is ~7MiB on a 1.5B model and scales with model size;
	// 1GiB is far above any real block and far below a memory hazard.
	kvBatchMaxBlock = int64(1) << 30
)

var kvCastagnoli = crc32.MakeTable(crc32.Castagnoli)

// encodeBatch serializes blocks in the order given by keys.
func encodeBatch(keys []string, blobs map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(kvBatchMagic)
	_ = binary.Write(&buf, binary.LittleEndian, kvBatchVersion)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(keys)))

	for _, k := range keys {
		data, ok := blobs[k]
		if !ok {
			return nil, fmt.Errorf("modelkv: key %q missing from batch payload", k)
		}
		if k == "" {
			return nil, fmt.Errorf("modelkv: empty key in batch")
		}
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(k)))
		buf.WriteString(k)
		_ = binary.Write(&buf, binary.LittleEndian, uint64(len(data)))
		_ = binary.Write(&buf, binary.LittleEndian, crc32.Checksum(data, kvCastagnoli))
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

// decodeBatch parses a batch object. A block whose checksum fails is an error
// rather than a silent omission: the caller asked for durability, and handing
// back corrupt KV would produce wrong tokens with no indication anything went
// wrong. Losing the block loudly is strictly better than serving it quietly.
func decodeBatch(payload []byte) (map[string][]byte, error) {
	r := bytes.NewReader(payload)

	magic := make([]byte, 4)
	if _, err := r.Read(magic); err != nil {
		return nil, fmt.Errorf("modelkv: short batch header: %w", err)
	}
	if string(magic) != kvBatchMagic {
		return nil, fmt.Errorf("modelkv: bad batch magic %q", string(magic))
	}
	var version, count uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("modelkv: batch version: %w", err)
	}
	if version != kvBatchVersion {
		return nil, fmt.Errorf("modelkv: unsupported batch version %d", version)
	}
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("modelkv: batch count: %w", err)
	}

	out := make(map[string][]byte, count)
	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("modelkv: block %d key length: %w", i, err)
		}
		if uint64(keyLen) > uint64(r.Len()) {
			return nil, fmt.Errorf("modelkv: block %d key length %d exceeds remaining bytes", i, keyLen)
		}
		keyBuf := make([]byte, keyLen)
		if _, err := readFull(r, keyBuf); err != nil {
			return nil, fmt.Errorf("modelkv: block %d key: %w", i, err)
		}
		var dataLen uint64
		if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
			return nil, fmt.Errorf("modelkv: block %d data length: %w", i, err)
		}
		// Compare as uint64 before any int64 conversion: a corrupt all-ones
		// length converts to int64(-1), which passes every signed bound check
		// and then drives make([]byte, dataLen) into a 16-exabyte allocation.
		if dataLen > uint64(kvBatchMaxBlock) {
			return nil, fmt.Errorf("modelkv: block %d length %d exceeds max", i, dataLen)
		}
		if dataLen > uint64(r.Len()) {
			return nil, fmt.Errorf("modelkv: block %d truncated: want %d bytes, %d remain", i, dataLen, r.Len())
		}
		var want uint32
		if err := binary.Read(r, binary.LittleEndian, &want); err != nil {
			return nil, fmt.Errorf("modelkv: block %d crc: %w", i, err)
		}
		data := make([]byte, dataLen)
		if _, err := readFull(r, data); err != nil {
			return nil, fmt.Errorf("modelkv: block %d data: %w", i, err)
		}
		if got := crc32.Checksum(data, kvCastagnoli); got != want {
			return nil, fmt.Errorf("modelkv: block %q checksum mismatch: want %08x got %08x", string(keyBuf), want, got)
		}
		out[string(keyBuf)] = data
	}
	return out, nil
}

// readFull fills buf or returns an error. bytes.Reader.Read may return a short
// read at the boundary, and a partial key or block read silently corrupts
// every subsequent offset in the batch.
func readFull(r *bytes.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, fmt.Errorf("unexpected EOF")
		}
	}
	return total, nil
}
