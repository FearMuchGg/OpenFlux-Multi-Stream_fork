package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Wire format for batched frames:
// [1 byte: version = 0x02]
// [1 byte: flags]
//   bit 0: compressed (zstd applied to payload section)
// [payload section]:
//   For each packet:
//     [2 bytes: length (big-endian)]
//     [length bytes: packet data]

const (
	batchVersion    = 0x02
	batchFlagCompressed = 0x01
)

var (
	ErrInvalidBatchVersion = errors.New("invalid batch version")
	ErrBatchTooLarge       = errors.New("batch too large")
	ErrMalformedBatch      = errors.New("malformed batch")
	
	// Shared zstd encoder/decoder (thread-safe, reusable)
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	// Initialize zstd encoder/decoder once
	var err error
	zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		panic(fmt.Sprintf("failed to create zstd encoder: %v", err))
	}
	
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create zstd decoder: %v", err))
	}
}

// encodeBatch creates a batched frame from multiple packets.
// Returns the encoded frame ready for transmission.
func encodeBatch(packets [][]byte) []byte {
	if len(packets) == 0 {
		return nil
	}

	// Calculate uncompressed payload size
	var payload bytes.Buffer
	for _, pkt := range packets {
		// [2 bytes length][data]
		lenBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(lenBuf, uint16(len(pkt)))
		payload.Write(lenBuf)
		payload.Write(pkt)
	}

	uncompressed := payload.Bytes()
	
	// Try compression
	compressed := zstdEncoder.EncodeAll(uncompressed, nil)
	
	var result []byte
	var flags byte
	
	// Use compressed only if it's smaller
	if len(compressed) < len(uncompressed) {
		flags |= batchFlagCompressed
		result = make([]byte, 2+len(compressed))
		result[0] = batchVersion
		result[1] = flags
		copy(result[2:], compressed)
	} else {
		result = make([]byte, 2+len(uncompressed))
		result[0] = batchVersion
		result[1] = flags
		copy(result[2:], uncompressed)
	}

	return result
}

// decodeBatch parses a batched frame back into individual packets.
func decodeBatch(data []byte) ([][]byte, error) {
	if len(data) < 2 {
		return nil, ErrMalformedBatch
	}

	version := data[0]
	if version != batchVersion {
		return nil, ErrInvalidBatchVersion
	}

	flags := data[1]
	payload := data[2:]

	// Decompress if needed
	if flags&batchFlagCompressed != 0 {
		var err error
		payload, err = zstdDecoder.DecodeAll(payload, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decode failed: %w", err)
		}
	}

	// Parse packets
	var packets [][]byte
	reader := bytes.NewReader(payload)

	for reader.Len() > 0 {
		// Read 2-byte length
		lenBuf := make([]byte, 2)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			return nil, fmt.Errorf("failed to read packet length: %w", err)
		}

		pktLen := binary.BigEndian.Uint16(lenBuf)
		if int(pktLen) > reader.Len() {
			return nil, ErrMalformedBatch
		}

		// Read packet data
		pktData := make([]byte, pktLen)
		if _, err := io.ReadFull(reader, pktData); err != nil {
			return nil, fmt.Errorf("failed to read packet data: %w", err)
		}

		packets = append(packets, pktData)
	}

	return packets, nil
}
