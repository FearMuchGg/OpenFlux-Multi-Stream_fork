package transport

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"

	"github.com/pierrec/lz4/v4"

	"universal-bypass-tool/utils"
)

const (
	MinCompressSize   = 200
	CompressionMarker = 0x1F
)

// LooksLikeFrame reports whether data has the structure compress() produces.
//
// It exists because a shared document carries other people's editor traffic,
// and the transport cannot tell that apart from our own payload without
// looking at it. compress() only ever emits one of two shapes, and both are
// cheap to recognise exactly:
//
//	0x00 + the raw IPv4 packet            (stored, not compressed)
//	0x1F + an lz4 frame (magic 0x184D2204)
//
// This is a structural check, not a substitute for decompression — the
// authoritative test is the IPv4 check in Receive. But it is precise enough
// that unrelated traffic will not be mistaken for ours, which matters because
// a transport uses it to decide whether a frame counts as evidence that the
// stream is alive.
func LooksLikeFrame(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	switch data[0] {
	case 0x00:
		// Literal frame: the IP packet follows immediately, so the version
		// nibble is right there.
		return len(data) >= 21 && data[1]>>4 == 4
	case CompressionMarker:
		// lz4 frame magic, little-endian on the wire.
		return len(data) >= 5 && data[1] == 0x04 && data[2] == 0x22 &&
			data[3] == 0x4D && data[4] == 0x18
	default:
		return false
	}
}

type CompressedTransport struct {
	Transport

	badFrames atomic.Uint64
}

func NewCompressedTransport(inner Transport) Transport {
	return &CompressedTransport{Transport: inner}
}

// BadFrames counts frames that decompressed but were not IPv4 packets, i.e.
// traffic that reached this layer from something other than the tunnel.
func (c *CompressedTransport) BadFrames() uint64 { return c.badFrames.Load() }

func (c *CompressedTransport) Send(data []byte) error {
	compressed := compress(data)
	return c.Transport.Send(compressed)
}

// SendFlow compresses exactly like Send, then forwards the inner flow identity
// down the chain. Without this the flow key would be lost at the compression
// layer and MultiStreamTransport could not keep a connection pinned to one
// document. If the wrapped transport is not flow-aware the key is simply
// dropped and the plain Send path is used.
func (c *CompressedTransport) SendFlow(key FlowKey, data []byte) error {
	compressed := compress(data)
	if inner, ok := c.Transport.(FlowAwareSender); ok {
		return inner.SendFlow(key, compressed)
	}
	return c.Transport.Send(compressed)
}

func (c *CompressedTransport) Receive(callback func([]byte)) {
	c.Transport.Receive(func(data []byte) {
		decompressed, err := decompress(data)
		if err != nil {
			// Drop it. The old fallback passed the still-compressed buffer
			// straight up, so a corrupt frame was injected into the gVisor
			// stack as if it were an IP packet. Every well-formed frame
			// carries either the 0x00 literal prefix or an lz4 frame, so a
			// decompression error means real corruption — and dropping it is
			// safe because the inner TCP retransmits.
			utils.Debugf("[COMPRESS] dropping corrupt frame (%d bytes): %v", len(data), err)
			return
		}

		// The authoritative check that this is really ours: the tunnel only
		// ever carries IPv4 (the gVisor stack registers IPv4 alone, and
		// DialTCP rejects IPv6), so anything else arrived from somewhere other
		// than the tunnel. Dropping it here keeps unrelated traffic out of the
		// gVisor stack entirely.
		if len(decompressed) < 20 || decompressed[0]>>4 != 4 {
			c.badFrames.Add(1)
			utils.Debugf("[COMPRESS] dropping %d-byte frame that is not an IPv4 packet",
				len(decompressed))
			return
		}

		callback(decompressed)
	})
}

// Pools for the per-packet lz4 machinery and its scratch buffers. The old code
// built a fresh bytes.Buffer and a fresh lz4.Writer for every single packet.
// lz4 already pools its own block buffer internally, so the win here is the
// wrapper structs and the growing scratch buffer.
var (
	lz4WriterPool = sync.Pool{New: func() any { return lz4.NewWriter(nil) }}
	lz4ReaderPool = sync.Pool{New: func() any { return lz4.NewReader(nil) }}
	scratchPool   = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	bytesReadPool = sync.Pool{New: func() any { return new(bytes.Reader) }}
)

// literalFrame wraps data with the "stored, not compressed" prefix. The decoder
// accepts either form, so falling back to this is always wire-compatible.
func literalFrame(data []byte) []byte {
	out := make([]byte, 0, len(data)+1)
	out = append(out, 0x00)
	return append(out, data...)
}

func compress(data []byte) []byte {
	if len(data) <= MinCompressSize {
		return literalFrame(data)
	}

	buf := scratchPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteByte(CompressionMarker)

	w := lz4WriterPool.Get().(*lz4.Writer)
	w.Reset(buf)
	_, werr := w.Write(data)
	cerr := w.Close()
	lz4WriterPool.Put(w)

	// lz4 made it bigger — an incompressible payload such as HTTPS. Store it
	// literally instead; the frame format allows both.
	if werr != nil || cerr != nil || buf.Len() >= len(data)+1 {
		scratchPool.Put(buf)
		return literalFrame(data)
	}

	// Copy out before returning the scratch buffer to the pool: the caller
	// keeps the result well past this call.
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	scratchPool.Put(buf)
	return out
}

func decompress(data []byte) ([]byte, error) {
	if len(data) < 1 {
		return data, nil
	}

	if data[0] == 0x00 {
		return data[1:], nil
	}

	br := bytesReadPool.Get().(*bytes.Reader)
	br.Reset(data[1:])

	r := lz4ReaderPool.Get().(*lz4.Reader)
	r.Reset(br)

	out, err := io.ReadAll(r)

	lz4ReaderPool.Put(r)
	bytesReadPool.Put(br)
	return out, err
}
