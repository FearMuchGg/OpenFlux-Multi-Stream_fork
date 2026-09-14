package transport

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"universal-bypass-tool/utils"
)

const (
	defaultMaxBatchBytes = 8192
	defaultMaxBatchCount = 64
	defaultLingerMs      = 5
	batchQueueDepth      = 4096
)

// BatchedTransport wraps an inner transport and coalesces multiple packets
// into a single batched frame before transmission. This reduces overhead
// from per-message JSON/base64 encoding in protocols like WebSocket.
//
// Critical packets (marked with special prefix) bypass batching and are
// sent immediately for low-latency operations (SYN/FIN/RST).
type BatchedTransport struct {
	Transport

	queue         chan []byte
	lingerMs      int
	maxBatchBytes int
	maxBatchCount int

	running atomic.Bool
	wg      sync.WaitGroup

	mu     sync.RWMutex
	userCb func([]byte)
}

// NewBatchedTransport creates a new batching layer over an inner transport.
// Configuration via environment variables:
//   - OPENFLUX_BATCH_BYTES: max batch size in bytes (default: 8192)
//   - OPENFLUX_BATCH_COUNT: max packets per batch (default: 64)
//   - OPENFLUX_BATCH_LINGER_MS: linger time in ms (default: 5)
func NewBatchedTransport(inner Transport) *BatchedTransport {
	b := &BatchedTransport{
		Transport:     inner,
		queue:         make(chan []byte, batchQueueDepth),
		lingerMs:      envInt("OPENFLUX_BATCH_LINGER_MS", defaultLingerMs),
		maxBatchBytes: envInt("OPENFLUX_BATCH_BYTES", defaultMaxBatchBytes),
		maxBatchCount: envInt("OPENFLUX_BATCH_COUNT", defaultMaxBatchCount),
	}

	// Start flush loop
	b.running.Store(true)
	b.wg.Add(1)
	go b.flushLoop()

	utils.Debugf("[BATCH] initialized: max_bytes=%d, max_count=%d, linger=%dms",
		b.maxBatchBytes, b.maxBatchCount, b.lingerMs)

	return b
}

// Send queues a packet for batched transmission.
// Returns error if queue is full.
func (b *BatchedTransport) Send(data []byte) error {
	if !b.running.Load() {
		return fmt.Errorf("batched transport stopped")
	}

	// Copy to avoid caller modifying slice after send
	p := make([]byte, len(data))
	copy(p, data)

	select {
	case b.queue <- p:
		return nil
	default:
		return fmt.Errorf("batch queue full")
	}
}

// SendImmediate sends a packet directly to inner transport, bypassing batching.
// Use for critical packets (SYN/FIN/RST) that need low latency.
func (b *BatchedTransport) SendImmediate(data []byte) error {
	return b.Transport.Send(data)
}

// Receive sets up the callback for decoded packets.
// The inner transport receives batched frames, which are decoded here.
func (b *BatchedTransport) Receive(callback func([]byte)) {
	b.mu.Lock()
	b.userCb = callback
	b.mu.Unlock()

	b.Transport.Receive(func(data []byte) {
		// Check if this is a batched frame (starts with 0x02)
		if len(data) > 0 && data[0] == batchVersion {
			pkts, err := decodeBatch(data)
			if err != nil {
				utils.Debugf("[BATCH] decode error (%d bytes): %v", len(data), err)
				return
			}

			b.mu.RLock()
			cb := b.userCb
			b.mu.RUnlock()

			if cb == nil {
				return
			}

			// Deliver each packet from the batch
			for _, p := range pkts {
				cb(p)
			}
		} else {
			// Not a batched frame, pass through (e.g., critical packets)
			b.mu.RLock()
			cb := b.userCb
			b.mu.RUnlock()

			if cb != nil {
				cb(data)
			}
		}
	})
}

// Stop gracefully stops the batched transport.
func (b *BatchedTransport) Stop() error {
	b.running.Store(false)
	close(b.queue)
	b.wg.Wait()
	return b.Transport.Stop()
}

// flushLoop continuously drains the queue and sends batched frames.
func (b *BatchedTransport) flushLoop() {
	defer b.wg.Done()

	for b.running.Load() {
		// Wait for first packet
		first, ok := <-b.queue
		if !ok {
			return
		}

		batch := [][]byte{first}
		size := 2 + len(first) // 2 bytes for length prefix

		// Phase 1: Burst coalescing - grab everything already in queue
		for size < b.maxBatchBytes && len(batch) < b.maxBatchCount {
			select {
			case p, ok := <-b.queue:
				if !ok {
					// Channel closed, send what we have
					b.sendBatch(batch)
					return
				}
				batch = append(batch, p)
				size += 2 + len(p)
			default:
				// Queue empty, exit burst phase
				goto linger
			}
		}

	linger:
		// Phase 2: Linger - wait briefly for stragglers
		if b.lingerMs > 0 && size < b.maxBatchBytes && len(batch) < b.maxBatchCount {
			timer := time.NewTimer(time.Duration(b.lingerMs) * time.Millisecond)
		lingerLoop:
			for size < b.maxBatchBytes && len(batch) < b.maxBatchCount {
				select {
				case p, ok := <-b.queue:
					if !ok {
						timer.Stop()
						b.sendBatch(batch)
						return
					}
					batch = append(batch, p)
					size += 2 + len(p)
				case <-timer.C:
					break lingerLoop
				}
			}
			timer.Stop()
		}

		// Send the batch
		b.sendBatch(batch)
	}
}

// sendBatch encodes and sends a batch of packets.
func (b *BatchedTransport) sendBatch(packets [][]byte) {
	if len(packets) == 0 {
		return
	}

	frame := encodeBatch(packets)
	if err := b.Transport.Send(frame); err != nil {
		utils.Debugf("[BATCH] send error: %v", err)
		// Don't retry here - upper layers (OFSP) handle reliability
	} else {
		utils.Debugf("[BATCH] sent %d packets in %d bytes", len(packets), len(frame))
	}
}

// envInt reads an integer from environment variable with default value.
func envInt(key string, defaultVal int) int {
	if val := getEnv(key); val != "" {
		var result int
		if _, err := fmt.Sscanf(val, "%d", &result); err == nil {
			return result
		}
	}
	return defaultVal
}

// getEnv is a helper to read environment variables.
func getEnv(key string) string {
	return os.Getenv(key)
}
