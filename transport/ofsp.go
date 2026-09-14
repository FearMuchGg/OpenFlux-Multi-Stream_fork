package transport

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"universal-bypass-tool/utils"
)

// =====================================================================
// OFSP v1 (OpenFlux Stream Protocol) — thin envelope over raw IP packets
// that gives us reliable multi-stream delivery:
//
//   [0]      magic   0xFF       (never a valid IP version nibble)
//   [1]      version 0x01
//   [2]      flags   bit0=needs-ACK, bit1=is-ACK,
//                    bit2=critical, bit3=keep-alive
//   [3..10]  seq     uint64 big-endian
//   [11..]   payload (raw IP packet, or empty for ACK/keep-alive)
//
// Sender assigns a seq to each outbound packet, stores it in a pending
// map, and retransmits if no ACK arrives before RTO. Receiver deduplicates
// by seq and sends back ACK envelopes. Critical TCP packets (SYN/FIN/RST)
// are additionally sent to 2 distinct alive sessions for redundancy.
// =====================================================================

const (
	EnvMagic   byte = 0xFF
	EnvVersion byte = 0x01
	EnvHdrLen       = 11 // magic + version + flags + seq(8)

	FlagNeedsACK  byte = 0x01
	FlagIsACK     byte = 0x02
	FlagCritical  byte = 0x04
	FlagKeepAlive byte = 0x08

	maxRetransmitAttempts = 8
	initialRTO            = 300 * time.Millisecond
	maxRTO                = 3 * time.Second
	retransmitTick        = 100 * time.Millisecond
	dedupTTL              = 30 * time.Second
	dedupCleanupTick      = 10 * time.Second
)

// OFSPTransport wraps an inner transport and adds reliability layer:
// - Sequence numbers for each packet
// - ACK/NAK mechanism
// - Retransmission on timeout
// - Deduplication of received packets
// - Critical packet detection (SYN/FIN/RST)
type OFSPTransport struct {
	Transport

	seqCounter   atomic.Uint64
	pending      sync.Map // uint64 -> *pendingEntry (outbound, awaiting ACK)
	receivedSeqs sync.Map // uint64 -> *dedupEntry  (inbound, dedup)

	running atomic.Bool
	wg      sync.WaitGroup

	mu     sync.RWMutex
	userCb func([]byte)
}

type pendingEntry struct {
	envelope []byte
	sentAt   time.Time
	attempts int
	critical bool
	mu       sync.Mutex
}

type dedupEntry struct {
	seenAt time.Time
}

// NewOFSPTransport creates a new OFSP reliability layer over an inner transport.
func NewOFSPTransport(inner Transport) *OFSPTransport {
	return &OFSPTransport{
		Transport: inner,
	}
}

// Start initializes OFSP loops.
func (t *OFSPTransport) Start() error {
	if err := t.Transport.Start(); err != nil {
		return err
	}

	t.running.Store(true)

	// Retransmit loop
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.retransmitLoop()
	}()

	// Dedup cleanup loop
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.dedupCleanupLoop()
	}()

	return nil
}

// Stop gracefully stops OFSP.
func (t *OFSPTransport) Stop() error {
	t.running.Store(false)
	t.wg.Wait()
	return t.Transport.Stop()
}

// Send wraps raw IP packet in OFSP envelope and sends it.
func (t *OFSPTransport) Send(data []byte) error {
	if !t.running.Load() {
		return fmt.Errorf("OFSP transport not running")
	}

	// Already wrapped (ACK, retransmit, requeue): route as-is.
	if IsEnvelope(data) {
		return t.Transport.Send(data)
	}

	// Wrap raw IP packet.
	critical := IsCriticalPacket(data)
	env := t.wrapPacket(data, critical)

	seq := binary.BigEndian.Uint64(env[3:11])
	t.pending.Store(seq, &pendingEntry{
		envelope: env,
		sentAt:   time.Now(),
		attempts: 0,
		critical: critical,
	})

	// For critical packets (SYN/FIN/RST), bypass batching if available
	if critical {
		if batched, ok := t.Transport.(*BatchedTransport); ok {
			utils.Debugf("[OFSP] critical packet seq=%d, bypassing batch", seq)
			return batched.SendImmediate(env)
		}
	}

	return t.Transport.Send(env)
}

// Receive sets up callback for decoded packets.
func (t *OFSPTransport) Receive(callback func([]byte)) {
	t.mu.Lock()
	t.userCb = callback
	t.mu.Unlock()

	t.Transport.Receive(func(data []byte) {
		// OFSP envelope? Parse flags, dedup, ACK, deliver payload.
		if IsEnvelope(data) {
			t.handleEnvelope(data)
			return
		}

		// Legacy: raw IP packet (older peer without OFSP). Pass through.
		t.mu.RLock()
		cb := t.userCb
		t.mu.RUnlock()

		if cb != nil {
			cb(data)
		}
	})
}

// wrapPacket creates an OFSP envelope around a raw IP packet.
func (t *OFSPTransport) wrapPacket(data []byte, critical bool) []byte {
	seq := t.seqCounter.Add(1)
	flags := FlagNeedsACK
	if critical {
		flags |= FlagCritical
	}
	env := make([]byte, EnvHdrLen+len(data))
	env[0] = EnvMagic
	env[1] = EnvVersion
	env[2] = flags
	binary.BigEndian.PutUint64(env[3:11], seq)
	copy(env[EnvHdrLen:], data)
	return env
}

// wrapACK builds an ACK envelope for the given inbound sequence number.
func (t *OFSPTransport) wrapACK(seq uint64) []byte {
	env := make([]byte, EnvHdrLen)
	env[0] = EnvMagic
	env[1] = EnvVersion
	env[2] = FlagIsACK
	binary.BigEndian.PutUint64(env[3:11], seq)
	return env
}

// wrapKeepAlive builds a keep-alive envelope (no payload, needs-ACK).
func (t *OFSPTransport) wrapKeepAlive() []byte {
	env := make([]byte, EnvHdrLen)
	env[0] = EnvMagic
	env[1] = EnvVersion
	env[2] = FlagNeedsACK | FlagKeepAlive
	binary.BigEndian.PutUint64(env[3:11], t.seqCounter.Add(1))
	return env
}

// IsEnvelope checks if data is an OFSP envelope.
func IsEnvelope(data []byte) bool {
	return len(data) >= EnvHdrLen && data[0] == EnvMagic && data[1] == EnvVersion
}

// IsCriticalPacket returns true iff `data` is an IPv4 or IPv6 packet
// whose TCP segment has SYN, FIN, or RST set.
func IsCriticalPacket(data []byte) bool {
	if len(data) < 20 {
		return false
	}
	version := data[0] >> 4
	var proto byte
	var payloadStart int

	switch version {
	case 4:
		ihl := int(data[0]&0x0F) * 4
		if ihl < 20 || len(data) < ihl {
			return false
		}
		proto = data[9]
		payloadStart = ihl
	case 6:
		if len(data) < 40 {
			return false
		}
		nextHeader := data[6]
		payloadStart = 40
	walkV6:
		for {
			switch nextHeader {
			case 0, 43, 44, 60: // hop-by-hop, routing, fragment, dst opts
				if len(data) < payloadStart+8 {
					return false
				}
				nextHeader = data[payloadStart]
				extLen := int(data[payloadStart+1])*8 + 8
				if extLen < 8 {
					return false
				}
				payloadStart += extLen
			case 6, 17, 1, 58: // TCP, UDP, ICMP, ICMPv6
				proto = nextHeader
				break walkV6
			default:
				break walkV6
			}
		}
	default:
		return false
	}

	if proto != 6 { // TCP
		return false
	}
	if len(data) < payloadStart+14 {
		return false
	}
	const fin, syn, rst = 0x01, 0x02, 0x04
	return data[payloadStart+13]&(fin|syn|rst) != 0
}

// handleEnvelope processes an inbound OFSP envelope.
func (t *OFSPTransport) handleEnvelope(env []byte) {
	if len(env) < EnvHdrLen {
		return
	}
	flags := env[2]
	seq := binary.BigEndian.Uint64(env[3:11])
	payload := env[EnvHdrLen:]

	// Inbound ACK: drop matching pending entry.
	if flags&FlagIsACK != 0 {
		t.pending.Delete(seq)
		return
	}

	// Dedup check for inbound data packets.
	entry := &dedupEntry{seenAt: time.Now()}
	if _, loaded := t.receivedSeqs.LoadOrStore(seq, entry); loaded {
		// Duplicate. The sender likely missed our first ACK, so resend it.
		if flags&FlagNeedsACK != 0 {
			_ = t.Transport.Send(t.wrapACK(seq))
		}
		return
	}

	// Send ACK synchronously.
	if flags&FlagNeedsACK != 0 {
		_ = t.Transport.Send(t.wrapACK(seq))
	}

	// Keep-alive: no payload, just a liveness probe.
	if flags&FlagKeepAlive != 0 {
		return
	}

	// Real data: deliver the raw IP packet to the user callback.
	t.mu.RLock()
	cb := t.userCb
	t.mu.RUnlock()

	if cb != nil {
		cb(payload)
	}
}

// retransmitLoop scans the pending map every 100ms and retransmits any
// packet that hasn't been ACKed within its (exponentially growing) RTO.
func (t *OFSPTransport) retransmitLoop() {
	ticker := time.NewTicker(retransmitTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !t.running.Load() {
				return
			}
			t.pending.Range(func(key, value interface{}) bool {
				seq := key.(uint64)
				entry := value.(*pendingEntry)

				entry.mu.Lock()
				defer entry.mu.Unlock()

				age := time.Since(entry.sentAt)
				rto := initialRTO * time.Duration(1<<uint(entry.attempts))
				if rto > maxRTO {
					rto = maxRTO
				}
				if age < rto {
					return true
				}

				if entry.attempts >= maxRetransmitAttempts {
					utils.Debugf("[OFSP] dropping packet seq=%d after %d attempts",
						seq, entry.attempts)
					t.pending.Delete(seq)
					return true
				}

				if err := t.Transport.Send(entry.envelope); err != nil {
					utils.Debugf("[OFSP] retransmit seq=%d failed: %v", seq, err)
					t.pending.Delete(seq)
					return true
				}
				entry.attempts++
				entry.sentAt = time.Now()
				utils.Debugf("[OFSP] retransmit seq=%d attempt=%d", seq, entry.attempts)
				return true
			})
		}
	}
}

// dedupCleanupLoop evicts stale entries from the inbound dedup map.
func (t *OFSPTransport) dedupCleanupLoop() {
	ticker := time.NewTicker(dedupCleanupTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !t.running.Load() {
				return
			}
			t.receivedSeqs.Range(func(key, value interface{}) bool {
				e := value.(*dedupEntry)
				if time.Since(e.seenAt) > dedupTTL {
					t.receivedSeqs.Delete(key)
				}
				return true
			})
		}
	}
}
