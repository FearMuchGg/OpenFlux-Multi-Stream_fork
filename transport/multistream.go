package transport

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"universal-bypass-tool/utils"
)

// flushTick is how often the retry buffer is re-examined when nothing else
// wakes it. It only matters while the buffer is non-empty and every stream is
// refusing packets, so it can be coarse.
const flushTick = 250 * time.Millisecond

// MultiStreamConfig tunes the resilience layer. The zero value is not usable;
// start from DefaultMultiStreamConfig.
type MultiStreamConfig struct {
	// Retry buffer bounds. A packet no stream would accept is held briefly
	// instead of dropped, so a short outage does not force the inner TCP to
	// wait out its own RTO before retrying.
	//
	// Both bounds are enforced: a packet can be ~64 KiB, so a count-only limit
	// of 4096 could pin 256 MiB.
	MaxBufferPackets int
	MaxBufferBytes   int
	MaxPacketAge     time.Duration

	// Flow-affinity table bounds.
	MaxFlowEntries int
	FlowEntryTTL   time.Duration

	// Quarantine: how many consecutive Send failures retire a stream, and how
	// long it stays out (growing exponentially up to QuarantineMax).
	QuarantineAfter int
	QuarantineBase  time.Duration
	QuarantineMax   time.Duration
}

// DefaultMultiStreamConfig returns sane defaults for a tunnel on a small VPS.
func DefaultMultiStreamConfig() MultiStreamConfig {
	return MultiStreamConfig{
		MaxBufferPackets: 4096,
		MaxBufferBytes:   8 << 20,
		MaxPacketAge:     2 * time.Second,
		MaxFlowEntries:   16384,
		FlowEntryTTL:     60 * time.Second,
		QuarantineAfter:  3,
		QuarantineBase:   5 * time.Second,
		QuarantineMax:    60 * time.Second,
	}
}

// streamHealth is what MultiStream has observed about one inner stream,
// independently of what that stream claims about itself. The concrete
// transports cannot be fully trusted here: vyandex, for instance, used to keep
// reporting IsConnected() == true forever after its socket died, which made the
// skip-dead-stream logic a no-op.
type streamHealth struct {
	sendErrors     atomic.Uint64
	consecutiveErr atomic.Int32
	quarantineLvl  atomic.Int32
	quarantineTill atomic.Int64 // unix nanos; 0 means not quarantined
}

// flowEntry is the sticky pin for one inner TCP connection. Both fields are
// atomics so the hot path only needs the read lock.
type flowEntry struct {
	streamIdx atomic.Uint32
	lastSeen  atomic.Int64 // unix nanos
}

// bufferedPacket is a packet held while every stream was unavailable.
type bufferedPacket struct {
	key    FlowKey
	hasKey bool
	data   []byte
	at     time.Time
}

// MultiStreamTransport fans a single logical tunnel out over N inner transports
// (issue #50). It is the UNION of the inner channels: one stream going down does
// not stop the tunnel, because the remaining ones keep carrying traffic.
//
// Two things make that actually true in practice, and both were missing before:
//
//  1. Stream selection is per-FLOW, not per-packet. Hashing the inner IP 4-tuple
//     and pinning the connection to one document keeps its packets in order.
//     Spreading them round-robin across documents with different RTTs made the
//     receiving stack see constant reordering — duplicate ACKs, fast retransmit,
//     collapsed congestion window — and, worse, made every retransmission take
//     another independent 1/N chance of landing on a dead stream.
//
//  2. A packet that no stream will take is buffered for a bounded time instead
//     of being dropped, so a short outage costs nothing. Beyond that window the
//     inner TCP's own retransmission takes over.
//
// Streams that keep refusing packets are quarantined with exponential backoff,
// so the tunnel stops wasting 1/N of its traffic on a black hole.
type MultiStreamTransport struct {
	streams []Transport
	health  []*streamHealth

	// Monotonic counter for the rotating fallback scan (packets we cannot
	// attribute to a flow: non-TCP, fragments, or a caller using plain Send).
	// Atomic, so the hot path stays lock-free.
	idx     atomic.Uint64
	running atomic.Bool

	mu     sync.RWMutex
	userCb func([]byte)

	startTime atomic.Int64 // unix nanos; written in Start, read in Stats

	// Sticky flow -> stream pins. Guarded by flowsMu; the hot path takes the
	// read lock and only touches the atomics inside flowEntry.
	flowsMu sync.RWMutex
	flows   map[FlowKey]*flowEntry

	// Retry buffer. A slice with a moving head rather than a channel, so the
	// flush loop can peek, age out and replay in FIFO order without
	// busy-spinning when every stream stays down.
	bufMu    sync.Mutex
	buf      []*bufferedPacket
	head     int
	bufBytes int

	droppedOld  atomic.Uint64
	droppedFull atomic.Uint64
	replayed    atomic.Uint64

	// Rate-limits the "no usable stream" warning.
	lastNoRouteLog atomic.Int64

	wake     chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once

	// flushStarted / flushExited let Stop wait for the flush loop, so it cannot
	// hand a packet to a transport that is being torn down.
	flushStarted atomic.Bool
	flushExited  chan struct{}

	cfg MultiStreamConfig
}

// NewMultiStreamTransport wraps inners into a single logical transport with
// default resilience settings. Callers pass at least one inner; a length-1
// slice is legal but pointless (main.go avoids it — a single URL builds the
// inner transport directly).
func NewMultiStreamTransport(inners []Transport) *MultiStreamTransport {
	return NewMultiStreamTransportWithConfig(inners, DefaultMultiStreamConfig())
}

// NewMultiStreamTransportWithConfig is NewMultiStreamTransport with explicit
// buffer, affinity and quarantine limits.
func NewMultiStreamTransportWithConfig(inners []Transport, cfg MultiStreamConfig) *MultiStreamTransport {
	streams := make([]Transport, len(inners))
	copy(streams, inners)

	m := &MultiStreamTransport{
		streams:     streams,
		health:      make([]*streamHealth, len(streams)),
		flows:       make(map[FlowKey]*flowEntry),
		wake:        make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		flushExited: make(chan struct{}),
		cfg:         cfg,
	}
	for i := range m.health {
		m.health[i] = &streamHealth{}
	}
	m.startTime.Store(time.Now().UnixNano())
	return m
}

// Start brings every inner transport up, in parallel.
//
// A stream that fails to start is quarantined and the tunnel carries on with
// the survivors: one unreachable document out of N must not take the whole
// tunnel down. (vyandex's Start performs a synchronous authorize that fails
// outright for an unreachable document, so a sequential fail-fast Start used to
// make N-1 healthy documents useless.) Only a total failure is an error.
func (m *MultiStreamTransport) Start() error {
	m.running.Store(true)
	m.startTime.Store(time.Now().UnixNano())

	var wg sync.WaitGroup
	errs := make([]error, len(m.streams))
	for i, s := range m.streams {
		wg.Add(1)
		go func(i int, s Transport) {
			defer wg.Done()
			errs[i] = s.Start()
		}(i, s)
	}
	wg.Wait()

	now := time.Now()
	up := 0
	for i, err := range errs {
		if err != nil {
			utils.Debugf("[MULTI] inner[%d] failed to start: %v", i, err)
			m.health[i].quarantineTill.Store(now.Add(m.cfg.QuarantineBase).UnixNano())
			continue
		}
		up++
	}

	if up == 0 {
		m.running.Store(false)
		return fmt.Errorf("multistream: none of %d inner streams could be started", len(m.streams))
	}
	if up < len(m.streams) {
		utils.Debugf("[MULTI] started %d/%d streams; the tunnel continues on the rest", up, len(m.streams))
	}

	m.flushStarted.Store(true)
	go m.flushLoop()
	return nil
}

// Stop stops every inner transport, returning the first error but attempting
// them all. The flush loop is drained first so it cannot hand a packet to a
// stream that is being torn down.
func (m *MultiStreamTransport) Stop() error {
	m.running.Store(false)
	m.stopOnce.Do(func() { close(m.stopCh) })

	if m.flushStarted.Load() {
		select {
		case <-m.flushExited:
		case <-time.After(time.Second):
			utils.Debugf("[MULTI] flush loop did not stop in time, stopping streams anyway")
		}
	}

	var firstErr error
	for _, s := range m.streams {
		if err := s.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Send routes one frame without a flow identity: used for packets that are not
// part of a single TCP flow (non-TCP, IP fragments) and by callers that do not
// use SendFlow.
//
// A nil return means "accepted", not "transmitted": the packet may be sitting
// in the retry buffer waiting for a stream to come back. An error is returned
// only when the transport is not running or has no streams at all.
func (m *MultiStreamTransport) Send(data []byte) error {
	return m.route(FlowKey{}, false, data)
}

// SendFlow routes one frame using the identity of the inner TCP connection it
// belongs to, so every packet of that connection stays on one document.
//
// Implements FlowAwareSender. The wrappers above (Compressed, Encrypted)
// forward the key down to here; MultiStream sits below them and never sees the
// IP header itself.
func (m *MultiStreamTransport) SendFlow(key FlowKey, data []byte) error {
	return m.route(key, true, data)
}

func (m *MultiStreamTransport) route(key FlowKey, hasKey bool, data []byte) error {
	if !m.running.Load() {
		return fmt.Errorf("multistream: not running")
	}
	if len(m.streams) == 0 {
		return fmt.Errorf("multistream: no inner streams")
	}

	now := time.Now().UnixNano()
	if m.tryStreams(key, hasKey, data, now) {
		return nil
	}

	// Every stream refused. Hold the packet briefly rather than dropping it:
	// otherwise the inner TCP has to wait out a full RTO before retrying, and
	// with several streams in play that wait compounds.
	m.warnNoRoute()
	m.enqueue(key, hasKey, data)
	return nil
}

// tryStreams makes one pass over the candidate streams and reports whether some
// stream accepted the packet.
//
// Streams are considered in two tiers: those that are demonstrably carrying
// traffic first, then the rest. A stream that looks dead to the liveness probe
// is only used when nothing better is available. It is deprioritised, never
// discarded — a probe can fail for reasons that have nothing to do with the
// tunnel, and treating that as death once took a working tunnel down for good.
//
// Shared by the normal send path and the retry-buffer flush, so a replayed
// packet follows exactly the same affinity as a fresh one and per-flow order is
// preserved.
func (m *MultiStreamTransport) tryStreams(key FlowKey, hasKey bool, data []byte, now int64) bool {
	n := len(m.streams)

	// The flow's pinned stream goes first, as long as it still looks alive.
	// Keeping a connection on one stream is what preserves its packet order.
	pinned, pinnedTried := -1, false
	if hasKey {
		if idx, ok := m.affinity(key, now); ok {
			pinned = idx
			if m.streamAlive(idx) {
				pinnedTried = true
				if m.sendTo(idx, data) {
					return true
				}
				// The pin is stale. Forget it so the next packet of this flow
				// re-picks instead of retrying the same stream.
				m.clearPin(key)
			}
		}
	}

	start := m.idx.Add(1) - 1
	for _, wantAlive := range [2]bool{true, false} {
		for i := uint64(0); i < uint64(n); i++ {
			idx := int((start + i) % uint64(n))
			if pinnedTried && idx == pinned {
				continue
			}
			if !m.streamUsable(idx, now) || m.streamAlive(idx) != wantAlive {
				continue
			}
			if m.sendTo(idx, data) {
				return true
			}
		}
	}
	return false
}

// sendTo hands one frame to a single stream and records the outcome.
func (m *MultiStreamTransport) sendTo(idx int, data []byte) bool {
	if err := m.streams[idx].Send(data); err != nil {
		m.noteFailure(idx)
		return false
	}
	m.noteSuccess(idx)
	return true
}

// streamAlive reports whether a stream is demonstrably carrying traffic. A
// transport that cannot report health is assumed alive, so this is purely an
// optimisation and never a way to disable a stream.
func (m *MultiStreamTransport) streamAlive(idx int) bool {
	if hr, ok := m.streams[idx].(HealthReporter); ok {
		return hr.Alive()
	}
	return true
}

// warnNoRoute logs, at most once every 30 seconds, that every stream is
// unusable. It reports how many still look alive, because "no usable stream
// while frames are arriving" is the signature of a stream being wrongly marked
// disconnected — the failure this code exists to prevent.
func (m *MultiStreamTransport) warnNoRoute() {
	now := time.Now().UnixNano()
	last := m.lastNoRouteLog.Load()
	if now-last < int64(30*time.Second) || !m.lastNoRouteLog.CompareAndSwap(last, now) {
		return
	}
	alive := 0
	for i := range m.streams {
		if m.streamAlive(i) {
			alive++
		}
	}
	log.Printf("[MULTI] no usable stream (%d of %d still look alive); buffering packets",
		alive, len(m.streams))
}

// ---- stream health ----

// streamUsable reports whether a stream may be selected: it claims to be
// connected AND MultiStream has not quarantined it.
func (m *MultiStreamTransport) streamUsable(idx int, now int64) bool {
	if !m.streams[idx].IsConnected() {
		return false
	}
	return m.health[idx].quarantineTill.Load() <= now
}

func (m *MultiStreamTransport) noteSuccess(idx int) {
	h := m.health[idx]
	h.consecutiveErr.Store(0)
	h.quarantineLvl.Store(0)
	h.quarantineTill.Store(0)
}

func (m *MultiStreamTransport) noteFailure(idx int) {
	h := m.health[idx]
	h.sendErrors.Add(1)

	if int(h.consecutiveErr.Add(1)) < m.cfg.QuarantineAfter {
		return
	}
	h.consecutiveErr.Store(0)

	lvl := h.quarantineLvl.Add(1)
	if lvl > 16 {
		lvl = 16
	}
	d := m.cfg.QuarantineBase << (lvl - 1)
	if d <= 0 || d > m.cfg.QuarantineMax {
		d = m.cfg.QuarantineMax
	}
	h.quarantineTill.Store(time.Now().Add(d).UnixNano())
	utils.Debugf("[MULTI] stream %d quarantined for %v (level %d)", idx, d, lvl)
}

// ---- flow affinity ----

// affinity returns the stream currently pinned to key, choosing and remembering
// a new one when the pin is missing, dead or quarantined.
func (m *MultiStreamTransport) affinity(key FlowKey, now int64) (int, bool) {
	n := len(m.streams)

	m.flowsMu.RLock()
	e := m.flows[key]
	m.flowsMu.RUnlock()

	if e != nil {
		if idx := int(e.streamIdx.Load()); idx < n && m.streamUsable(idx, now) {
			e.lastSeen.Store(now)
			return idx, true
		}
	}

	// Rotate the starting point instead of deriving it from the flow hash.
	//
	// Hashing piled an uneven number of flows onto the same documents, so a
	// browser opening six connections to one host could land them all on one
	// document while another sat idle. Rotation spreads new flows evenly. The
	// sticky pin above is untouched, so an individual connection still keeps
	// every one of its packets on the same document and in order.
	start := int((m.idx.Add(1) - 1) % uint64(n))
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if !m.streamUsable(idx, now) {
			continue
		}
		m.pinFlow(key, idx, now)
		return idx, true
	}
	return 0, false
}

func (m *MultiStreamTransport) pinFlow(key FlowKey, idx int, now int64) {
	m.flowsMu.Lock()
	e := m.flows[key]
	if e == nil {
		e = &flowEntry{}
		m.flows[key] = e
		m.evictFlowsLocked(now)
	}
	e.streamIdx.Store(uint32(idx))
	e.lastSeen.Store(now)
	m.flowsMu.Unlock()
}

func (m *MultiStreamTransport) clearPin(key FlowKey) {
	m.flowsMu.Lock()
	delete(m.flows, key)
	m.flowsMu.Unlock()
}

// evictFlowsLocked keeps the affinity table bounded. A dropped pin is harmless:
// the next packet of that flow simply re-picks a stream, which is exactly what
// a brand new flow does.
func (m *MultiStreamTransport) evictFlowsLocked(now int64) {
	if len(m.flows) <= m.cfg.MaxFlowEntries {
		return
	}

	ttl := int64(m.cfg.FlowEntryTTL)
	for k, e := range m.flows {
		if now-e.lastSeen.Load() > ttl {
			delete(m.flows, k)
		}
	}
	if len(m.flows) <= m.cfg.MaxFlowEntries {
		return
	}

	over := len(m.flows) - m.cfg.MaxFlowEntries
	for k := range m.flows {
		if over <= 0 {
			break
		}
		delete(m.flows, k)
		over--
	}
}

// ---- retry buffer ----

func (m *MultiStreamTransport) enqueue(key FlowKey, hasKey bool, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)

	m.bufMu.Lock()
	m.buf = append(m.buf, &bufferedPacket{key: key, hasKey: hasKey, data: cp, at: time.Now()})
	m.bufBytes += len(cp)
	for m.bufferedLocked() > m.cfg.MaxBufferPackets ||
		(m.bufBytes > m.cfg.MaxBufferBytes && m.bufferedLocked() > 1) {
		if !m.evictOldestLocked() {
			break
		}
		m.droppedFull.Add(1)
	}
	m.bufMu.Unlock()

	m.wakeFlush()
}

func (m *MultiStreamTransport) wakeFlush() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *MultiStreamTransport) bufferedLocked() int {
	return len(m.buf) - m.head
}

func (m *MultiStreamTransport) evictOldestLocked() bool {
	if m.head >= len(m.buf) {
		return false
	}
	m.bufBytes -= len(m.buf[m.head].data)
	m.buf[m.head] = nil
	m.head++
	m.compactLocked()
	return true
}

// compactLocked reclaims the front of the slice once enough has been consumed.
func (m *MultiStreamTransport) compactLocked() {
	if m.head == 0 {
		return
	}
	if m.head >= len(m.buf) {
		m.buf = m.buf[:0]
		m.head = 0
		return
	}
	if m.head >= 1024 && m.head*2 >= len(m.buf) {
		m.buf = append(m.buf[:0], m.buf[m.head:]...)
		m.head = 0
	}
}

func (m *MultiStreamTransport) flushLoop() {
	defer close(m.flushExited)

	ticker := time.NewTicker(flushTick)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-m.wake:
		case <-ticker.C:
		}
		m.flushBuffered()
	}
}

// flushBuffered replays buffered packets in FIFO order. It stops at the first
// packet that no stream will take, keeping that packet at the head so ordering
// is preserved, and returns — the wake channel or the ticker will bring it back.
func (m *MultiStreamTransport) flushBuffered() {
	for {
		if !m.running.Load() {
			return
		}

		m.bufMu.Lock()
		if m.bufferedLocked() == 0 {
			m.bufMu.Unlock()
			return
		}
		pkt := m.buf[m.head]
		if time.Since(pkt.at) > m.cfg.MaxPacketAge {
			// Older than the window we promise to cover; the inner TCP has
			// already retransmitted it, so replaying it now would only add
			// duplicate traffic.
			m.evictOldestLocked()
			m.bufMu.Unlock()
			m.droppedOld.Add(1)
			continue
		}
		m.bufMu.Unlock()

		now := time.Now().UnixNano()
		if !m.tryStreams(pkt.key, pkt.hasKey, pkt.data, now) {
			return
		}

		m.bufMu.Lock()
		// Identity check: a concurrent enqueue may have evicted this packet
		// while we were sending it, in which case the head has moved on.
		if m.head < len(m.buf) && m.buf[m.head] == pkt {
			m.evictOldestLocked()
		}
		m.bufMu.Unlock()
		m.replayed.Add(1)
	}
}

// ---- observation ----

// Receive registers cb for the union of all inner streams. Frames from any
// stream go up unchanged; the layer above is responsible for demuxing.
func (m *MultiStreamTransport) Receive(cb func([]byte)) {
	m.mu.Lock()
	m.userCb = cb
	m.mu.Unlock()

	// Wire every inner stream's callback back to us. This is safe to call
	// before or after Start because each inner transport buffers its own state.
	for _, s := range m.streams {
		s.Receive(func(data []byte) {
			m.mu.RLock()
			u := m.userCb
			m.mu.RUnlock()
			if u != nil {
				u(data)
			}
		})
	}
}

// IsConnected reports true when at least one stream is both connected and not
// quarantined. The tunnel is usable while any stream survives.
func (m *MultiStreamTransport) IsConnected() bool {
	now := time.Now().UnixNano()
	for i := range m.streams {
		if m.streamUsable(i, now) {
			return true
		}
	}
	return false
}

// Stats aggregates counters across inner streams. Connected is the OR over
// usable streams. Uptime is measured from the MultiStream's own Start.
func (m *MultiStreamTransport) Stats() TransportStats {
	var agg TransportStats
	for _, s := range m.streams {
		st := s.Stats()
		agg.BytesSent += st.BytesSent
		agg.BytesReceived += st.BytesReceived
		agg.PacketsSent += st.PacketsSent
		agg.PacketsRecv += st.PacketsRecv
		agg.Reconnects += st.Reconnects
	}
	agg.Connected = m.IsConnected()
	agg.Uptime = time.Since(time.Unix(0, m.startTime.Load()))
	return agg
}

// Buffered reports how many packets and bytes are currently held in the retry
// buffer, waiting for a stream to recover.
func (m *MultiStreamTransport) Buffered() (packets int, bytes int) {
	m.bufMu.Lock()
	defer m.bufMu.Unlock()
	return m.bufferedLocked(), m.bufBytes
}

// Dropped reports packets discarded from the retry buffer: old (past
// MaxPacketAge, the inner TCP has already retransmitted them) and full
// (evicted to respect the byte/packet caps).
func (m *MultiStreamTransport) Dropped() (old uint64, full uint64) {
	return m.droppedOld.Load(), m.droppedFull.Load()
}

// Replayed reports how many buffered packets were successfully re-sent.
func (m *MultiStreamTransport) Replayed() uint64 {
	return m.replayed.Load()
}

// Streams exposes the inner transports for a status printer or a test. The
// returned slice is a copy: callers must not mutate the MultiStream's own
// backing array, but the elements are the live inner transports.
func (m *MultiStreamTransport) Streams() []Transport {
	out := make([]Transport, len(m.streams))
	copy(out, m.streams)
	return out
}

// Quarantined reports whether stream idx is currently held out of rotation.
func (m *MultiStreamTransport) Quarantined(idx int) bool {
	if idx < 0 || idx >= len(m.health) {
		return false
	}
	return m.health[idx].quarantineTill.Load() > time.Now().UnixNano()
}
