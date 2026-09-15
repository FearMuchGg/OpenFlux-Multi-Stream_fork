package transport

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sentRecord is one frame a mockStream accepted, together with the flow
// identity it was routed with.
type sentRecord struct {
	data []byte
	key  FlowKey
}

// mockStream is a Transport we can flip up/down and inspect. It implements
// FlowAwareSender and HealthReporter, so the affinity and liveness tests
// exercise the real paths.
type mockStream struct {
	mu        sync.Mutex
	connected atomic.Bool
	alive     atomic.Bool
	sent      []sentRecord
	cb        func([]byte)
	failSend  atomic.Bool
	startErr  error
}

func (m *mockStream) Start() error {
	if m.startErr != nil {
		return m.startErr
	}
	m.connected.Store(true)
	m.alive.Store(true)
	return nil
}

func (m *mockStream) Stop() error {
	m.connected.Store(false)
	m.alive.Store(false)
	return nil
}

// setAlive simulates what a transport reports when its liveness probe is not
// being answered and no traffic is arriving either.
func (m *mockStream) setAlive(v bool) { m.alive.Store(v) }

// HealthReporter. Note these are independent: a stream can be alive while the
// probe says otherwise, which is the whole point.
func (m *mockStream) Alive() bool   { return m.alive.Load() }
func (m *mockStream) Healthy() bool { return m.alive.Load() }

func (m *mockStream) ProbeStats() (sent, misses, pongs, latePongs uint64) { return 0, 0, 0, 0 }
func (m *mockStream) RxAge() time.Duration                                { return 0 }

func (m *mockStream) Send(data []byte) error { return m.record(data, FlowKey{}) }

func (m *mockStream) SendFlow(key FlowKey, data []byte) error { return m.record(data, key) }

func (m *mockStream) record(data []byte, key FlowKey) error {
	if m.failSend.Load() {
		return fmt.Errorf("mock: send failure")
	}
	cp := append([]byte(nil), data...)
	m.mu.Lock()
	m.sent = append(m.sent, sentRecord{data: cp, key: key})
	m.mu.Unlock()
	return nil
}

func (m *mockStream) Receive(cb func([]byte)) {
	m.mu.Lock()
	m.cb = cb
	m.mu.Unlock()
}

func (m *mockStream) IsConnected() bool     { return m.connected.Load() }
func (m *mockStream) Stats() TransportStats { return TransportStats{Connected: m.IsConnected()} }

func (m *mockStream) inject(data []byte) {
	m.mu.Lock()
	cb := m.cb
	m.mu.Unlock()
	if cb != nil {
		cb(data)
	}
}

func (m *mockStream) sentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sent)
}

func (m *mockStream) records() []sentRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sentRecord, len(m.sent))
	copy(out, m.sent)
	return out
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition was not met in time")
}

func testFlowKey() FlowKey {
	return FlowKey{
		Src:     [4]byte{10, 10, 10, 2},
		Dst:     [4]byte{203, 0, 113, 7},
		SrcPort: 40001,
		DstPort: 443,
	}
}

// ---- routing ----

// N=3 healthy streams should each get exactly 1/3 of unkeyed packets.
func TestMultiStreamRoundRobinHealthy(t *testing.T) {
	a, b, c := &mockStream{}, &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b, c})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	for i := 0; i < 30; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if a.sentCount() != 10 || b.sentCount() != 10 || c.sentCount() != 10 {
		t.Fatalf("uneven RR distribution: a=%d b=%d c=%d", a.sentCount(), b.sentCount(), c.sentCount())
	}
}

// A stream that reports IsConnected()=false must be skipped without hanging or
// erroring, and every packet must land on a healthy peer.
func TestMultiStreamSkipsDisconnected(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	a.Stop()

	for i := 0; i < 10; i++ {
		if err := ms.Send([]byte{1}); err != nil {
			t.Fatalf("Send %d over 1 live stream: %v", i, err)
		}
	}
	if a.sentCount() != 0 {
		t.Fatalf("dead stream received %d packets", a.sentCount())
	}
	if b.sentCount() != 10 {
		t.Fatalf("live stream missed packets: got %d", b.sentCount())
	}
}

// A healthy stream whose Send() returns an error (queue full, mid-reconnect)
// must NOT swallow the packet — MultiStream tries the next stream.
func TestMultiStreamFailoverOnSendError(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	_ = ms.Start()
	defer ms.Stop()

	a.failSend.Store(true)

	for i := 0; i < 5; i++ {
		if err := ms.Send([]byte{9}); err != nil {
			t.Fatalf("Send should have failed over to b: %v", err)
		}
	}
	if a.sentCount() != 0 {
		t.Fatalf("failing stream unexpectedly stored packets: %d", a.sentCount())
	}
	if b.sentCount() != 5 {
		t.Fatalf("failover target should have all 5: got %d", b.sentCount())
	}
}

// With every stream down a packet is buffered rather than dropped, and the call
// still reports success: nil means "accepted", not "transmitted".
func TestMultiStreamAllDownBuffersInsteadOfFailing(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	_ = ms.Start()
	defer ms.Stop()

	a.Stop()
	b.Stop()

	if err := ms.Send([]byte("x")); err != nil {
		t.Fatalf("Send should accept and buffer, got %v", err)
	}
	if pkts, _ := ms.Buffered(); pkts != 1 {
		t.Fatalf("expected the packet to be buffered, got %d", pkts)
	}
	if ms.IsConnected() {
		t.Fatal("IsConnected should be false when every stream is down")
	}
}

// Fan-in: frames from any stream must reach the user's single callback.
func TestMultiStreamReceiveFanIn(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	_ = ms.Start()
	defer ms.Stop()

	got := make(chan []byte, 4)
	ms.Receive(func(p []byte) { got <- p })
	a.inject([]byte("from-a"))
	b.inject([]byte("from-b"))

	var g1, g2 []byte
	select {
	case g1 = <-got:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first inject")
	}
	select {
	case g2 = <-got:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for second inject")
	}
	okOrder1 := bytes.Equal(g1, []byte("from-a")) && bytes.Equal(g2, []byte("from-b"))
	okOrder2 := bytes.Equal(g1, []byte("from-b")) && bytes.Equal(g2, []byte("from-a"))
	if !(okOrder1 || okOrder2) {
		t.Fatalf("unexpected fan-in: %q %q", g1, g2)
	}
}

// Stats must OR the Connected bit over inner streams.
func TestMultiStreamStatsAggregation(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	_ = ms.Start()
	defer ms.Stop()

	if !ms.Stats().Connected {
		t.Fatal("Stats.Connected should be true when both up")
	}
	a.Stop()
	if !ms.Stats().Connected {
		t.Fatal("Stats.Connected should stay true while ANY stream is up")
	}
	b.Stop()
	if ms.Stats().Connected {
		t.Fatal("Stats.Connected should be false when all down")
	}
}

// A stream that comes back after being marked down must resume receiving
// traffic on the next round-robin tick.
func TestMultiStreamRecoversAfterFlap(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	_ = ms.Start()
	defer ms.Stop()

	for i := 0; i < 10; i++ {
		_ = ms.Send([]byte{1})
	}
	beforeA, beforeB := a.sentCount(), b.sentCount()
	if beforeA == 0 || beforeB == 0 {
		t.Fatalf("initial distribution missed a stream: a=%d b=%d", beforeA, beforeB)
	}

	a.Stop()
	for i := 0; i < 10; i++ {
		_ = ms.Send([]byte{2})
	}
	if a.sentCount() != beforeA {
		t.Fatal("dead a should not have received traffic")
	}
	if b.sentCount()-beforeB != 10 {
		t.Fatalf("live b should have absorbed all 10 while a was down, got %d", b.sentCount()-beforeB)
	}

	a.Start()
	beforeA2 := a.sentCount()
	for i := 0; i < 10; i++ {
		_ = ms.Send([]byte{3})
	}
	if a.sentCount()-beforeA2 == 0 {
		t.Fatal("recovered a should be receiving traffic again")
	}
}

// ---- flow affinity ----

// Every packet of one inner TCP connection must go to the same document:
// spreading them across documents with different RTTs is what caused constant
// reordering and spurious retransmits.
func TestMultiStreamFlowAffinityStable(t *testing.T) {
	a, b, c := &mockStream{}, &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b, c})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	key := testFlowKey()
	for i := 0; i < 50; i++ {
		if err := ms.SendFlow(key, []byte{byte(i)}); err != nil {
			t.Fatalf("SendFlow %d: %v", i, err)
		}
	}

	owner := -1
	for i, s := range []*mockStream{a, b, c} {
		if s.sentCount() == 0 {
			continue
		}
		if owner != -1 {
			t.Fatalf("one flow was spread across streams %d and %d", owner, i)
		}
		owner = i
	}
	if owner == -1 {
		t.Fatal("no stream received the flow")
	}
	if got := []*mockStream{a, b, c}[owner].sentCount(); got != 50 {
		t.Fatalf("pinned stream got %d of 50 packets", got)
	}
}

// When the pinned document dies the flow must move, and it must NOT be yanked
// back when that document recovers — migrating a live connection mid-stream
// would reorder it for no benefit.
func TestMultiStreamFlowAffinityFailoverSticks(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	// Only a is usable, so the flow pins to a.
	b.Stop()
	key := testFlowKey()
	if err := ms.SendFlow(key, []byte("warm")); err != nil {
		t.Fatal(err)
	}
	if a.sentCount() != 1 {
		t.Fatalf("warm-up should have pinned to a: a=%d b=%d", a.sentCount(), b.sentCount())
	}

	// a dies, b comes back.
	a.Stop()
	b.Start()
	for i := 0; i < 5; i++ {
		if err := ms.SendFlow(key, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if b.sentCount() != 5 {
		t.Fatalf("flow should have failed over to b, b=%d", b.sentCount())
	}

	// a recovers: the flow must stay on b.
	a.Start()
	before := b.sentCount()
	for i := 0; i < 5; i++ {
		if err := ms.SendFlow(key, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if b.sentCount()-before != 5 {
		t.Fatalf("recovered stream stole the flow back: b got %d of 5", b.sentCount()-before)
	}
}

// Packets that are not part of a single TCP flow (ICMP, fragments, truncated
// frames) must still be routed, via the rotating scan.
func TestMultiStreamUnkeyedPacketsUseRotation(t *testing.T) {
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	for i := 0; i < 10; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if a.sentCount() != 5 || b.sentCount() != 5 {
		t.Fatalf("unkeyed packets should rotate evenly: a=%d b=%d", a.sentCount(), b.sentCount())
	}
	for _, rec := range a.records() {
		if rec.key != (FlowKey{}) {
			t.Fatal("plain Send must not invent a flow key")
		}
	}
}

// ---- retry buffer ----

func TestMultiStreamBufferReplaysInOrder(t *testing.T) {
	a := &mockStream{}
	cfg := DefaultMultiStreamConfig()
	cfg.MaxPacketAge = time.Hour
	ms := NewMultiStreamTransportWithConfig([]Transport{a}, cfg)
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	a.Stop()

	for i := 0; i < 3; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if pkts, _ := ms.Buffered(); pkts != 3 {
		t.Fatalf("expected 3 buffered packets, got %d", pkts)
	}
	if a.sentCount() != 0 {
		t.Fatal("a stopped stream must not have received anything")
	}

	a.Start()
	waitFor(t, 2*time.Second, func() bool { p, _ := ms.Buffered(); return p == 0 })

	if a.sentCount() != 3 {
		t.Fatalf("expected 3 replayed packets, got %d", a.sentCount())
	}
	if ms.Replayed() != 3 {
		t.Fatalf("expected 3 replayed, got %d", ms.Replayed())
	}
	for i, rec := range a.records() {
		if len(rec.data) != 1 || rec.data[0] != byte(i) {
			t.Fatalf("replay out of order at %d: %v", i, rec.data)
		}
	}
}

func TestMultiStreamBufferEvictsByPacketCount(t *testing.T) {
	a := &mockStream{}
	cfg := DefaultMultiStreamConfig()
	cfg.MaxBufferPackets = 4
	cfg.MaxPacketAge = time.Hour
	ms := NewMultiStreamTransportWithConfig([]Transport{a}, cfg)
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	a.Stop()
	for i := 0; i < 10; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}

	if pkts, _ := ms.Buffered(); pkts != 4 {
		t.Fatalf("buffer should be capped at 4 packets, got %d", pkts)
	}
	if _, full := ms.Dropped(); full != 6 {
		t.Fatalf("expected 6 evictions, got %d", full)
	}

	a.Start()
	waitFor(t, 2*time.Second, func() bool { p, _ := ms.Buffered(); return p == 0 })

	recs := a.records()
	if len(recs) != 4 {
		t.Fatalf("expected 4 replayed, got %d", len(recs))
	}
	for i, rec := range recs {
		if rec.data[0] != byte(6+i) {
			t.Fatalf("eviction should drop the oldest; got %d at position %d", rec.data[0], i)
		}
	}
}

// A packet can be ~64 KiB, so the buffer must also be bounded by bytes — a
// count-only limit would let it pin hundreds of megabytes.
func TestMultiStreamBufferEvictsByBytes(t *testing.T) {
	a := &mockStream{}
	cfg := DefaultMultiStreamConfig()
	cfg.MaxBufferBytes = 1000
	cfg.MaxPacketAge = time.Hour
	ms := NewMultiStreamTransportWithConfig([]Transport{a}, cfg)
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	a.Stop()
	for i := 0; i < 20; i++ {
		if err := ms.Send(make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
	}

	pkts, bytesHeld := ms.Buffered()
	if bytesHeld > 1000 {
		t.Fatalf("byte cap exceeded: %d bytes held", bytesHeld)
	}
	if pkts == 0 || pkts >= 20 {
		t.Fatalf("expected partial retention under the byte cap, got %d", pkts)
	}
}

// Packets older than MaxPacketAge are dropped: the inner TCP has already
// retransmitted them, so replaying them would only duplicate traffic.
func TestMultiStreamBufferDropsStalePackets(t *testing.T) {
	a := &mockStream{}
	cfg := DefaultMultiStreamConfig()
	cfg.MaxPacketAge = 20 * time.Millisecond
	ms := NewMultiStreamTransportWithConfig([]Transport{a}, cfg)
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	a.Stop()
	if err := ms.Send([]byte("stale")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool {
		old, _ := ms.Dropped()
		return old > 0
	})
	if pkts, _ := ms.Buffered(); pkts != 0 {
		t.Fatalf("stale packet should have been dropped, %d left", pkts)
	}
}

// ---- startup ----

// One unreachable document out of N must not take the tunnel down: the whole
// point of multi-stream is that the survivors carry on.
func TestMultiStreamStartToleratesPartialFailure(t *testing.T) {
	good1 := &mockStream{}
	bad := &mockStream{startErr: fmt.Errorf("document unreachable")}
	good2 := &mockStream{}

	ms := NewMultiStreamTransport([]Transport{good1, bad, good2})
	if err := ms.Start(); err != nil {
		t.Fatalf("one bad document must not kill the tunnel: %v", err)
	}
	defer ms.Stop()

	if !ms.IsConnected() {
		t.Fatal("tunnel should be up on the two good streams")
	}
	for i := 0; i < 20; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if bad.sentCount() != 0 {
		t.Fatalf("failed stream received %d packets", bad.sentCount())
	}
	if got := good1.sentCount() + good2.sentCount(); got != 20 {
		t.Fatalf("good streams got %d of 20", got)
	}
}

// Only a total failure is fatal.
func TestMultiStreamStartFailsWhenNothingComesUp(t *testing.T) {
	a := &mockStream{startErr: fmt.Errorf("nope")}
	b := &mockStream{startErr: fmt.Errorf("nope")}

	ms := NewMultiStreamTransport([]Transport{a, b})
	if err := ms.Start(); err == nil {
		t.Fatal("expected an error when no stream can be started")
	}
	if ms.IsConnected() {
		t.Fatal("a tunnel with no streams must not report connected")
	}
}

// A stream that keeps refusing packets is quarantined, so the tunnel stops
// wasting a share of its traffic on a black hole.
func TestMultiStreamQuarantinesFailingStream(t *testing.T) {
	cfg := DefaultMultiStreamConfig()
	cfg.QuarantineAfter = 3
	cfg.QuarantineBase = time.Minute
	a, b := &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransportWithConfig([]Transport{a, b}, cfg)
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	a.failSend.Store(true)
	for i := 0; i < 10; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	if !ms.Quarantined(0) {
		t.Fatal("a stream failing every send must be quarantined")
	}
	if b.sentCount() != 10 {
		t.Fatalf("healthy stream should have absorbed all 10, got %d", b.sentCount())
	}
}

// ---- liveness ----

// An idle but perfectly healthy channel receives no data, so an idle-timeout
// heuristic would kill it. Here the probes are answered.
func TestLivenessIdleChannelStaysHealthy(t *testing.T) {
	live := NewLivenessTracker(5 * time.Millisecond)

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if nonce := live.NextProbe(); nonce != 0 {
			live.OnPong(nonce) // peer answers, but no data ever flows
		}
		time.Sleep(time.Millisecond)
	}

	if !live.Healthy() || !live.Alive() {
		t.Fatal("an idle but responsive channel must stay healthy and alive")
	}
	sent, misses, pongs, late := live.ProbeStats()
	if misses != 0 || late != 0 {
		t.Fatalf("unexpected misses/late pongs: %d/%d", misses, late)
	}
	if sent == 0 || pongs == 0 {
		t.Fatalf("expected probes to have been sent and answered, got %d/%d", sent, pongs)
	}
}

// A channel that answers nothing and delivers nothing is genuinely suspect.
func TestLivenessUnansweredProbeMarksSuspect(t *testing.T) {
	live := NewLivenessTracker(5 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && live.Healthy() {
		live.NextProbe() // never answered
		time.Sleep(time.Millisecond)
	}

	if live.Healthy() {
		t.Fatal("a channel that never answers must not report healthy")
	}
	if !live.Suspect() {
		t.Fatal("with no data either, the stream must be suspect")
	}
	if _, misses, _, _ := live.ProbeStats(); misses == 0 {
		t.Fatal("expected recorded misses")
	}
}

// The regression this guards: a probe can fail for reasons that have nothing to
// do with the tunnel, and treating that as death took a working tunnel down for
// good. A stream that is delivering validated frames is ALIVE whatever the
// probe says — while still reporting the probe as unhealthy, so a half-broken
// outbound path stays visible.
func TestLivenessDataKeepsStreamAliveDespiteProbeFailure(t *testing.T) {
	live := NewLivenessTracker(5 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && live.Healthy() {
		live.NextProbe()    // never answered
		live.OnValidFrame() // but traffic keeps arriving
		time.Sleep(time.Millisecond)
	}

	if live.Healthy() {
		t.Fatal("inbound data must not be treated as a probe acknowledgement")
	}
	if !live.Alive() {
		t.Fatal("a stream delivering validated frames must stay alive")
	}
	if live.Suspect() {
		t.Fatal("such a stream must not be suspect")
	}
	if live.ValidFrames() == 0 {
		t.Fatal("expected validated frames to be counted")
	}
}

// A stream stops being alive once its frames stop, and comes back when they
// resume. Both directions matter: this is what keeps a stream in rotation while
// it is working, and lets it drop out when it genuinely stops.
func TestLivenessAliveExpiresWithoutFrames(t *testing.T) {
	live := NewLivenessTracker(10 * time.Millisecond) // dataWindow = 30ms

	// Drive the probe to failure first, otherwise Healthy() is still the
	// initial optimistic true.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && live.Healthy() {
		live.NextProbe()
		time.Sleep(time.Millisecond)
	}
	if live.Healthy() {
		t.Fatal("expected the probe to time out first")
	}

	live.OnValidFrame()
	if !live.Alive() {
		t.Fatal("a fresh validated frame must make the stream alive")
	}

	time.Sleep(60 * time.Millisecond)
	if live.Alive() {
		t.Fatal("with no further frames the stream must stop being alive")
	}
}

// A pong that arrives after its probe already timed out still proves the stream
// works; it is counted separately as late, which is the signature of a probe
// that is delivered but slower than its timeout.
func TestLivenessLatePongCountedAndRestoresHealth(t *testing.T) {
	live := NewLivenessTracker(5 * time.Millisecond)

	first := live.NextProbe()
	if first == 0 {
		t.Fatal("expected a probe to be issued")
	}

	// Let that probe time out and be replaced, so `first` is no longer the
	// outstanding one and the link is already unhealthy.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && live.Healthy() {
		live.NextProbe()
		time.Sleep(time.Millisecond)
	}
	if live.Healthy() {
		t.Fatal("expected the probe to time out first")
	}
	if sent, _, _, _ := live.ProbeStats(); sent < 2 {
		t.Fatalf("expected at least one replacement probe, got %d sent", sent)
	}

	// The answer to the first probe finally arrives.
	live.OnPong(first)

	if !live.Healthy() {
		t.Fatal("any pong must restore health")
	}
	if _, _, _, late := live.ProbeStats(); late == 0 {
		t.Fatal("a pong for an abandoned probe must be counted as late")
	}
}

func TestLivenessRecoversAfterPong(t *testing.T) {
	live := NewLivenessTracker(5 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && live.Healthy() {
		live.NextProbe()
		time.Sleep(time.Millisecond)
	}
	if live.Healthy() {
		t.Fatal("expected the link to be declared unhealthy first")
	}

	recoverBy := time.Now().Add(2 * time.Second)
	for time.Now().Before(recoverBy) && !live.Healthy() {
		if nonce := live.NextProbe(); nonce != 0 {
			live.OnPong(nonce)
		}
		time.Sleep(time.Millisecond)
	}
	if !live.Healthy() {
		t.Fatal("an answered probe must restore health")
	}
}

// A disabled tracker must never probe and must always look usable, so a peer
// that does not answer is not mistaken for a dead document.
func TestLivenessDisabledNeverGoesUnhealthy(t *testing.T) {
	live := NewDisabledLivenessTracker()
	for i := 0; i < 100; i++ {
		if nonce := live.NextProbe(); nonce != 0 {
			t.Fatal("a disabled tracker must not produce probes")
		}
	}
	if !live.Healthy() || !live.Alive() || live.Suspect() {
		t.Fatal("a disabled tracker must always report healthy and alive")
	}
}

// ---- stream selection by liveness ----

// The core regression test: a stream whose probe is failing but which is still
// carrying traffic must stay in rotation. Before this, such a stream was marked
// disconnected and never came back, turning the tunnel permanently one-way.
func TestMultiStreamKeepsAliveStreamDespiteProbeFailure(t *testing.T) {
	a := &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	// The transport reports "not answering the probe, but frames are arriving".
	a.setAlive(true)

	for i := 0; i < 5; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if a.sentCount() != 5 {
		t.Fatalf("an alive stream must keep receiving traffic, got %d of 5", a.sentCount())
	}
	if !ms.IsConnected() {
		t.Fatal("the tunnel must still report connected")
	}
}

// A stream that is neither answering nor delivering is deprioritised, but must
// still be used when nothing better exists — the probe is a hint, not a verdict.
func TestMultiStreamUsesSuspectStreamAsLastResort(t *testing.T) {
	a := &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	a.setAlive(false)

	for i := 0; i < 5; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if a.sentCount() != 5 {
		t.Fatalf("a suspect stream must still carry traffic when it is the only one, got %d", a.sentCount())
	}
	if pkts, _ := ms.Buffered(); pkts != 0 {
		t.Fatalf("nothing should have been buffered, got %d", pkts)
	}
}

// When both are usable, the one that answers wins.
func TestMultiStreamPrefersAliveStream(t *testing.T) {
	suspect := &mockStream{}
	alive := &mockStream{}
	ms := NewMultiStreamTransport([]Transport{suspect, alive})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	suspect.setAlive(false)

	for i := 0; i < 10; i++ {
		if err := ms.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if suspect.sentCount() != 0 {
		t.Fatalf("a suspect stream must not be chosen while an alive one exists, got %d", suspect.sentCount())
	}
	if alive.sentCount() != 10 {
		t.Fatalf("the alive stream should have taken all 10, got %d", alive.sentCount())
	}
}

// ---- integrity ----

// The regression this guards: a corrupt frame used to be passed upward as if it
// were an IP packet, which is how garbage reached the gVisor stack.
func TestCompressedTransportDropsCorruptFrame(t *testing.T) {
	inner := &mockStream{}
	inner.connected.Store(true)
	ct := NewCompressedTransport(inner)

	got := make(chan []byte, 4)
	ct.Receive(func(b []byte) { got <- b })

	corrupt := append([]byte{CompressionMarker}, []byte("definitely not an lz4 frame")...)
	inner.inject(corrupt)

	select {
	case b := <-got:
		t.Fatalf("corrupt frame delivered upward as %d bytes: % x", len(b), b)
	case <-time.After(50 * time.Millisecond):
	}

	good := ipv4Packet([]byte("hello"))
	inner.inject(compress(good))
	select {
	case b := <-got:
		if !bytes.Equal(b, good) {
			t.Fatalf("valid frame decoded to % x", b)
		}
	case <-time.After(time.Second):
		t.Fatal("a valid frame was not delivered")
	}
}

// ipv4Packet builds a minimal but structurally valid IPv4 packet. The tunnel
// only ever carries IPv4, and the compressor now rejects anything else, so test
// fixtures have to look like real tunnel traffic.
func ipv4Packet(payload []byte) []byte {
	pkt := make([]byte, 20+len(payload))
	pkt[0] = 0x45 // version 4, IHL 5
	pkt[2], pkt[3] = byte(len(pkt)>>8), byte(len(pkt))
	pkt[9] = 6 // TCP
	copy(pkt[20:], payload)
	return pkt
}

func TestLooksLikeFrame(t *testing.T) {
	ipPkt := ipv4Packet([]byte("payload"))

	if !LooksLikeFrame(literalFrame(ipPkt)) {
		t.Fatal("a literal IPv4 frame must be recognised")
	}
	if !LooksLikeFrame(compress(bytes.Repeat([]byte("x"), 400))) {
		t.Fatal("an lz4 frame must be recognised")
	}
	if LooksLikeFrame(nil) {
		t.Fatal("an empty buffer is not a frame")
	}
	if LooksLikeFrame([]byte{0x00, 0x74, 0x65, 0x78, 0x74}) {
		t.Fatal("a literal frame whose payload is not IPv4 must be rejected")
	}
	if LooksLikeFrame([]byte("---OFXPING---1")) {
		t.Fatal("probe text must not look like a frame")
	}
	if LooksLikeFrame([]byte{CompressionMarker, 0x01, 0x02, 0x03, 0x04}) {
		t.Fatal("a bad lz4 magic must be rejected")
	}
}

// Noise from a shared document can decode into a well-formed lz4 frame that
// contains something other than an IP packet. It must not reach gVisor, and it
// must not count as evidence that the stream is alive.
func TestCompressedTransportDropsNonIPv4Frame(t *testing.T) {
	inner := &mockStream{}
	inner.connected.Store(true)
	ct := NewCompressedTransport(inner)

	got := make(chan []byte, 4)
	ct.Receive(func(b []byte) { got <- b })

	notAPacket := bytes.Repeat([]byte("this is not an ip packet "), 20) // > MinCompressSize
	frame := compress(notAPacket)

	if frame[0] != CompressionMarker {
		t.Fatalf("fixture should have taken the lz4 branch, got prefix %#x", frame[0])
	}
	if !LooksLikeFrame(frame) {
		t.Fatal("the fixture must pass the structural check, or the test proves nothing")
	}

	inner.inject(frame)

	select {
	case b := <-got:
		t.Fatalf("non-IPv4 payload delivered upward as %d bytes", len(b))
	case <-time.After(50 * time.Millisecond):
	}

	cc, ok := ct.(*CompressedTransport)
	if !ok {
		t.Fatal("expected the concrete compressed transport")
	}
	if cc.BadFrames() != 1 {
		t.Fatalf("expected 1 bad frame counted, got %d", cc.BadFrames())
	}
}

// The subtle piece: the flow key must survive the compression and encryption
// wrappers, otherwise MultiStream never sees it and affinity silently does
// nothing.
func TestFlowKeySurvivesCompressedAndEncryptedWrappers(t *testing.T) {
	inner := &mockStream{}
	inner.connected.Store(true)

	enc, err := NewEncryptedTransport(inner, "0123456789abcdef0123", "ctx", false)
	if err != nil {
		t.Fatalf("NewEncryptedTransport: %v", err)
	}
	ct := NewCompressedTransport(enc)

	key := testFlowKey()
	payload := []byte("a payload comfortably longer than the compression threshold, " +
		"so that lz4 actually runs and the frame is not stored literally")

	fs, ok := ct.(FlowAwareSender)
	if !ok {
		t.Fatal("CompressedTransport must be flow-aware, otherwise the tunnel cannot pin flows")
	}
	if err := fs.SendFlow(key, payload); err != nil {
		t.Fatalf("SendFlow: %v", err)
	}

	recs := inner.records()
	if len(recs) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(recs))
	}
	if recs[0].key != key {
		t.Fatalf("flow key lost in the wrapper chain: got %+v want %+v", recs[0].key, key)
	}
	if bytes.Equal(recs[0].data, payload) {
		t.Fatal("payload reached the inner transport unprocessed")
	}
	if recs[0].data[0] != encryptedMagic[0] {
		t.Fatalf("expected an encrypted frame, got first byte %#x", recs[0].data[0])
	}
}

// New flows must be spread across documents rather than piling onto whichever
// one the hash happened to favour. A browser opens several connections per
// host, and those should use the whole fan-out.
func TestMultiStreamFlowAffinityBalancesStreams(t *testing.T) {
	a, b, c := &mockStream{}, &mockStream{}, &mockStream{}
	ms := NewMultiStreamTransport([]Transport{a, b, c})
	if err := ms.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ms.Stop()

	const flows = 6
	for i := 0; i < flows; i++ {
		key := FlowKey{
			Src:     [4]byte{10, 0, 0, 2},
			Dst:     [4]byte{203, 0, 113, byte(i + 1)},
			SrcPort: uint16(40000 + i),
			DstPort: 443,
		}
		if err := ms.SendFlow(key, []byte{byte(i)}); err != nil {
			t.Fatalf("SendFlow %d: %v", i, err)
		}
	}

	// Six distinct flows over three streams should come out two each.
	for i, s := range []*mockStream{a, b, c} {
		if got := s.sentCount(); got != 2 {
			t.Fatalf("stream %d carried %d of 6 flows, want 2 (a=%d b=%d c=%d)",
				i, got, a.sentCount(), b.sentCount(), c.sentCount())
		}
	}
}

// Exercise the pooled lz4 path repeatedly. Reusing a pooled writer or reader
// incorrectly corrupts frames, and a single-shot round trip would not catch it.
func TestCompressedTransportRoundTripRepeated(t *testing.T) {
	inner := &mockStream{}
	inner.connected.Store(true)
	ct := NewCompressedTransport(inner)

	got := make(chan []byte, 8)
	ct.Receive(func(b []byte) { got <- b })

	compressible := ipv4Packet(bytes.Repeat([]byte("abcdefgh"), 400))

	// Deterministic pseudo-random bytes inside a valid IPv4 packet: lz4 cannot
	// shrink them, so this exercises the literal-frame fallback.
	rnd := make([]byte, 4096)
	x := uint32(12345)
	for i := range rnd {
		x = x*1664525 + 1013904223
		rnd[i] = byte(x >> 24)
	}
	incompressible := ipv4Packet(rnd)

	for round := 0; round < 20; round++ {
		for _, payload := range [][]byte{compressible, incompressible} {
			inner.mu.Lock()
			inner.sent = nil
			inner.mu.Unlock()

			if err := ct.Send(payload); err != nil {
				t.Fatalf("round %d: Send: %v", round, err)
			}
			recs := inner.records()
			if len(recs) != 1 {
				t.Fatalf("round %d: expected 1 frame, got %d", round, len(recs))
			}

			// The compressible payload must actually take the lz4 branch; if
			// pooling broke the writer we would silently fall back to literal.
			if bytes.Equal(payload, compressible) && recs[0].data[0] != CompressionMarker {
				t.Fatalf("round %d: compressible payload did not take the lz4 path (prefix %#x)",
					round, recs[0].data[0])
			}

			inner.inject(recs[0].data)

			select {
			case back := <-got:
				if !bytes.Equal(back, payload) {
					t.Fatalf("round %d: payload corrupted, %d bytes in, %d out",
						round, len(payload), len(back))
				}
			case <-time.After(time.Second):
				t.Fatalf("round %d: nothing came back", round)
			}
		}
	}
}
