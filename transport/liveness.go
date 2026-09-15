package transport

import (
	"sync/atomic"
	"time"
)

// DefaultProbeInterval is how often a stream sends a liveness probe.
const DefaultProbeInterval = 3 * time.Second

// LivenessTracker turns an explicit ping/pong exchange into a health signal for
// one transport stream.
//
// It deliberately reports TWO independent things, and callers must not confuse
// them:
//
//   - Healthy() — the peer answered our probe. Diagnostic, and a hint that the
//     stream's outbound path works.
//   - Alive() — the stream is carrying traffic at all: either the probe was
//     answered, or a frame validated as real tunnel traffic arrived recently.
//
// The distinction exists because a probe can fail for reasons that have nothing
// to do with the tunnel — a probe the relay never delivers, a reply that
// arrives after the timeout, a read loop busy injecting a burst. Treating that
// as "the stream is dead" once took a working tunnel down permanently: nothing
// restores a stream once it is marked disconnected, so every connection through
// it stalled while the socket stayed open. A stream that is demonstrably
// delivering data must therefore never be discarded on probe evidence alone.
//
// Why inbound data is not enough on its own: a stream can be receiving while
// its outbound path is broken. That is what Healthy() is for — it lets the
// router prefer a stream that answers, without hard-excluding one that only
// receives.
//
// Probes continue while a stream looks unhealthy, so recovery needs no
// scheduling: a late pong, or the next validated frame, flips Alive() back on
// the spot.
type LivenessTracker struct {
	interval   time.Duration
	timeout    time.Duration
	dataWindow time.Duration
	maxMisses  int32
	disabled   bool

	probeSeq  atomic.Uint64
	pending   atomic.Uint64 // nonce of the probe awaiting a pong; 0 = none
	pendingAt atomic.Int64  // unix nanos when that probe was sent
	misses    atomic.Int32
	healthy   atomic.Bool
	lastPong  atomic.Int64 // unix nanos
	lastRx    atomic.Int64 // unix nanos of the last VALIDATED frame
	lastRTT   atomic.Int64 // nanos, last observed probe round trip

	probesSent  atomic.Uint64
	missTotal   atomic.Uint64
	pongs       atomic.Uint64
	latePongs   atomic.Uint64
	validFrames atomic.Uint64
}

// NewLivenessTracker starts a tracker that probes every interval, waits up to
// two intervals for an answer, and marks the probe unhealthy after two
// consecutive unanswered ones.
func NewLivenessTracker(interval time.Duration) *LivenessTracker {
	if interval <= 0 {
		interval = DefaultProbeInterval
	}
	now := time.Now().UnixNano()
	l := &LivenessTracker{
		interval:   interval,
		timeout:    2 * interval,
		dataWindow: 3 * interval,
		maxMisses:  2,
	}
	l.healthy.Store(true)
	l.lastPong.Store(now)
	l.lastRx.Store(now)
	return l
}

// NewDisabledLivenessTracker returns a tracker that never probes and always
// reports healthy and alive. Used when --liveness-probe is off.
func NewDisabledLivenessTracker() *LivenessTracker {
	l := NewLivenessTracker(DefaultProbeInterval)
	l.disabled = true
	return l
}

// Interval is how often the caller should ask for a probe.
func (l *LivenessTracker) Interval() time.Duration { return l.interval }

// NextProbe returns a nonce to send as a ping, or 0 when there is nothing to
// send right now — a probe is still within its timeout window, or the tracker
// is disabled.
//
// A probe that has outlived its timeout is abandoned, counted as a miss, and
// replaced by a fresh one, so the caller simply calls this on every tick.
func (l *LivenessTracker) NextProbe() uint64 {
	if l.disabled {
		return 0
	}

	if l.pending.Load() != 0 {
		if time.Since(time.Unix(0, l.pendingAt.Load())) < l.timeout {
			return 0
		}
		l.pending.Store(0)
		l.missTotal.Add(1)
		if l.misses.Add(1) >= l.maxMisses {
			l.healthy.Store(false)
		}
	}

	nonce := l.probeSeq.Add(1)
	l.pending.Store(nonce)
	l.pendingAt.Store(time.Now().UnixNano())
	l.probesSent.Add(1)
	return nonce
}

// OnPong records an answer to our probe. Any pong proves the stream is alive,
// including one that arrives after its probe already timed out — that case is
// counted separately as a late pong, because it is the signature of a probe
// that is being delivered, just slowly.
func (l *LivenessTracker) OnPong(nonce uint64) {
	if l.disabled {
		return
	}
	now := time.Now().UnixNano()

	if nonce != 0 && l.pending.Load() == nonce {
		l.pending.Store(0)
		l.lastRTT.Store(now - l.pendingAt.Load())
	} else {
		l.latePongs.Add(1)
	}

	l.misses.Store(0)
	l.healthy.Store(true)
	l.lastPong.Store(now)
	l.pongs.Add(1)
}

// OnValidFrame records that a frame which passed validation as real tunnel
// traffic arrived.
//
// Call this ONLY after validating the frame. Counting unvalidated payloads
// would let unrelated noise in a shared document keep a dead stream looking
// alive forever.
func (l *LivenessTracker) OnValidFrame() {
	l.lastRx.Store(time.Now().UnixNano())
	l.validFrames.Add(1)
}

// Healthy reports whether the last probe was answered within its timeout.
// Always true when the tracker is disabled.
func (l *LivenessTracker) Healthy() bool { return l.healthy.Load() }

// Alive reports whether the stream is carrying traffic: the probe was answered,
// or a validated frame arrived within the data window.
//
// This — not Healthy() — is what should gate stream selection. See the type
// comment for why.
func (l *LivenessTracker) Alive() bool {
	if l.disabled {
		return true
	}
	if l.healthy.Load() {
		return true
	}
	return time.Since(time.Unix(0, l.lastRx.Load())) < l.dataWindow
}

// Suspect reports that the stream is neither answering probes nor delivering
// data. It is a hint to deprioritise the stream, not to discard it.
func (l *LivenessTracker) Suspect() bool { return !l.Alive() }

// Disabled reports whether probing is turned off.
func (l *LivenessTracker) Disabled() bool { return l.disabled }

// LastPong and LastRx expose the two clocks separately, so status output can
// distinguish "peer is not answering" from "peer is not sending data".
func (l *LivenessTracker) LastPong() time.Time { return time.Unix(0, l.lastPong.Load()) }
func (l *LivenessTracker) LastRx() time.Time   { return time.Unix(0, l.lastRx.Load()) }

// RxAge is how long ago the last validated frame arrived.
func (l *LivenessTracker) RxAge() time.Duration {
	return time.Since(time.Unix(0, l.lastRx.Load()))
}

// RTT is the round trip of the last answered probe, or 0 if none has been.
func (l *LivenessTracker) RTT() time.Duration { return time.Duration(l.lastRTT.Load()) }

// ProbeStats returns probes sent, probe misses, pongs received (including late
// ones), and late pongs.
//
// latePongs is the number that tells the two failure modes apart: non-zero
// means probes ARE being delivered and answered, just slower than the timeout;
// zero alongside a high miss count means the peer never answered at all.
func (l *LivenessTracker) ProbeStats() (sent, misses, pongs, latePongs uint64) {
	return l.probesSent.Load(), l.missTotal.Load(), l.pongs.Load(), l.latePongs.Load()
}

// ValidFrames is the number of validated tunnel frames seen on this stream.
func (l *LivenessTracker) ValidFrames() uint64 { return l.validFrames.Load() }
