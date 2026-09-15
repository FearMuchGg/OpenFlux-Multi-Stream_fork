package transport

import (
	"sync"
	"sync/atomic"
	"time"
)

type TransportConfig struct {
	MaxReconnectAttempts int
	ReconnectDelay       time.Duration
	ReconnectMultiplier  float64
	MaxQueueSize         int
	KeepAliveInterval    time.Duration

	// LivenessProbe enables the end-to-end ping/pong health check that decides
	// whether a stream is really usable (see LivenessTracker).
	//
	// Both peers must agree: a peer built without probe support never answers,
	// so it would look permanently dead. The zero value is therefore "off",
	// and only DefaultConfig turns it on.
	LivenessProbe bool

	// MultiStreamBufferBytes / MultiStreamMaxPackets cap the multi-stream retry
	// buffer. Zero means "use the MultiStreamTransport default". Lower them on
	// memory-tight targets such as the iOS Network Extension, which runs under
	// a hard memory limit.
	MultiStreamBufferBytes int
	MultiStreamMaxPackets  int
}

type Transport interface {
	Start() error
	Stop() error
	Send(data []byte) error
	Receive(callback func([]byte))
	IsConnected() bool
	Stats() TransportStats
}

// FrameCounter is an optional interface for transports that can report the
// health of their per-frame write path. The status line uses it when a stream
// implements it and simply omits these numbers otherwise.
//
// It exists because a transport's queue-full drops and write failures were
// previously invisible unless --debug was on, which made throughput problems
// impossible to diagnose from the outside.
type FrameCounter interface {
	// FrameStats returns frames written, socket write errors, sends that gave
	// up waiting for queue space, and received payloads that were not
	// recognisable as tunnel frames.
	FrameStats() (frames, writeErrors, sendTimeouts, badFrames uint64)
}

// HealthReporter is an optional interface for transports that can report the
// state of their liveness probe. MultiStreamTransport uses it to PREFER a
// stream that answers, never to discard one that does not.
//
// The two answers mean different things and must not be conflated:
//
//   - Healthy: the peer answered our probe. A stream that fails this may still
//     be carrying traffic perfectly well — the probe itself can fail for
//     reasons unrelated to the tunnel.
//   - Alive: the stream is carrying traffic at all, by probe or by validated
//     data frames.
//
// A transport that does not implement this is treated as alive.
type HealthReporter interface {
	Healthy() bool
	Alive() bool
	// ProbeStats returns probes sent, misses, pongs and late pongs.
	ProbeStats() (sent, misses, pongs, latePongs uint64)
	// RxAge is how long ago the last validated frame arrived.
	RxAge() time.Duration
}

type TransportStats struct {
	BytesSent     uint64
	BytesReceived uint64
	PacketsSent   uint64
	PacketsRecv   uint64
	Reconnects    uint64
	Connected     bool
	Uptime        time.Duration
}

func DefaultConfig() TransportConfig {
	return TransportConfig{
		MaxReconnectAttempts: 999999,
		ReconnectDelay:       0,
		ReconnectMultiplier:  1.1,
		// 4096 packets of headroom, ~6 MiB at a full 1500-byte MTU, so a brief
		// stall in the transport's writer does not have to drop anything. The
		// bounded wait in Send is what actually throttles the sender; this just
		// absorbs bursts.
		MaxQueueSize:      4096,
		KeepAliveInterval: 10 * time.Second,
		LivenessProbe:     true,
	}
}

type BaseTransport struct {
	config    TransportConfig
	running   atomic.Int32
	connected atomic.Int32
	stats     TransportStats
	startTime time.Time

	receiveCallback func([]byte)
	Mu              sync.RWMutex

	reconnectAttempts atomic.Int32
}

func NewBaseTransport(config TransportConfig) *BaseTransport {
	return &BaseTransport{
		config:    config,
		startTime: time.Now(),
	}
}

func (b *BaseTransport) Start() error {
	b.running.Store(1)
	b.startTime = time.Now()
	return nil
}

func (b *BaseTransport) Stop() error {
	b.running.Store(0)
	b.connected.Store(0)
	return nil
}

func (b *BaseTransport) IsRunning() bool {
	return b.running.Load() == 1
}

func (b *BaseTransport) IsConnected() bool {
	return b.connected.Load() == 1
}

func (b *BaseTransport) SetConnected(connected bool) {
	if connected {
		b.connected.Store(1)
	} else {
		b.connected.Store(0)
	}
}

func (b *BaseTransport) Receive(callback func([]byte)) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.receiveCallback = callback
}

func (b *BaseTransport) CallReceive(data []byte) {
	b.Mu.RLock()
	cb := b.receiveCallback
	b.Mu.RUnlock()
	if cb != nil {
		cb(data)
	}
}

func (b *BaseTransport) GetSession(accessor func(interface{})) {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	// This is a helper for subclasses
}

func (b *BaseTransport) Stats() TransportStats {
	return TransportStats{
		BytesSent:     atomic.LoadUint64(&b.stats.BytesSent),
		BytesReceived: atomic.LoadUint64(&b.stats.BytesReceived),
		PacketsSent:   atomic.LoadUint64(&b.stats.PacketsSent),
		PacketsRecv:   atomic.LoadUint64(&b.stats.PacketsRecv),
		Reconnects:    uint64(b.reconnectAttempts.Load()),
		Connected:     b.IsConnected(),
		Uptime:        time.Since(b.startTime),
	}
}

func (b *BaseTransport) RecordSend(bytes int) {
	atomic.AddUint64(&b.stats.BytesSent, uint64(bytes))
	atomic.AddUint64(&b.stats.PacketsSent, 1)
}

func (b *BaseTransport) RecordReceive(bytes int) {
	atomic.AddUint64(&b.stats.BytesReceived, uint64(bytes))
	atomic.AddUint64(&b.stats.PacketsRecv, 1)
}

func (b *BaseTransport) RecordReconnect() {
	b.reconnectAttempts.Add(1)
}

func (b *BaseTransport) GetConfig() TransportConfig {
	return b.config
}
