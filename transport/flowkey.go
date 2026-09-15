package transport

// FlowKey identifies one inner TCP connection carried by the tunnel: the
// 4-tuple of the IP packet that the tunnel's gVisor stack emitted. It is a
// comparable value, so it can be used directly as a map key.
//
// Direction matters: the client hashes (client -> destination) while the exit
// node hashes (destination -> client). The two peers deliberately do NOT agree
// on a mapping — each direction is independent, the framing is stateless per
// packet, and the inner TCP reassembles whatever arrives.
type FlowKey struct {
	Src     [4]byte
	Dst     [4]byte
	SrcPort uint16
	DstPort uint16
}

// FlowAwareSender is implemented by transports that can route a packet using
// the identity of the inner TCP flow it belongs to. MultiStreamTransport uses
// it for sticky per-flow affinity so that every packet of one connection stays
// on the same document, instead of being spread round-robin across all of them
// (which costs reordering, spurious fast retransmits and a collapsed
// congestion window).
//
// It is deliberately optional and additive: the wrappers that sit between the
// tunnel and MultiStreamTransport forward it down the chain, and a Transport
// that does not implement it keeps working through the plain Send path.
//
// The wrappers must forward it rather than let MultiStream parse the payload
// itself: MultiStream sits *below* compression and encryption, so by the time
// a packet reaches it the IP header is gone.
type FlowAwareSender interface {
	SendFlow(key FlowKey, data []byte) error
}

// Hash returns a stable FNV-1a hash of the flow.
//
// Stability matters whenever this is used to place a flow: a flow that hashed
// differently on a later packet would migrate between documents mid-connection.
//
// MultiStreamTransport itself no longer derives its choice from this — it
// rotates the starting point so parallel connections spread evenly, and keeps
// the result in a sticky pin. The hash is kept as a utility for custom
// flow-aware transports that want to shard flows themselves.
func (k FlowKey) Hash() uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, b := range k.Src {
		h ^= uint64(b)
		h *= prime64
	}
	for _, b := range k.Dst {
		h ^= uint64(b)
		h *= prime64
	}
	h ^= uint64(k.SrcPort)
	h *= prime64
	h ^= uint64(k.DstPort)
	h *= prime64
	return h
}
