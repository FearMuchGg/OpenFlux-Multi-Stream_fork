package tunnel

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"

	"universal-bypass-tool/network"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

// ExitMode выбирает, как выходная нода общается с интернетом.
type ExitMode int

const (
	ExitModeProxy ExitMode = iota // gVisor TCP-терминация + net.Dial (по умолчанию, работает везде)
	ExitModeRaw                   // raw sockets + SNAT (только Linux, требует root)
)

func (m ExitMode) String() string {
	if m == ExitModeRaw {
		return "raw"
	}
	return "proxy"
}

// ParseExitMode разбирает строку из флага --mode.
func ParseExitMode(s string) (ExitMode, error) {
	switch s {
	case "", "proxy":
		return ExitModeProxy, nil
	case "raw":
		return ExitModeRaw, nil
	default:
		return ExitModeProxy, fmt.Errorf("unknown mode %q (want proxy|raw)", s)
	}
}

type TCPTunnel struct {
	gvisorStack *stack.Stack
	tunnelEP    *TunnelLinkEndpoint
	transport   transport.Transport
	isExitNode  bool
	exitMode    ExitMode
	rawEP       *RawSocketEndpoint
	startTime   time.Time
	packetCount atomic.Uint64
}

// TCP buffer size range for gvisor stacks.
var (
	TCPBufMin     = 4 * 1024 * 1024
	TCPBufDefault = 16 * 1024 * 1024
	TCPBufMax     = 64 * 1024 * 1024
)

// TCPMaxRetries is how many failed retransmission probes gVisor makes before it
// aborts a connection. The gVisor default is 15, which — with exponential RTO
// backoff — tears down every tunnelled TCP connection after roughly five to ten
// minutes of total outage.
//
// The whole point of multi-stream is to survive documents dropping, so the
// budget is raised: a long outage should slow the tunnel down, not kill every
// connection through it. Only connections inside the tunnel are affected; the
// exit node's real outbound connections use net.Dial, not this stack.
var TCPMaxRetries uint64 = 64

// StatsInterval is how often [STATS] is printed. Set from --stats-interval.
var StatsInterval = 10 * time.Second

// SetTCPBuffers applies the configured TCP send/receive buffer ranges to s.
func SetTCPBuffers(s *stack.Stack) {
	rcv := tcpip.TCPReceiveBufferSizeRangeOption{Min: TCPBufMin, Default: TCPBufDefault, Max: TCPBufMax}
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &rcv); err != nil {
		utils.Debugf("[TUNNEL] set recv buffer: %v", err)
	}
	snd := tcpip.TCPSendBufferSizeRangeOption{Min: TCPBufMin, Default: TCPBufDefault, Max: TCPBufMax}
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &snd); err != nil {
		utils.Debugf("[TUNNEL] set send buffer: %v", err)
	}
}

// SetTCPTimeouts applies the retransmission budget to s.
func SetTCPTimeouts(s *stack.Stack) {
	opt := tcpip.TCPMaxRetriesOption(TCPMaxRetries)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt); err != nil {
		utils.Debugf("[TUNNEL] set TCP max retries: %v", err)
	}
}

// sendPacket hands one IP packet to the transport chain.
//
// When the chain is flow-aware the packet's inner TCP 4-tuple is parsed out
// first, so MultiStream can keep the whole connection on a single document.
// Without this the flow identity would be lost above the compression and
// encryption layers, which is where MultiStream sits.
//
// Anything that cannot be attributed to one flow — ICMP, an IP fragment, a
// truncated frame — falls through to plain Send, which uses the rotating scan.
func sendPacket(trans transport.Transport, data []byte) {
	if fs, flowAware := trans.(transport.FlowAwareSender); flowAware {
		if key, parsed := network.ParseFlowKey(data); parsed {
			if err := fs.SendFlow(key, data); err != nil {
				utils.Debugf("[TUNNEL] trans.SendFlow error: %v", err)
			}
			return
		}
	}
	if err := trans.Send(data); err != nil {
		utils.Debugf("[TUNNEL] trans.Send error: %v", err)
	}
}

func NewTCPTunnel(trans transport.Transport, isExitNode bool) *TCPTunnel {
	return NewTCPTunnelMode(trans, isExitNode, ExitModeProxy)
}

func NewTCPTunnelMode(trans transport.Transport, isExitNode bool, mode ExitMode) *TCPTunnel {
	t := &TCPTunnel{
		transport:  trans,
		isExitNode: isExitNode,
		exitMode:   mode,
		startTime:  time.Now(),
	}

	utils.Debugf("[TUNNEL] Net stack init...")
	t.gvisorStack = stack.New(stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		// CUBIC rather than gVisor's default of Reno. The tunnel's inner TCP
		// runs over a high-RTT, lossy relay, which is exactly the regime Reno
		// handles worst: it halves its window on every loss and recovers
		// slowly. CUBIC ramps back much more aggressively.
		//
		// Congestion control is a property of this stack and is applied
		// per-endpoint, so this is a local choice — the peer needs no matching
		// build and the wire format is unchanged.
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocolCUBIC},
	})

	SetTCPBuffers(t.gvisorStack)
	SetTCPTimeouts(t.gvisorStack)

	tunnelEP := NewTunnelLinkEndpoint()
	tunnelEP.onOutgoingPacket = func(data []byte) {
		t.packetCount.Add(1)
		sendPacket(trans, data)
	}
	t.tunnelEP = tunnelEP

	tunnelNIC := tcpip.NICID(1)
	if err := t.gvisorStack.CreateNIC(tunnelNIC, tunnelEP); err != nil {
		utils.Debugf("[TUNNEL] CreateNIC tunnel error: %v", err)
	}

	if isExitNode {
		if mode == ExitModeRaw {
			t.setupExitNodeRaw(tunnelNIC)
		} else {
			t.setupExitNodeProxy(tunnelNIC)
		}
	} else {
		t.setupClient(tunnelNIC)
	}

	trans.Receive(func(data []byte) {
		tunnelEP.InjectInbound(data)
	})

	if StatsInterval > 0 {
		utils.SafeGo("tunnel.printStats", t.printStats)
	}
	return t
}

// ---- exit node: proxy ----

func (t *TCPTunnel) setupExitNodeProxy(tunnelNIC tcpip.NICID) {
	utils.Debugf("[TUNNEL] EXIT NODE - proxy mode (no raw sockets)")

	t.gvisorStack.SetPromiscuousMode(tunnelNIC, true)
	t.gvisorStack.SetSpoofing(tunnelNIC, true)
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         tunnelNIC,
	})

	fwd := tcp.NewForwarder(t.gvisorStack, 0, 8192, t.handleExitTCP)
	t.gvisorStack.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
}

func (t *TCPTunnel) handleExitTCP(r *tcp.ForwarderRequest) {
	id := r.ID()
	dest := fmt.Sprintf("%s:%d", id.LocalAddress.String(), id.LocalPort)

	var wq waiter.Queue
	ep, tErr := r.CreateEndpoint(&wq)
	if tErr != nil {
		utils.Debugf("[EXIT] CreateEndpoint %s: %v", dest, tErr)
		r.Complete(true)
		return
	}
	r.Complete(false)
	local := gonet.NewTCPConn(&wq, ep)

	utils.SafeGo("exit.flow", func() {
		remote, err := net.DialTimeout("tcp", dest, 10*time.Second)
		if err != nil {
			utils.Debugf("[EXIT] dial %s failed: %v", dest, err)
			local.Close()
			return
		}
		if tc, ok := remote.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			_ = tc.SetReadBuffer(16 * 1024 * 1024)
			_ = tc.SetWriteBuffer(16 * 1024 * 1024)
		}
		utils.Debugf("[EXIT] %s connected", dest)

		go func() {
			buf := make([]byte, 256*1024)
			io.CopyBuffer(remote, local, buf)
			remote.Close()
			local.Close()
		}()
		buf := make([]byte, 256*1024)
		io.CopyBuffer(local, remote, buf)
		local.Close()
		remote.Close()
	})
}

// ---- exit node: raw (Linux only) ----

func (t *TCPTunnel) setupExitNodeRaw(tunnelNIC tcpip.NICID) {
	localIP := getLocalIP()
	utils.Debugf("[TUNNEL] EXIT NODE - raw mode, local IP: %s", localIP)

	rawEP, err := NewRawSocketEndpoint(tcpip.NICID(2))
	if err != nil {
		utils.Debugf("[TUNNEL] raw socket error (mode raw requires root on Linux): %v", err)
		utils.Debugf("[TUNNEL] falling back to proxy mode")
		t.exitMode = ExitModeProxy
		t.setupExitNodeProxy(tunnelNIC)
		return
	}

	t.rawEP = rawEP
	rawEP.SetTransportSender(func(data []byte) {
		t.packetCount.Add(1)
		sendPacket(t.transport, data)
	})

	internetNIC := tcpip.NICID(2)
	if err := t.gvisorStack.CreateNIC(internetNIC, rawEP); err != nil {
		utils.Debugf("[TUNNEL] CreateNIC internet error: %v", err)
		return
	}

	var ipBytes [4]byte
	fmt.Sscanf(localIP, "%d.%d.%d.%d", &ipBytes[0], &ipBytes[1], &ipBytes[2], &ipBytes[3])
	internetAddr := tcpip.AddrFrom4(ipBytes)
	t.gvisorStack.AddProtocolAddress(internetNIC, tcpip.ProtocolAddress{
		Protocol: ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   internetAddr,
			PrefixLen: 24,
		},
	}, stack.AddressProperties{})

	t.gvisorStack.SetForwardingDefaultAndAllNICs(ipv4.ProtocolNumber, true)
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         internetNIC,
	})

	tunnelSubnet := tcpip.AddressWithPrefix{
		Address:   tcpip.AddrFrom4([4]byte{10, 10, 10, 0}),
		PrefixLen: 24,
	}.Subnet()
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: tunnelSubnet,
		NIC:         tunnelNIC,
	})
}

// ---- client ----

func (t *TCPTunnel) setupClient(tunnelNIC tcpip.NICID) {
	clientAddr := tcpip.AddrFrom4([4]byte{10, 10, 10, 2})
	t.gvisorStack.AddProtocolAddress(tunnelNIC, tcpip.ProtocolAddress{
		Protocol: ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   clientAddr,
			PrefixLen: 24,
		},
	}, stack.AddressProperties{})

	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         tunnelNIC,
	})
}

func (t *TCPTunnel) DialTCP(address string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}

	ip := tcpAddr.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("IPv6 not supported")
	}
	utils.Debugf("[TUNNEL] DialTCP %s -> %s:%d", address, ip.String(), tcpAddr.Port)

	nic := tcpip.NICID(1)
	if t.isExitNode && t.exitMode == ExitModeRaw {
		nic = tcpip.NICID(2)
	}

	conn, err := gonet.DialTCP(t.gvisorStack, tcpip.FullAddress{
		NIC:  nic,
		Addr: tcpip.AddrFrom4([4]byte{ip[0], ip[1], ip[2], ip[3]}),
		Port: uint16(tcpAddr.Port),
	}, ipv4.ProtocolNumber)

	return conn, err
}

func (t *TCPTunnel) ListenTCP(port uint16) (net.Listener, error) {
	return gonet.ListenTCP(t.gvisorStack, tcpip.FullAddress{
		NIC:  1,
		Port: port,
	}, ipv4.ProtocolNumber)
}

func (t *TCPTunnel) printStats() {
	ticker := time.NewTicker(StatsInterval)
	defer ticker.Stop()

	// Counters are monotonic, but a stream that reconnects can restart its own
	// totals; clamp so a decrease never shows up as a huge bogus rate.
	var prevTx, prevRx, prevOut, prevRetrans uint64
	prevAt := time.Now()

	for range ticker.C {
		now := time.Now()
		elapsed := now.Sub(prevAt).Seconds()
		if elapsed <= 0 {
			continue
		}
		prevAt = now

		gs := t.gvisorStack.Stats()
		ts := t.transport.Stats()

		tx, rx := ts.BytesSent, ts.BytesReceived
		out := t.packetCount.Load()
		retrans := gs.TCP.Retransmits.Value()

		dTx := rate(tx, prevTx)
		dRx := rate(rx, prevRx)
		dOut := rate(out, prevOut)
		dRetrans := rate(retrans, prevRetrans)

		prevTx, prevRx, prevOut, prevRetrans = tx, rx, out, retrans

		// Frames that reached the compressor but were not IPv4 packets. A
		// steady climb here means something other than the tunnel is writing
		// into the document.
		var badFrames uint64
		if ct, ok := t.transport.(*transport.CompressedTransport); ok {
			badFrames = ct.BadFrames()
		}

		// Printed unconditionally: without --debug there was previously no way
		// at all to see whether the tunnel was moving data.
		log.Printf("[STATS] up=%v mode=%s est=%d out=%d/s tx=%.1fKB/s rx=%.1fKB/s retrans=+%d/%d badframe=%d",
			time.Since(t.startTime).Round(time.Second),
			t.exitMode.String(),
			gs.TCP.CurrentEstablished.Value(),
			uint64(float64(dOut)/elapsed),
			float64(dTx)/elapsed/1024,
			float64(dRx)/elapsed/1024,
			dRetrans,
			retrans,
			badFrames,
		)
	}
}

// rate returns cur-prev, or 0 when the counter went backwards.
func rate(cur, prev uint64) uint64 {
	if cur < prev {
		return 0
	}
	return cur - prev
}

// ---- local IP helpers (only needed for raw mode) ----

// localIPOverride, when set, is the address the exit node uses as its egress
// IP (both for source rewriting and the return-packet filter).
var localIPOverride string

// SetLocalIP overrides the auto-detected egress IP for the exit node.
func SetLocalIP(ip string) { localIPOverride = ip }

func getLocalIP() string {
	if localIPOverride != "" {
		return localIPOverride
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "192.168.1.100"
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
