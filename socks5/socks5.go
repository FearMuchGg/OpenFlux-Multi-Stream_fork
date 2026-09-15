package socks5

import (
	"fmt"
	"io"
	"net"
	"sync"

	"universal-bypass-tool/utils"
)

type Dialer interface {
	DialTCP(address string) (net.Conn, error)
}

// SOCKS5 address types (RFC 1928 §4).
const (
	socksAddrIPv4   = 0x01
	socksAddrDomain = 0x03
	socksAddrIPv6   = 0x04
)

// SOCKS5 reply status codes (RFC 1928 §6).
const (
	socksStatusSucceeded            = 0x00
	socksStatusGeneralFailure       = 0x01
	socksStatusHostUnreachable      = 0x04
	socksStatusCommandNotSupported  = 0x07
	socksStatusAddrTypeNotSupported = 0x08
)

// socksSuccessReply is a CONNECT reply with a zeroed bound address, which every
// client accepts.
var socksSuccessReply = []byte{0x05, socksStatusSucceeded, 0x00, socksAddrIPv4, 0, 0, 0, 0, 0, 0}

// socksReply sends a failure reply.
//
// Replying matters even when the connection is about to close. Clients turn a
// silent close into "proxy closed connection", which says nothing about the
// cause, whereas a status code is reported verbatim — and for an unsupported
// address type it lets the client retry over IPv4.
func socksReply(w io.Writer, status byte) {
	w.Write([]byte{0x05, status, 0x00, socksAddrIPv4, 0, 0, 0, 0, 0, 0})
}

type SOCKS5Server struct {
	listenAddr string
	dialer     Dialer

	mu       sync.Mutex
	listener net.Listener
	closed   bool
}

func NewSOCKS5Server(addr string, dialer Dialer) *SOCKS5Server {
	return &SOCKS5Server{listenAddr: addr, dialer: dialer}
}

// Bind reserves the listen address so callers can detect "address already in
// use" synchronously, before serving. Safe to call once; Start binds lazily if
// it wasn't called.
func (s *SOCKS5Server) Bind() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	if s.listener != nil {
		return nil
	}
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

func (s *SOCKS5Server) Start() error {
	if err := s.Bind(); err != nil {
		return err
	}

	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	defer listener.Close()

	utils.Debugf("[SOCKS5] Listening on %s", s.listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				utils.Debugf("[SOCKS5] Listener closed, stopping")
				return net.ErrClosed
			}
			utils.Debugf("[SOCKS5] Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// Close stops the server, unblocking Start's accept loop.
func (s *SOCKS5Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *SOCKS5Server) handleConnection(clientConn net.Conn) {
	// A malformed request must never crash the host process; contain any
	// panic to this connection.
	defer func() {
		if r := recover(); r != nil {
			utils.Debugf("[SOCKS5] Recovered from panic in handler: %v", r)
		}
	}()
	defer clientConn.Close()

	// This is a real kernel socket, and Nagle is on by default for TCP. The
	// proxy carries interactive traffic, where a delayed segment is pure added
	// latency. gVisor's own endpoints already default to Nagle off, so this
	// socket was the only one buffering writes.
	if tc, ok := clientConn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	buf := make([]byte, 256)

	// Greeting: VER, NMETHODS, METHODS...
	if _, err := io.ReadFull(clientConn, buf[:2]); err != nil {
		return
	}
	if buf[0] != 0x05 {
		return
	}
	if nmethods := int(buf[1]); nmethods > 0 {
		if _, err := io.ReadFull(clientConn, buf[:nmethods]); err != nil {
			return
		}
	}
	// No authentication, whatever the client offered.
	if _, err := clientConn.Write([]byte{0x05, 0x00}); err != nil {
		return
	}

	// Request: VER, CMD, RSV, ATYP, ADDR, PORT.
	//
	// Read with io.ReadFull, not a single Read. TCP is a stream: a 10-byte IPv4
	// CONNECT can legitimately arrive in two segments, and the old single Read
	// then saw n < 10 and returned without replying — which the client reports
	// as "proxy closed connection", with nothing to indicate the real cause.
	if _, err := io.ReadFull(clientConn, buf[:4]); err != nil {
		return
	}
	if buf[0] != 0x05 {
		return
	}
	if buf[1] != 0x01 {
		utils.Debugf("[SOCKS5] Unsupported command %#x", buf[1])
		socksReply(clientConn, socksStatusCommandNotSupported)
		return
	}

	var targetAddr string
	switch buf[3] {
	case socksAddrIPv4:
		if _, err := io.ReadFull(clientConn, buf[:6]); err != nil {
			return
		}
		targetAddr = fmt.Sprintf("%d.%d.%d.%d:%d",
			buf[0], buf[1], buf[2], buf[3],
			uint16(buf[4])<<8|uint16(buf[5]))

	case socksAddrDomain:
		if _, err := io.ReadFull(clientConn, buf[:1]); err != nil {
			return
		}
		domainLen := int(buf[0])
		if domainLen == 0 {
			socksReply(clientConn, socksStatusGeneralFailure)
			return
		}
		// Domain (domainLen bytes) followed by a 2-byte port.
		if _, err := io.ReadFull(clientConn, buf[:domainLen+2]); err != nil {
			return
		}
		targetAddr = fmt.Sprintf("%s:%d",
			string(buf[:domainLen]),
			uint16(buf[domainLen])<<8|uint16(buf[domainLen+1]))

	case socksAddrIPv6:
		// Drain the address first. Closing a socket that still has unread data
		// in its receive buffer sends an RST rather than a FIN, and that can
		// destroy the reply we are about to send — leaving the client with no
		// answer at all, which is the exact problem this branch exists to fix.
		if _, err := io.ReadFull(clientConn, buf[:18]); err != nil {
			return
		}
		utils.Debugf("[SOCKS5] IPv6 target requested, tunnel is IPv4-only")
		socksReply(clientConn, socksStatusAddrTypeNotSupported)
		return

	default:
		utils.Debugf("[SOCKS5] Unsupported address type %#x", buf[3])
		socksReply(clientConn, socksStatusAddrTypeNotSupported)
		return
	}

	utils.Debugf("[SOCKS5] CONNECT %s", targetAddr)

	targetConn, err := s.dialer.DialTCP(targetAddr)
	if err != nil {
		utils.Debugf("[SOCKS5] Dial failed: %v", err)
		socksReply(clientConn, socksStatusHostUnreachable)
		return
	}
	defer targetConn.Close()

	if _, err := clientConn.Write(socksSuccessReply); err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer targetConn.Close()
		io.Copy(targetConn, clientConn)
	}()

	go func() {
		defer wg.Done()
		defer clientConn.Close()
		io.Copy(clientConn, targetConn)
	}()

	wg.Wait()
}
