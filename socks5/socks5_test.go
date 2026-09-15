package socks5

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type fakeDialer struct {
	mu   sync.Mutex
	addr string
	err  error
}

func (d *fakeDialer) DialTCP(address string) (net.Conn, error) {
	d.mu.Lock()
	d.addr = address
	d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	// The far end of a pipe, drained so the tunnel's copy loops do not block.
	server, client := net.Pipe()
	go func() {
		io.Copy(io.Discard, server)
		server.Close()
	}()
	return client, nil
}

func (d *fakeDialer) dialed() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.addr
}

// send writes without blocking the test.
//
// net.Pipe has no buffer, so a request the proxy deliberately does not finish
// reading — an unsupported command, where only the 4-byte header is consumed —
// would block the write forever. On a real socket those leftover bytes simply
// sit in the kernel receive buffer.
func send(c net.Conn, data []byte) {
	go func() { c.Write(data) }()
}

// startHandshake runs the greeting exchange and returns the client end, ready
// for the request to be sent.
func startHandshake(t *testing.T, d Dialer) net.Conn {
	t.Helper()

	server, client := net.Pipe()
	srv := NewSOCKS5Server(":0", d)
	go srv.handleConnection(server)

	send(client, []byte{0x05, 0x01, 0x00})
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(client, greeting); err != nil {
		t.Fatalf("read greeting reply: %v", err)
	}
	if greeting[0] != 0x05 || greeting[1] != 0x00 {
		t.Fatalf("unexpected greeting reply: % x", greeting)
	}
	return client
}

// readReply reads the 10-byte SOCKS5 reply, failing if the proxy closed the
// connection instead of answering.
func readReply(t *testing.T, c net.Conn) []byte {
	t.Helper()

	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(c, reply); err != nil {
		t.Fatalf("no SOCKS5 reply (proxy closed without answering?): %v", err)
	}
	return reply
}

func TestSOCKS5IPv4Connect(t *testing.T) {
	d := &fakeDialer{}
	client := startHandshake(t, d)
	defer client.Close()

	send(client, []byte{0x05, 0x01, 0x00, 0x01, 93, 184, 216, 34, 0x01, 0xBB})

	reply := readReply(t, client)
	if reply[1] != socksStatusSucceeded {
		t.Fatalf("status %#x, want success", reply[1])
	}
	if got := d.dialed(); got != "93.184.216.34:443" {
		t.Fatalf("dialed %q, want 93.184.216.34:443", got)
	}
}

func TestSOCKS5DomainConnect(t *testing.T) {
	d := &fakeDialer{}
	client := startHandshake(t, d)
	defer client.Close()

	host := "example.com"
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, host...)
	req = append(req, 0x01, 0xBB)
	send(client, req)

	reply := readReply(t, client)
	if reply[1] != socksStatusSucceeded {
		t.Fatalf("status %#x, want success", reply[1])
	}
	if got := d.dialed(); got != "example.com:443" {
		t.Fatalf("dialed %q, want example.com:443", got)
	}
}

// The regression this guards: a single Read is not guaranteed to return the
// whole request. A 10-byte IPv4 CONNECT split across two segments used to make
// the handler give up and close without any reply, which the client reports as
// "proxy closed connection" with no hint of the cause.
func TestSOCKS5HandlesSplitRequest(t *testing.T) {
	d := &fakeDialer{}
	client := startHandshake(t, d)
	defer client.Close()

	send(client, []byte{0x05, 0x01, 0x00, 0x01, 93, 184})
	send(client, []byte{216, 34, 0x01, 0xBB})

	reply := readReply(t, client)
	if reply[1] != socksStatusSucceeded {
		t.Fatalf("status %#x, want success", reply[1])
	}
	if got := d.dialed(); got != "93.184.216.34:443" {
		t.Fatalf("dialed %q", got)
	}
}

// Browsers that resolve names locally (Chrome and Edge) can send an IPv6
// address for any dual-stack host. The tunnel cannot serve it, but it must say
// so: told the address type is unsupported, a client falls back to IPv4, while
// a silent close just looks like a broken proxy.
func TestSOCKS5RejectsIPv6WithAReply(t *testing.T) {
	d := &fakeDialer{}
	client := startHandshake(t, d)
	defer client.Close()

	req := append([]byte{0x05, 0x01, 0x00, 0x04}, make([]byte, 16)...)
	req = append(req, 0x01, 0xBB)
	send(client, req)

	reply := readReply(t, client)
	if reply[1] != socksStatusAddrTypeNotSupported {
		t.Fatalf("status %#x, want 0x08 address type not supported", reply[1])
	}
	if got := d.dialed(); got != "" {
		t.Fatalf("must not dial an IPv6 target, dialed %q", got)
	}
}

func TestSOCKS5RejectsUnsupportedCommand(t *testing.T) {
	d := &fakeDialer{}
	client := startHandshake(t, d)
	defer client.Close()

	// BIND is 0x02; only CONNECT is implemented.
	send(client, []byte{0x05, 0x02, 0x00, 0x01, 1, 2, 3, 4, 0, 80})

	reply := readReply(t, client)
	if reply[1] != socksStatusCommandNotSupported {
		t.Fatalf("status %#x, want 0x07 command not supported", reply[1])
	}
	if got := d.dialed(); got != "" {
		t.Fatalf("must not dial for an unsupported command, dialed %q", got)
	}
}

func TestSOCKS5RepliesWhenDialFails(t *testing.T) {
	d := &fakeDialer{err: errors.New("no route")}
	client := startHandshake(t, d)
	defer client.Close()

	send(client, []byte{0x05, 0x01, 0x00, 0x01, 1, 2, 3, 4, 0, 80})

	reply := readReply(t, client)
	if reply[1] != socksStatusHostUnreachable {
		t.Fatalf("status %#x, want 0x04 host unreachable", reply[1])
	}
}
