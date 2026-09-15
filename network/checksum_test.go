package network

import (
	"testing"

	"universal-bypass-tool/transport"
)

// ipv4TCP builds a minimal IPv4 + TCP header pair. ihl is the IP header length
// in 32-bit words, so ihl=6 exercises the options path.
func ipv4TCP(srcPort, dstPort uint16, ihl int) []byte {
	pkt := make([]byte, ihl*4+20)
	pkt[0] = byte(0x40 | ihl)
	pkt[2], pkt[3] = byte(len(pkt)>>8), byte(len(pkt))
	pkt[8] = 64
	pkt[9] = 6
	copy(pkt[12:16], []byte{10, 10, 10, 2})
	copy(pkt[16:20], []byte{203, 0, 113, 7})
	off := ihl * 4
	pkt[off], pkt[off+1] = byte(srcPort>>8), byte(srcPort)
	pkt[off+2], pkt[off+3] = byte(dstPort>>8), byte(dstPort)
	return pkt
}

func TestParseFlowKeyValid(t *testing.T) {
	key, ok := ParseFlowKey(ipv4TCP(40001, 443, 5))
	if !ok {
		t.Fatal("a plain IPv4/TCP packet must yield a flow key")
	}
	want := transport.FlowKey{
		Src:     [4]byte{10, 10, 10, 2},
		Dst:     [4]byte{203, 0, 113, 7},
		SrcPort: 40001,
		DstPort: 443,
	}
	if key != want {
		t.Fatalf("got %+v, want %+v", key, want)
	}
}

// IPv4 options shift the TCP header; reading ports at a fixed offset 20 would
// return garbage and split one connection across streams.
func TestParseFlowKeyWithIPOptions(t *testing.T) {
	key, ok := ParseFlowKey(ipv4TCP(1234, 80, 6))
	if !ok {
		t.Fatal("a packet with IP options must still yield a flow key")
	}
	if key.SrcPort != 1234 || key.DstPort != 80 {
		t.Fatalf("ports read from the wrong offset: %+v", key)
	}
}

func TestParseFlowKeyRejects(t *testing.T) {
	cases := []struct {
		name string
		pkt  []byte
	}{
		{"empty", nil},
		{"too short", make([]byte, 10)},
		{"header only, no tcp", make([]byte, 20)},
		{"ipv6 version", func() []byte {
			p := ipv4TCP(1, 2, 5)
			p[0] = 0x60
			return p
		}()},
		{"ihl below minimum", func() []byte {
			p := ipv4TCP(1, 2, 5)
			p[0] = 0x43 // IHL=3, below the 20-byte minimum
			return p
		}()},
		{"non-tcp protocol", func() []byte {
			p := ipv4TCP(1, 2, 5)
			p[9] = 17 // UDP
			return p
		}()},
		{"icmp", func() []byte {
			p := ipv4TCP(1, 2, 5)
			p[9] = 1
			return p
		}()},
		{"first fragment (MF set)", func() []byte {
			p := ipv4TCP(1, 2, 5)
			p[6] |= 0x20
			return p
		}()},
		{"later fragment (offset set)", func() []byte {
			p := ipv4TCP(1, 2, 5)
			p[6] |= 0x00
			p[7] = 8 // fragment offset 1
			return p
		}()},
		{"truncated tcp header", ipv4TCP(1, 2, 5)[:30]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := ParseFlowKey(tc.pkt); ok {
				t.Fatalf("%s should not yield a flow key", tc.name)
			}
		})
	}
}

// Fragments must be rejected as a whole: keying only the first fragment would
// split one datagram across documents and reorder it.
func TestParseFlowKeyFragmentOffsetVariants(t *testing.T) {
	for _, off := range []uint16{1, 100, 8191} {
		pkt := ipv4TCP(1, 2, 5)
		pkt[6] = byte(off>>8) & 0x1f
		pkt[7] = byte(off)
		if _, ok := ParseFlowKey(pkt); ok {
			t.Fatalf("fragment offset %d must be rejected", off)
		}
	}
}

// The hash must be stable for a flow and differ between flows; affinity is
// worthless otherwise.
func TestFlowKeyHashStability(t *testing.T) {
	a := transport.FlowKey{Src: [4]byte{10, 0, 0, 1}, Dst: [4]byte{1, 1, 1, 1}, SrcPort: 1000, DstPort: 443}
	b := a
	if a.Hash() != b.Hash() {
		t.Fatal("the same flow must hash the same every time")
	}

	other := a
	other.SrcPort = 1001
	if a.Hash() == other.Hash() {
		t.Fatal("different flows should hash differently")
	}
}
