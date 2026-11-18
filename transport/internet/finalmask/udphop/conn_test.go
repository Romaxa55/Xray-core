package udphop

import (
	stdnet "net"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/finalmask"
)

// Over a dialerProxy stream the dial reports 0.0.0.0 as the remote: no hopping there, the conn itself comes back.
func TestNewUDPHopConnNoHopOverStream(t *testing.T) {
	sock, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	dials := 0
	d := &finalmask.Dialer{DialUDP: func(net.Destination) (net.Conn, error) {
		dials++
		return &net.PacketConnWrapper{PacketConn: sock, Dest: &net.UDPAddr{IP: []byte{0, 0, 0, 0}}}, nil
	}}
	dest := net.UDPDestination(net.LocalHostIP, 443)
	got, err := NewUDPHopConn(&Config{IntervalMin: 5, IntervalMax: 10}, &dest, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, hop := got.(*udpHopConn); hop {
		got.Close()
		t.Fatal("port hopping started over a stream: hop() would dial udp:0.0.0.0")
	}
	if got != sock || dials != 1 {
		t.Fatalf("want the dialed conn back after one dial, got %T after %d dials", got, dials)
	}
}

// A socket keeps hopping.
func TestNewUDPHopConnHopsOnSocket(t *testing.T) {
	sock, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &finalmask.Dialer{DialUDP: func(net.Destination) (net.Conn, error) {
		return &net.PacketConnWrapper{PacketConn: sock, Dest: &net.UDPAddr{IP: stdnet.IPv4(127, 0, 0, 1), Port: 443}}, nil
	}}
	dest := net.UDPDestination(net.LocalHostIP, 443)
	got, err := NewUDPHopConn(&Config{IntervalMin: 5, IntervalMax: 10}, &dest, d)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if _, hop := got.(*udpHopConn); !hop {
		t.Fatalf("want a hopping conn on a UDP socket, got %T", got)
	}
}
