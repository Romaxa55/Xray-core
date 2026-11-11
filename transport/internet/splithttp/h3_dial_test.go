package splithttp

import (
	"context"
	gotls "crypto/tls"
	"fmt"
	gonet "net"
	"reflect"
	"testing"
	"time"

	"github.com/apernet/quic-go"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/hysteria/udphop"
)

// drainOutbound is the dialerProxy outbound: it takes the stream DialSystem opens to it and never answers.
type drainOutbound struct {
	outbound.Handler
	tag string
}

func (h *drainOutbound) Tag() string { return h.tag }

func (h *drainOutbound) Dispatch(_ context.Context, link *transport.Link) {
	for {
		mb, err := link.Reader.ReadMultiBuffer()
		buf.ReleaseMulti(mb)
		if err != nil {
			return
		}
	}
}

type proxyOutbounds struct{ outbound.Manager }

func (proxyOutbounds) GetHandler(tag string) outbound.Handler {
	if tag == "proxy" || tag == "fallback" {
		return &drainOutbound{tag: tag}
	}
	return nil
}

// MegaV#360: XHTTP over HTTP/3 through a dialerProxy. Port hopping (udpHop) and the fallback wrapper of
// dialer_fallback.go (dialerProxyFallbackTag) reached panic(reflect.TypeOf(c)), which killed the whole process.
func TestH3PacketConnThroughDialerProxy(t *testing.T) {
	internet.InitSystemDialer(nil, proxyOutbounds{}, nil)
	defer internet.InitSystemDialer(nil, nil, nil)

	dest := net.UDPDestination(net.LocalHostIP, 443)
	for _, fallback := range []string{"", "fallback"} {
		for _, ports := range [][]uint32{nil, {443, 8443, 9443}} {
			for _, mask := range []bool{false, true} {
				t.Run(fmt.Sprintf("fallback=%q/hop=%v/mask=%v", fallback, len(ports) > 0, mask), func(t *testing.T) {
					ss := &internet.MemoryStreamConfig{
						SocketSettings: &internet.SocketConfig{DialerProxy: "proxy", DialerProxyFallbackTag: fallback},
					}
					if mask {
						ss.UdpmaskManager = finalmask.NewUdpmaskManager([]finalmask.Udpmask{&salamander.Config{Password: "1234"}})
					}
					qp := &internet.QuicParams{UdpHop: &internet.UdpHop{Ports: ports, IntervalMin: 5, IntervalMax: 10}}
					pc, addr, err := h3PacketConn(context.Background(), dest, ss, qp)
					if err != nil {
						t.Fatalf("dial: %v", err)
					}
					defer pc.Close()
					if addr == nil {
						t.Fatal("no remote address")
					}
					// One stream to the proxy: datagrams over it, never a hopping conn. The mask wraps it in one layer.
					inner := pc
					if mask {
						f := reflect.ValueOf(pc).Elem().FieldByName("PacketConn")
						if !f.IsValid() {
							t.Fatalf("mask wrapper %T has no PacketConn field", pc)
						}
						inner, _ = f.Interface().(net.PacketConn)
					}
					if _, ok := inner.(*internet.FakePacketConn); !ok {
						t.Fatalf("got %T under %T, want *internet.FakePacketConn", inner, pc)
					}
				})
			}
		}
	}
}

// Without a dialerProxy the dial goes to the system dialer: a UDP socket, hopping kept.
func TestH3PacketConnSocket(t *testing.T) {
	ss := &internet.MemoryStreamConfig{SocketSettings: &internet.SocketConfig{}}
	qp := &internet.QuicParams{UdpHop: &internet.UdpHop{Ports: []uint32{443, 8443}, IntervalMin: 5, IntervalMax: 10}}
	pc, addr, err := h3PacketConn(context.Background(), net.UDPDestination(net.LocalHostIP, 443), ss, qp)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer pc.Close()
	if addr == nil || (addr.Port != 443 && addr.Port != 8443) {
		t.Fatalf("got addr %v, want 127.0.0.1 on a hop port", addr)
	}
	if _, ok := pc.(*udphop.UdpHopPacketConn); !ok {
		t.Fatalf("got %T, want *udphop.UdpHopPacketConn: port hopping lost", pc)
	}
}

func h3TLS() *gotls.Config {
	return &gotls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}
}

// MegaV#360: a dead server behind a dialerProxy. quic.DialEarly's single-use Transport waited for a read loop the
// proxy stream never ends, and the dial hung until the proxy outbound dropped the stream (300 s by default).
func TestDialH3ReturnsThroughDialerProxy(t *testing.T) {
	internet.InitSystemDialer(nil, proxyOutbounds{}, nil)
	defer internet.InitSystemDialer(nil, nil, nil)

	for _, tc := range []struct {
		name, fallback string
		ports          []uint32
	}{
		{"proxy", "", nil},
		{"proxy+fallback", "fallback", nil},
		{"proxy+hop", "", []uint32{443, 8443}},
	} {
		// The dial ends either by the handshake timeout or by its context; both paths hung before.
		for _, end := range []struct {
			name           string
			ctxAfter, idle time.Duration
		}{
			{"handshake-timeout", 5 * time.Second, time.Second},
			{"context", time.Second, 30 * time.Second},
		} {
			t.Run(tc.name+"/"+end.name, func(t *testing.T) {
				ss := &internet.MemoryStreamConfig{SocketSettings: &internet.SocketConfig{DialerProxy: "proxy", DialerProxyFallbackTag: tc.fallback}}
				qp := &internet.QuicParams{UdpHop: &internet.UdpHop{Ports: tc.ports, IntervalMin: 5, IntervalMax: 10}}
				ctx, cancel := context.WithTimeout(context.Background(), end.ctxAfter)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					conn, err := dialH3(ctx, net.UDPDestination(net.LocalHostIP, 443), ss, qp, h3TLS(), &quic.Config{HandshakeIdleTimeout: end.idle})
					if conn != nil {
						conn.CloseWithError(0, "")
					}
					done <- err
				}()
				limit := min(end.ctxAfter, end.idle) + 4*time.Second
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("a dial to a server that never answers succeeded")
					}
				case <-time.After(limit):
					t.Fatalf("the dial still hangs %v after it should have ended", limit-min(end.ctxAfter, end.idle))
				}
			})
		}
	}
}

// The client's connection IDs stay empty, as with quic.DialEarly: the Initial packet carries a zero-length source
// connection ID, so the handshake looks on the wire as it did before MegaV#360.
func TestDialH3EmptyConnIDs(t *testing.T) {
	srv, err := gonet.ListenUDP("udp", &gonet.UDPAddr{IP: gonet.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	port := srv.LocalAddr().(*gonet.UDPAddr).Port

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ss := &internet.MemoryStreamConfig{SocketSettings: &internet.SocketConfig{}}
		conn, err := dialH3(ctx, net.UDPDestination(net.LocalHostIP, net.Port(port)), ss, &internet.QuicParams{}, h3TLS(), &quic.Config{HandshakeIdleTimeout: time.Second})
		if err == nil {
			conn.CloseWithError(0, "")
		}
	}()

	b := make([]byte, 2048)
	srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := srv.ReadFrom(b)
	if err != nil {
		t.Fatal(err)
	}
	// Long header: flags, version (4 bytes), DCID length and DCID, SCID length and SCID.
	if n < 7 || b[0]&0x80 == 0 {
		t.Fatalf("not a long-header packet: % x", b[:min(n, 16)])
	}
	dcid := int(b[5])
	if n < 7+dcid {
		t.Fatalf("short Initial: % x", b[:n])
	}
	if scid := b[6+dcid]; scid != 0 {
		t.Errorf("source connection ID of %d bytes, want 0 as with quic.DialEarly", scid)
	}
	cancel()
	<-done
}
