package internet_test

import (
	"context"
	gonet "net"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/kcp"
	_ "github.com/xtls/xray-core/transport/internet/udp"
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

// proxyOutbounds knows the tags "proxy" and "fallback": what DialSystem resolves for sockopt.dialerProxy and
// sockopt.dialerProxyFallbackTag.
type proxyOutbounds struct{ outbound.Manager }

func (proxyOutbounds) GetHandler(tag string) outbound.Handler {
	if tag == "proxy" || tag == "fallback" {
		return &drainOutbound{tag: tag}
	}
	return nil
}

// MegaV#360: mKCP and udp with a udpmask through a dialerProxy. With dialerProxyFallbackTag, DialSystem returns the
// fallback wrapper of dialer_fallback.go, which hid the cnc type from their type switch: panic(reflect.TypeOf(c))
// killed the whole process (the app on Windows, the VPN service on Android).
func TestUDPTransportsThroughDialerProxy(t *testing.T) {
	internet.InitSystemDialer(nil, proxyOutbounds{}, nil)
	defer internet.InitSystemDialer(nil, nil, nil)

	for _, fallback := range []string{"", "fallback"} {
		for _, tc := range []struct {
			name     string
			dest     net.Destination
			protocol string
			settings interface{}
		}{
			{"udp", net.UDPDestination(net.LocalHostIP, 443), "udp", nil},
			{"mkcp", net.TCPDestination(net.LocalHostIP, 443), kcp.ProtocolName, &kcp.Config{
				Mtu: 1350, Tti: 50, UplinkCapacity: 5, DownlinkCapacity: 20, CwndMultiplier: 20, MaxSendingWindow: 2 * 1024 * 1024,
			}},
		} {
			t.Run(tc.name+"/fallback="+fallback, func(t *testing.T) {
				ss := &internet.MemoryStreamConfig{
					ProtocolName:     tc.protocol,
					ProtocolSettings: tc.settings,
					SocketSettings:   &internet.SocketConfig{DialerProxy: "proxy", DialerProxyFallbackTag: fallback},
					UdpmaskManager:   finalmask.NewUdpmaskManager([]finalmask.Udpmask{&salamander.Config{Password: "1234"}}),
				}
				conn, err := internet.Dial(context.Background(), tc.dest, ss)
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				if conn == nil {
					t.Fatal("no conn and no error")
				}
				conn.Close()
			})
		}
	}
}

// Without a dialerProxy a stream is no UDP socket: an error, not a panic.
func TestDialedPacketConnStreamWithoutProxy(t *testing.T) {
	a, b := gonet.Pipe()
	defer a.Close()
	defer b.Close()
	if _, err := internet.UDPHopSocket(a); err == nil {
		t.Fatal("want an error for a hop that is not a UDP socket")
	}
	if _, _, _, err := internet.DialedPacketConn(a, false); err == nil {
		t.Fatal("want an error for a stream without a dialerProxy")
	}
}
