package internet_test

import (
	"context"
	gonet "net"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
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

// MegaV#360: the finalmask dialer of mKCP, udp and the other UDP transports through a dialerProxy. With
// dialerProxyFallbackTag, DialSystem returns the fallback wrapper of dialer_fallback.go, which hides the cnc type from
// the upstream type switch: panic(reflect.TypeOf(c)) killed the whole process (the app on Windows, the VPN service on
// Android).
func TestFinalMaskDialUDPThroughDialerProxy(t *testing.T) {
	internet.InitSystemDialer(nil, proxyOutbounds{}, nil)
	defer internet.InitSystemDialer(nil, nil, nil)

	for _, fallback := range []string{"", "fallback"} {
		t.Run("fallback="+fallback, func(t *testing.T) {
			ss, err := internet.ToMemoryStreamConfig(&internet.StreamConfig{
				ProtocolName:   "udp",
				SocketSettings: &internet.SocketConfig{DialerProxy: "proxy", DialerProxyFallbackTag: fallback},
				Udpmasks:       []*serial.TypedMessage{serial.ToTypedMessage(&salamander.Config{Password: "1234"})},
			})
			if err != nil {
				t.Fatal(err)
			}
			if ss.FinalMask == nil {
				t.Fatal("no finalmask built")
			}
			conn, err := ss.FinalMask.DialUDP(context.Background(), net.UDPDestination(net.LocalHostIP, 443))
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

// Without a dialerProxy a stream is no UDP socket: an error, not a panic.
func TestDialedPacketConn(t *testing.T) {
	a, b := gonet.Pipe()
	defer a.Close()
	defer b.Close()
	if _, _, err := internet.DialedPacketConn(a, false); err == nil {
		t.Fatal("want an error for a stream without a dialerProxy")
	}
	if _, _, err := internet.DialedPacketConn(a, true); err == nil {
		t.Fatal("want an error for a stream without a TCP remote address")
	}
	if _, _, err := internet.DialedPacketConn(nil, true); err == nil {
		t.Fatal("want an error for no conn")
	}
}
