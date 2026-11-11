package splithttp

import (
	"context"
	gotls "crypto/tls"
	"testing"
	"time"

	"github.com/apernet/quic-go"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
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
		mask           bool
	}{
		{"proxy", "", false},
		{"proxy+fallback", "fallback", false},
		{"proxy+mask", "", true},
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
				sc := &internet.StreamConfig{
					ProtocolName:   "splithttp",
					SocketSettings: &internet.SocketConfig{DialerProxy: "proxy", DialerProxyFallbackTag: tc.fallback},
				}
				if tc.mask {
					sc.Udpmasks = []*serial.TypedMessage{serial.ToTypedMessage(&salamander.Config{Password: "1234"})}
				}
				ss, err := internet.ToMemoryStreamConfig(sc)
				if err != nil {
					t.Fatal(err)
				}
				qp := &internet.QuicParams{}
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
