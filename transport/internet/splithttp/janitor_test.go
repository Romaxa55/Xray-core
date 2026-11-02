package splithttp

import (
	"bytes"
	"context"
	"crypto/rand"
	gonet "net"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/udp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type janitorConn struct {
	closed     bool
	closeCalls int
}

func (c *janitorConn) IsClosed() bool { return c.closed }

func (c *janitorConn) Close() error {
	c.closeCalls++
	return nil
}

func closeAll(removed []*XmuxClient) {
	for _, c := range removed {
		c.maybeClose()
	}
}

func TestRemoveIdleKeepsBusyAndFreshClients(t *testing.T) {
	conn := &janitorConn{}
	m := NewXmuxManager(XmuxConfig{}, func() XmuxConn { return conn })
	c := m.GetXmuxClient(context.Background())

	removed, empty := m.removeIdle(monoNow(), time.Minute, nil)
	if empty || len(removed) != 0 {
		t.Fatal("a fresh client was removed")
	}

	c.AddRunning()
	removed, empty = m.removeIdle(monoNow()+time.Hour, time.Minute, nil)
	if empty || len(removed) != 0 {
		t.Fatal("a client with a running request was removed")
	}

	c.DoneRunning()
	removed, empty = m.removeIdle(monoNow()+time.Hour, time.Minute, nil)
	if !empty || len(removed) != 1 {
		t.Fatal("an idle client was kept")
	}
	if conn.closeCalls != 0 {
		t.Fatal("removeIdle closed the client itself")
	}
	closeAll(removed)
	if conn.closeCalls != 1 {
		t.Fatalf("Close called %d times, want 1", conn.closeCalls)
	}
	if len(m.xmuxClients) != 0 {
		t.Fatalf("%d clients left in the manager", len(m.xmuxClients))
	}
}

func TestRemoveIdleDropsClosedClientAndClosesItWhenDone(t *testing.T) {
	conn := &janitorConn{}
	m := NewXmuxManager(XmuxConfig{}, func() XmuxConn { return conn })
	c := m.GetXmuxClient(context.Background())
	c.AddRunning()
	conn.closed = true

	removed, empty := m.removeIdle(monoNow(), time.Hour, nil)
	if !empty || len(removed) != 1 {
		t.Fatal("a closed client was kept")
	}
	closeAll(removed)
	if conn.closeCalls != 0 {
		t.Fatal("Close called while a request is still running")
	}
	c.DoneRunning()
	if conn.closeCalls != 1 {
		t.Fatalf("Close called %d times after the last request, want 1", conn.closeCalls)
	}
}

func TestSweepIdleDialersDropsEmptyManagersAndStops(t *testing.T) {
	globalDialerAccess.Lock()
	savedMap, savedRunning := globalDialerMap, dialerJanitorRunning
	conn := &janitorConn{}
	m := NewXmuxManager(XmuxConfig{}, func() XmuxConn { return conn })
	m.GetXmuxClient(context.Background())
	globalDialerMap = map[dialerConf]*XmuxManager{{}: m}
	dialerJanitorRunning = true
	globalDialerAccess.Unlock()
	t.Cleanup(func() {
		globalDialerAccess.Lock()
		globalDialerMap, dialerJanitorRunning = savedMap, savedRunning
		globalDialerAccess.Unlock()
	})

	if !sweepIdleDialers(monoNow()) {
		t.Fatal("the janitor stopped while a manager is in use")
	}
	if len(globalDialerMap) != 1 {
		t.Fatal("a fresh manager was removed")
	}

	if sweepIdleDialers(monoNow() + dialerIdleTimeout() + time.Second) {
		t.Fatal("the janitor keeps running on an empty map")
	}
	if len(globalDialerMap) != 0 {
		t.Fatalf("%d managers left in the map", len(globalDialerMap))
	}
	if dialerJanitorRunning {
		t.Fatal("dialerJanitorRunning is still set")
	}
	if conn.closeCalls != 1 {
		t.Fatalf("Close called %d times, want 1", conn.closeCalls)
	}
}

func listenEchoXH(t *testing.T, port net.Port, streamSettings *internet.MemoryStreamConfig) {
	listen, err := ListenXH(context.Background(), net.LocalHostIP, port, streamSettings, func(conn stat.Connection) {
		go func() {
			defer conn.Close()
			b := buf.New()
			defer b.Release()
			for {
				b.Clear()
				if _, err := b.ReadFrom(conn); err != nil {
					return
				}
				common.Must2(conn.Write(b.Bytes()))
			}
		}()
	})
	common.Must(err)
	t.Cleanup(func() { listen.Close() })
	time.Sleep(500 * time.Millisecond)
}

func h3StreamSettings(config *Config) *internet.MemoryStreamConfig {
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	return &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: config,
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
			PinnedPeerCertSha256: [][]byte{ctHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}
}

// A packet-up upload rotated to another client (hMaxRequestTimes hit) keeps
// posting through it; the janitor must not close that client under a live
// connection.
func TestJanitorKeepsRotatedUploadClient(t *testing.T) {
	port := udp.PickPort()
	streamSettings := h3StreamSettings(&Config{
		Path: "shs",
		Mode: "packet-up",
		Xmux: &XmuxConfig{HMaxRequestTimes: &RangeConfig{From: 4, To: 4}},
	})
	listenEchoXH(t, port, streamSettings)

	dest := net.UDPDestination(net.DomainAddress("localhost"), port)
	conn, err := Dial(context.Background(), dest, streamSettings)
	common.Must(err)
	defer conn.Close()
	globalDialerAccess.Lock()
	first := globalDialerMap[dialerConf{dest, streamSettings}].xmuxClients[0]
	globalDialerAccess.Unlock()

	echo := func(step int) bool {
		b1 := make([]byte, 256)
		common.Must2(rand.Read(b1))
		if _, err := conn.Write(b1); err != nil {
			t.Logf("step %d: write error %v", step, err)
			return false
		}
		got := make(chan []byte, 1)
		go func() {
			b2 := buf.New()
			if _, err := b2.ReadFullFrom(conn, 256); err != nil {
				got <- nil
				return
			}
			got <- b2.Bytes()
		}()
		select {
		case b := <-got:
			return bytes.Equal(b, b1)
		case <-time.After(3 * time.Second):
			t.Logf("step %d: no echo in 3s", step)
			return false
		}
	}

	for i := 0; i < 4; i++ {
		if !echo(i) {
			t.Fatalf("echo %d failed before the sweep", i)
		}
	}
	globalDialerAccess.Lock()
	rotated := !slices.Contains(globalDialerMap[dialerConf{dest, streamSettings}].xmuxClients, first)
	globalDialerAccess.Unlock()
	if !rotated {
		t.Fatal("the upload did not rotate to another client, the test checks nothing")
	}
	sweepIdleDialers(monoNow() + dialerIdleTimeout() + time.Second)
	if !echo(100) {
		t.Fatal("the live connection broke after the sweep: its upload client was closed")
	}
}

// An idle XHTTP/3 client is closed for real, and its manager leaves the map.
func TestJanitorClosesIdleH3(t *testing.T) {
	port := udp.PickPort()
	streamSettings := h3StreamSettings(&Config{Path: "shs"})
	listenEchoXH(t, port, streamSettings)

	dest := net.UDPDestination(net.DomainAddress("localhost"), port)
	conn, err := Dial(context.Background(), dest, streamSettings)
	common.Must(err)
	common.Must2(conn.Write([]byte("ping")))
	b2 := buf.New()
	common.Must2(b2.ReadFullFrom(conn, 4))

	globalDialerAccess.Lock()
	dc := globalDialerMap[dialerConf{dest, streamSettings}].xmuxClients[0].XmuxConn.(*DefaultDialerClient)
	running := dialerJanitorRunning
	globalDialerAccess.Unlock()
	if !running {
		t.Fatal("getHTTPClient did not start the janitor")
	}
	conn.Close()
	time.Sleep(100 * time.Millisecond)

	sweepIdleDialers(monoNow() + dialerIdleTimeout() + time.Second)
	globalDialerAccess.Lock()
	_, kept := globalDialerMap[dialerConf{dest, streamSettings}]
	globalDialerAccess.Unlock()
	if kept {
		t.Fatal("the manager of an idle client stayed in the map")
	}
	req, _ := http.NewRequest("GET", "https://localhost/", nil)
	if _, err := dc.client.Transport.RoundTrip(req); err != http3.ErrTransportClosed {
		t.Fatalf("the h3 transport is not closed: %v", err)
	}
}

// delayRelay forwards UDP between clients and the server on loopback, one
// upstream socket per client address, delaying every packet by d each way.
func delayRelay(t *testing.T, serverPort net.Port, d time.Duration) net.Port {
	front, err := gonet.ListenUDP("udp", &gonet.UDPAddr{IP: gonet.IPv4(127, 0, 0, 1)})
	common.Must(err)
	t.Cleanup(func() { front.Close() })
	var mu sync.Mutex
	backs := map[string]*gonet.UDPConn{}
	go func() {
		for {
			b := make([]byte, 2048)
			n, a, err := front.ReadFromUDP(b)
			if err != nil {
				return
			}
			mu.Lock()
			back, ok := backs[a.String()]
			if !ok {
				back, err = gonet.DialUDP("udp", nil, &gonet.UDPAddr{IP: gonet.IPv4(127, 0, 0, 1), Port: int(serverPort)})
				common.Must(err)
				backs[a.String()] = back
				t.Cleanup(func() { back.Close() })
				go func(back *gonet.UDPConn, a *gonet.UDPAddr) {
					for {
						b := make([]byte, 2048)
						n, err := back.Read(b)
						if err != nil {
							return
						}
						p := b[:n]
						time.AfterFunc(d, func() { front.WriteToUDP(p, a) })
					}
				}(back, a)
			}
			mu.Unlock()
			p := b[:n]
			time.AfterFunc(d, func() { back.Write(p) })
		}
	}()
	return net.Port(front.LocalAddr().(*gonet.UDPAddr).Port)
}

// packet-up over h3 with an 80 ms RTT rotates clients every few POSTs while a
// POST still waits for its response; a retired client must stay open until
// that POST returns. With maxConcurrency 1 a second conn may share the
// rotated client.
func TestInflightRotation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		concurrency   *RangeConfig
		chunks, other int
	}{
		{"single", nil, 40, -1},
		{"shared-early", &RangeConfig{From: 1, To: 1}, 8, 2},
		{"shared-late", &RangeConfig{From: 1, To: 1}, 20, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			port := udp.PickPort()
			streamSettings := h3StreamSettings(&Config{
				Path: "shs",
				Mode: "packet-up",
				Xmux: &XmuxConfig{
					MaxConcurrency:   tc.concurrency,
					HMaxRequestTimes: &RangeConfig{From: 3, To: 3},
				},
			})
			listenEchoXH(t, port, streamSettings)
			dest := net.UDPDestination(net.DomainAddress("localhost"), delayRelay(t, port, 40*time.Millisecond))
			conn, err := Dial(context.Background(), dest, streamSettings)
			common.Must(err)
			defer conn.Close()

			const size = 100
			sent := make([]byte, 0, tc.chunks*size)
			got := make(chan []byte, 1)
			go func() {
				all := make([]byte, 0, tc.chunks*size)
				b := make([]byte, 4096)
				for len(all) < tc.chunks*size {
					n, err := conn.Read(b)
					if err != nil {
						break
					}
					all = append(all, b[:n]...)
				}
				got <- all
			}()
			for i := 0; i < tc.chunks; i++ {
				if i == tc.other {
					other, err := Dial(context.Background(), dest, streamSettings)
					common.Must(err)
					defer other.Close()
				}
				p := make([]byte, size)
				rand.Read(p)
				sent = append(sent, p...)
				if _, err := conn.Write(p); err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
				time.Sleep(35 * time.Millisecond)
			}
			select {
			case all := <-got:
				if !bytes.Equal(all, sent) {
					t.Fatalf("echoed %d of %d bytes", len(all), len(sent))
				}
			case <-time.After(10 * time.Second):
				t.Fatal("echo timed out")
			}
		})
	}
}

// A server that keeps QUIC alive but never answers a packet-up POST: the POST
// times out, releases its hold, and the janitor closes the client.
func TestJanitorClosesClientOfHungPost(t *testing.T) {
	saved := postPacketTimeout
	postPacketTimeout = func() time.Duration { return 2 * time.Second }
	t.Cleanup(func() { postPacketTimeout = saved })

	port := udp.PickPort()
	streamSettings := h3StreamSettings(&Config{Path: "shs", Mode: "packet-up"})
	serverTLS := streamSettings.SecuritySettings.(*tls.Config).GetTLSConfig()
	serverTLS.NextProtos = []string{"h3"}
	block := make(chan struct{})
	posts := make(chan struct{}, 16)
	srv := &http3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
		} else {
			posts <- struct{}{}
		}
		select {
		case <-r.Context().Done():
		case <-block:
		}
	})}
	ln, err := quic.ListenAddrEarly(gonet.JoinHostPort("127.0.0.1", port.String()), serverTLS, &quic.Config{})
	common.Must(err)
	go srv.ServeListener(ln)
	t.Cleanup(func() { close(block); srv.Close(); ln.Close() })

	dest := net.UDPDestination(net.DomainAddress("localhost"), port)
	conn, err := Dial(context.Background(), dest, streamSettings)
	common.Must(err)
	common.Must2(conn.Write([]byte("hello")))
	select {
	case <-posts:
	case <-time.After(3 * time.Second):
		t.Fatal("no POST reached the server")
	}
	globalDialerAccess.Lock()
	c := globalDialerMap[dialerConf{dest, streamSettings}].xmuxClients[0]
	globalDialerAccess.Unlock()
	conn.Close()
	if c.holds.Load() == 0 {
		t.Fatal("the hung POST does not hold its client")
	}

	deadline := time.Now().Add(6 * time.Second)
	for c.holds.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if h := c.holds.Load(); h > 0 {
		t.Fatalf("the hung POST still holds its client: holds = %d", h)
	}

	sweepIdleDialers(monoNow() + dialerIdleTimeout() + time.Second)
	globalDialerAccess.Lock()
	_, kept := globalDialerMap[dialerConf{dest, streamSettings}]
	globalDialerAccess.Unlock()
	if kept {
		t.Fatal("the manager of the hung client stayed in the map")
	}
	req, _ := http.NewRequest("GET", "https://localhost/", nil)
	if _, err := c.XmuxConn.(*DefaultDialerClient).client.Transport.RoundTrip(req); err != http3.ErrTransportClosed {
		t.Fatalf("the h3 transport is not closed: %v", err)
	}
}
