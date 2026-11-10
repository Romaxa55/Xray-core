package hysteria

import (
	"context"
	go_tls "crypto/tls"
	"crypto/x509"
	gonet "net"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/hysteria/udphop"
	"github.com/xtls/xray-core/transport/pipe"
)

// proxyStream is what internet.DialSystem returns for a UDP dest through a dialerProxy (the shape of
// internet.redirect): a cnc.Connection over pipes. The "outbound" drains the uplink and never answers.
func proxyStream() net.Conn {
	ur, uw := pipe.New()
	dr, dw := pipe.New()
	go func() {
		for {
			mb, err := ur.ReadMultiBuffer()
			buf.ReleaseMulti(mb)
			if err != nil {
				return
			}
		}
	}()
	return cnc.NewConnection(
		cnc.ConnectionInputMulti(uw),
		cnc.ConnectionOutputMultiUDP(dr),
		cnc.ConnectionOnClose(common.ChainedClosable{uw, dw}),
	)
}

// MegaV#355: what internet.DialSystem returns through a dialerProxy used to reach panic(reflect.TypeOf(c)) in
// dial, which killed the whole process.
func TestHopPacketConn(t *testing.T) {
	t.Run("dialerProxy stream", func(t *testing.T) {
		conn := proxyStream()
		defer conn.Close()
		pc, addr, socket, err := hopPacketConn(conn, true)
		if err != nil || socket || addr == nil {
			t.Fatalf("got socket=%v addr=%v err=%v, want a fake packet conn", socket, addr, err)
		}
		if _, ok := pc.(*internet.FakePacketConn); !ok {
			t.Fatalf("got %T, want *internet.FakePacketConn", pc)
		}
	})

	t.Run("dialerProxy stream behind a wrapper", func(t *testing.T) {
		// The fallback wrapper of dialer_fallback.go hides the cnc type the same way.
		conn := struct{ net.Conn }{proxyStream()}
		defer conn.Close()
		if _, _, socket, err := hopPacketConn(conn, true); err != nil || socket {
			t.Fatalf("got socket=%v err=%v, want a fake packet conn", socket, err)
		}
	})

	t.Run("stream without dialerProxy is an error", func(t *testing.T) {
		conn := proxyStream()
		defer conn.Close()
		if _, _, _, err := hopPacketConn(conn, false); err == nil {
			t.Fatal("want an error: QUIC does not run on a plain stream")
		}
	})

	t.Run("UDP socket", func(t *testing.T) {
		udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IP{127, 0, 0, 1}})
		if err != nil {
			t.Fatal(err)
		}
		defer udp.Close()
		dest := &net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 443}
		for _, viaProxy := range []bool{false, true} {
			pc, addr, socket, err := hopPacketConn(&internet.PacketConnWrapper{PacketConn: udp, Dest: dest}, viaProxy)
			if err != nil || !socket || pc != udp || addr.Port != 443 {
				t.Fatalf("viaProxy=%v: got pc=%T socket=%v addr=%v err=%v, want the socket itself", viaProxy, pc, socket, addr, err)
			}
		}
	})

	t.Run("UDP socket with a non-UDP dest is an error", func(t *testing.T) {
		udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IP{127, 0, 0, 1}})
		if err != nil {
			t.Fatal(err)
		}
		defer udp.Close()
		if _, _, _, err := hopPacketConn(&internet.PacketConnWrapper{PacketConn: udp, Dest: &net.IPAddr{}}, false); err == nil {
			t.Fatal("want an error for a remote address that is not *net.UDPAddr")
		}
	})

	t.Run("unknown conn is an error", func(t *testing.T) {
		a, b := gonet.Pipe()
		defer a.Close()
		defer b.Close()
		if _, _, _, err := hopPacketConn(a, true); err == nil {
			t.Fatal("want an error for a conn without TCP addresses")
		}
		if _, _, _, err := hopPacketConn(nil, true); err == nil {
			t.Fatal("want an error for no conn")
		}
	})
}

// closeQUIC on dial's error path through a dialerProxy that never answers (DialEarly fails, no QUIC conn): the
// PacketConn, then the Transport. Transport.Close waits for its read loop and only sets a read deadline to end it,
// which a cnc.Connection ignores: the other order hangs until the proxy outbound closes the stream (connIdle, 300 s).
func TestCloseOverProxyStream(t *testing.T) {
	pc, addr, _, err := hopPacketConn(proxyStream(), true)
	if err != nil {
		t.Fatal(err)
	}
	tr := &quic.Transport{Conn: pc}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := tr.DialEarly(ctx, addr, &go_tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}, &quic.Config{}); err == nil {
		t.Fatal("the stream never answers: want a dial timeout")
	}
	done := make(chan struct{})
	go func() {
		closeQUIC(nil, closeErrCodeProtocolError, pc, tr)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("closeQUIC still blocked after 3 s")
	}
}

// A QUIC server on pc that accepts and keeps every connection; the returned func stops it.
func quicServer(t *testing.T, pc gonet.PacketConn) func() {
	c, _ := cert.MustGenerate(nil)
	key := common.Must2(x509.ParsePKCS8PrivateKey(c.PrivateKey))
	tlsConfig := &go_tls.Config{
		Certificates: []go_tls.Certificate{{Certificate: [][]byte{c.Certificate}, PrivateKey: key}},
		NextProtos:   []string{"h3"},
	}
	tr := &quic.Transport{Conn: pc}
	l, err := tr.Listen(tlsConfig, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			if _, err := l.Accept(context.Background()); err != nil {
				return
			}
		}
	}()
	return func() {
		_ = l.Close()
		_ = pc.Close()
		_ = tr.Close()
	}
}

// An established client over pc, as dial leaves it.
func connectedClient(t *testing.T, pc gonet.PacketConn, addr gonet.Addr) *client {
	tr := &quic.Transport{Conn: pc}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	qc, err := tr.Dial(ctx, addr, &go_tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	return &client{conn: qc, tr: tr, pktConn: pc}
}

// MegaV#355: client.close (closeQUIC) returns for every PacketConn dial builds, the dialerProxy stream included.
func TestClientClose(t *testing.T) {
	closeWithin := func(t *testing.T, c *client) {
		done := make(chan struct{})
		go func() {
			c.close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("client.close still blocked after 3 s")
		}
	}

	t.Run("UDP socket", func(t *testing.T) {
		spc := common.Must2(gonet.ListenPacket("udp", "127.0.0.1:0"))
		defer quicServer(t, spc)()
		pc := common.Must2(gonet.ListenUDP("udp", &gonet.UDPAddr{IP: gonet.IP{127, 0, 0, 1}}))
		closeWithin(t, connectedClient(t, pc, spc.LocalAddr()))
	})

	t.Run("port hopping", func(t *testing.T) {
		spc := common.Must2(gonet.ListenPacket("udp", "127.0.0.1:0"))
		defer quicServer(t, spc)()
		sa := spc.LocalAddr().(*gonet.UDPAddr)
		pc := common.Must2(gonet.ListenUDP("udp", &gonet.UDPAddr{IP: gonet.IP{127, 0, 0, 1}}))
		hop := udphop.NewUDPHopPacketConn(udphop.ToAddrs(sa.IP, []uint32{uint32(sa.Port)}), 5*time.Second, 5*time.Second,
			func(*gonet.UDPAddr) (gonet.PacketConn, error) { return gonet.ListenUDP("udp", nil) }, pc, 0)
		closeWithin(t, connectedClient(t, hop, sa))
	})

	t.Run("dialerProxy stream", func(t *testing.T) {
		// Two cnc ends over a pair of pipes: the client's stream and the server's.
		ur, uw := pipe.New()
		dr, dw := pipe.New()
		cli := cnc.NewConnection(cnc.ConnectionInputMulti(uw), cnc.ConnectionOutputMultiUDP(dr),
			cnc.ConnectionOnClose(common.ChainedClosable{uw, dw}))
		srv := cnc.NewConnection(cnc.ConnectionInputMulti(dw), cnc.ConnectionOutputMultiUDP(ur))
		defer quicServer(t, &internet.FakePacketConn{Conn: srv})()
		pc, addr, _, err := hopPacketConn(cli, true)
		if err != nil {
			t.Fatal(err)
		}
		closeWithin(t, connectedClient(t, pc, addr))
	})
}
