package grpc

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// isolateDialerMap gives the test an empty globalDialerMap and restores the
// previous one afterwards.
func isolateDialerMap(t *testing.T) {
	t.Helper()
	globalDialerAccess.Lock()
	saved, savedRunning := globalDialerMap, dialerJanitorRunning
	globalDialerMap = map[dialerConf]*grpcClient{}
	globalDialerAccess.Unlock()
	t.Cleanup(func() {
		globalDialerAccess.Lock()
		globalDialerMap, dialerJanitorRunning = saved, savedRunning
		globalDialerAccess.Unlock()
	})
}

func testClientConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestSweepIdleDialers(t *testing.T) {
	isolateDialerMap(t)
	idle := dialerIdleTimeout()
	now := 10 * idle

	idleClient := &grpcClient{ClientConn: testClientConn(t), lastUsed: now - idle - time.Second}
	busyClient := &grpcClient{ClientConn: testClientConn(t), active: 1, lastUsed: 0}
	freshClient := &grpcClient{ClientConn: testClientConn(t), lastUsed: now - time.Second}
	dest := net.TCPDestination(net.LocalHostIP, 1)
	globalDialerAccess.Lock()
	globalDialerMap[dialerConf{dest, &internet.MemoryStreamConfig{}}] = idleClient
	globalDialerMap[dialerConf{dest, &internet.MemoryStreamConfig{}}] = busyClient
	globalDialerMap[dialerConf{dest, &internet.MemoryStreamConfig{}}] = freshClient
	dialerJanitorRunning = true
	globalDialerAccess.Unlock()

	if !sweepIdleDialers(now) {
		t.Fatal("busy and fresh clients are left, the janitor must keep running")
	}
	if s := idleClient.GetState(); s != connectivity.Shutdown {
		t.Fatalf("idle client state %v, want Shutdown", s)
	}
	if s := busyClient.GetState(); s == connectivity.Shutdown {
		t.Fatal("busy client closed")
	}
	if s := freshClient.GetState(); s == connectivity.Shutdown {
		t.Fatal("fresh client closed")
	}

	globalDialerAccess.Lock()
	busyClient.active = 0
	globalDialerAccess.Unlock()
	if sweepIdleDialers(now + 2*idle) {
		t.Fatal("map is empty, the janitor must stop")
	}
	globalDialerAccess.Lock()
	n, running := len(globalDialerMap), dialerJanitorRunning
	globalDialerAccess.Unlock()
	if n != 0 || running {
		t.Fatalf("map has %d clients, janitor running %v; want 0, false", n, running)
	}
}

// A tunnel counts as open until its Close (called twice here, as proxies do),
// and the janitor closes the ClientConn of an instance whose tunnels are gone.
func TestDialerJanitorTracksTunnels(t *testing.T) {
	isolateDialerMap(t)

	port := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{ProtocolName: protocolName, ProtocolSettings: &Config{ServiceName: "janitor"}}
	ln, err := Listen(context.Background(), net.LocalHostIP, port, settings, func(c stat.Connection) {
		go func() {
			defer c.Close()
			_, _ = io.Copy(c, c)
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	dest := net.TCPDestination(net.LocalHostIP, port)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	open := func() stat.Connection {
		t.Helper()
		conn, err := Dial(ctx, dest, settings)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	client := func() *grpcClient {
		t.Helper()
		globalDialerAccess.Lock()
		defer globalDialerAccess.Unlock()
		if len(globalDialerMap) != 1 {
			t.Fatalf("map has %d clients, want 1", len(globalDialerMap))
		}
		for _, c := range globalDialerMap {
			return c
		}
		return nil
	}
	active := func(c *grpcClient) int {
		globalDialerAccess.Lock()
		defer globalDialerAccess.Unlock()
		return c.active
	}

	a, b := open(), open()
	c := client()
	if n := active(c); n != 2 {
		t.Fatalf("two tunnels on one ClientConn: active %d, want 2", n)
	}
	a.Close()
	a.Close()
	if n := active(c); n != 1 {
		t.Fatalf("after closing one tunnel twice: active %d, want 1", n)
	}
	if !sweepIdleDialers(monoNow() + 2*dialerIdleTimeout()) {
		t.Fatal("a tunnel is open, the janitor must keep the client")
	}
	if s := c.GetState(); s == connectivity.Shutdown {
		t.Fatal("client with an open tunnel closed")
	}

	b.Close()
	if n := active(c); n != 0 {
		t.Fatalf("all tunnels closed: active %d, want 0", n)
	}
	if sweepIdleDialers(monoNow() + 2*dialerIdleTimeout()) {
		t.Fatal("no tunnels left, the janitor must drop the client and stop")
	}
	if s := c.GetState(); s != connectivity.Shutdown {
		t.Fatalf("idle client state %v, want Shutdown", s)
	}

	// The same key dials a new ClientConn after the janitor dropped the old one.
	d := open()
	defer d.Close()
	if c2 := client(); c2 == c {
		t.Fatal("dropped ClientConn reused")
	}
}
