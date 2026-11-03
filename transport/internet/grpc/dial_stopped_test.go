package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	"google.golang.org/grpc/connectivity"
)

func startedInstance(t *testing.T) *core.Instance {
	t.Helper()
	inst, err := core.New(&core.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	return inst
}

// A tunnel blocked on its first read (the server accepted the stream and stays
// silent, as in a VLESS encryption handshake) never reaches Close. Once its
// instance is stopped the janitor closes the ClientConn anyway and the read
// returns; the tunnel of a running instance is left alone.
func TestDialerJanitorClosesClientsOfStoppedInstances(t *testing.T) {
	isolateDialerMap(t)
	// The test sweeps by hand; a real janitor started by Dial would outlive it.
	globalDialerAccess.Lock()
	dialerJanitorRunning = true
	globalDialerAccess.Unlock()

	port := tcp.PickPort()
	serverSettings := &internet.MemoryStreamConfig{ProtocolName: protocolName, ProtocolSettings: &Config{ServiceName: "silent"}}
	hold := make(chan struct{})
	ln, err := Listen(context.Background(), net.LocalHostIP, port, serverSettings, func(c stat.Connection) {
		go func() {
			defer c.Close()
			<-hold
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	defer close(hold)

	running, stopped := startedInstance(t), startedInstance(t)
	defer running.Close()

	dest := net.TCPDestination(net.LocalHostIP, port)
	type tunnel struct {
		conn   stat.Connection
		client *grpcClient
		read   chan error
	}
	open := func(inst *core.Instance) tunnel {
		t.Helper()
		// Each instance has its own stream settings, so its own map key.
		settings := &internet.MemoryStreamConfig{ProtocolName: protocolName, ProtocolSettings: &Config{ServiceName: "silent"}}
		ctx := context.WithValue(context.Background(), core.XrayKey(1), inst)
		conn, err := Dial(ctx, dest, settings)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		read := make(chan error, 1)
		go func() {
			_, err := conn.Read(make([]byte, 1))
			read <- err
		}()
		globalDialerAccess.Lock()
		client := globalDialerMap[dialerConf{dest, settings}]
		globalDialerAccess.Unlock()
		if client == nil || client.instance != inst {
			t.Fatal("tunnel's client is not in the map with its instance")
		}
		return tunnel{conn, client, read}
	}
	a, b := open(running), open(stopped)
	defer a.client.Close()
	defer a.conn.Close()
	defer b.conn.Close()

	if !sweepIdleDialers(monoNow()) {
		t.Fatal("tunnels are open, the janitor must keep running")
	}
	select {
	case err := <-b.read:
		t.Fatalf("read returned before the instance stopped: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	if err := stopped.Close(); err != nil {
		t.Fatal(err)
	}
	if !sweepIdleDialers(monoNow()) {
		t.Fatal("the running instance's tunnel is open, the janitor must keep running")
	}
	select {
	case err := <-b.read:
		if err == nil {
			t.Fatal("read of a closed ClientConn returned no error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tunnel of a stopped instance is still blocked")
	}
	if s := b.client.GetState(); s != connectivity.Shutdown {
		t.Fatalf("stopped instance's client state %v, want Shutdown", s)
	}
	select {
	case err := <-a.read:
		t.Fatalf("running instance's tunnel returned: %v", err)
	default:
	}
	if s := a.client.GetState(); s == connectivity.Shutdown {
		t.Fatal("running instance's client closed")
	}
	globalDialerAccess.Lock()
	n := len(globalDialerMap)
	globalDialerAccess.Unlock()
	if n != 1 {
		t.Fatalf("map has %d clients, want 1 (the running instance's)", n)
	}
}
