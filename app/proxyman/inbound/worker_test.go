package inbound

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// readUntilEOF stands for socks.(*Server).handleUDPPayload: it reads the UDP conn until the read fails.
type readUntilEOF struct {
	started chan struct{}
	ended   chan struct{}
	calls   atomic.Int32
}

func (*readUntilEOF) Network() []net.Network { return []net.Network{net.Network_UDP} }

func (p *readUntilEOF) Process(_ context.Context, _ net.Network, conn stat.Connection, _ routing.Dispatcher) error {
	if p.calls.Add(1) > 1 {
		return nil
	}
	close(p.started)
	defer close(p.ended)
	reader := conn.(buf.Reader)
	for {
		mb, err := reader.ReadMultiBuffer()
		if err != nil {
			return err
		}
		buf.ReleaseMulti(mb)
	}
}

// The checker is the only thing that ends idle UDP connections, and Close stops it: a connection still in
// activeConn kept its Process goroutine (and the dispatcher pipes behind it) blocked for the life of the process.
func TestUDPWorkerCloseEndsActiveConnections(t *testing.T) {
	p := &readUntilEOF{started: make(chan struct{}), ended: make(chan struct{})}
	w := &udpWorker{
		proxy:   p,
		address: net.LocalHostIP,
		port:    net.Port(0),
		stream:  &internet.MemoryStreamConfig{},
		ctx:     context.WithValue(context.Background(), "cone", true),
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	b := buf.New()
	b.WriteString("query")
	w.callback(b, net.UDPDestination(net.LocalHostIP, net.Port(40000)), net.Destination{})
	select {
	case <-p.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Process did not start")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("Process is still blocked on the UDP conn after the worker was closed")
	}

	// A packet the hub had queued before Close must not open a new connection.
	late := buf.New()
	late.WriteString("late")
	w.callback(late, net.UDPDestination(net.LocalHostIP, net.Port(40001)), net.Destination{})
	time.Sleep(200 * time.Millisecond)

	w.RLock()
	left := len(w.activeConn)
	w.RUnlock()
	if left != 0 {
		t.Fatalf("activeConn keeps %d connections after Close", left)
	}
	if n := p.calls.Load(); n != 1 {
		t.Fatalf("Process ran %d times, want 1: a packet after Close opened a connection", n)
	}
}

// Close can take the lock between getConnection and checker.Start in callback. Start runs clean() at once, which
// fails on the now empty activeConn: callback must drop the packet, not panic (a panic kills a gomobile host app).
func TestUDPWorkerCallbackSurvivesCloseBeforeCheckerStart(t *testing.T) {
	p := &readUntilEOF{started: make(chan struct{}), ended: make(chan struct{})}
	w := &udpWorker{
		proxy:   p,
		address: net.LocalHostIP,
		port:    net.Port(0),
		stream:  &internet.MemoryStreamConfig{},
		ctx:     context.WithValue(context.Background(), "cone", true),
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	var closeOnce sync.Once
	w.checker = &task.Periodic{
		Interval: time.Minute,
		Execute: func() error {
			closeOnce.Do(func() { _ = w.Close() })
			return w.clean()
		},
	}

	b := buf.New()
	b.WriteString("query")
	w.callback(b, net.UDPDestination(net.LocalHostIP, net.Port(40002)), net.Destination{})
	time.Sleep(200 * time.Millisecond)

	if n := p.calls.Load(); n != 0 {
		t.Fatalf("Process ran %d times for a connection Close had already closed", n)
	}
}
