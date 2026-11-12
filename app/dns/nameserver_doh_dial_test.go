package dns_test

import (
	"context"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/xtls/xray-core/app/dns"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// silentRoute dispatches every connection to an outbound that takes what it
// is sent and never answers: an exit that accepts and then passes nothing.
type silentRoute struct {
	dials  atomic.Int32
	closed chan struct{} // the client closed a dispatched connection
}

func (*silentRoute) Type() interface{} { return routing.DispatcherType() }
func (*silentRoute) Start() error      { return nil }
func (*silentRoute) Close() error      { return nil }

func (r *silentRoute) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	r.dials.Add(1)
	upReader, upWriter := pipe.New(pipe.WithoutSizeLimit())
	downReader, _ := pipe.New(pipe.WithoutSizeLimit())
	go func() {
		for {
			mb, err := upReader.ReadMultiBuffer()
			buf.ReleaseMulti(mb)
			if err != nil {
				r.closed <- struct{}{}
				return
			}
		}
	}()
	return &transport.Link{Reader: downReader, Writer: upWriter}, nil
}

func (*silentRoute) DispatchLink(context.Context, net.Destination, *transport.Link) error {
	return errors.New("not used")
}

func TestDOHNameServerEndsSilentDial(t *testing.T) {
	route := &silentRoute{closed: make(chan struct{}, 8)}
	u, err := url.Parse("https://dns.example/dns-query")
	common.Must(err)
	s := NewDoHNameServer(u, route, false, true, false, 0, net.IP(nil))

	// Each query gives up after a second; the dial the first one started
	// is not bound to it and must end by its own deadline, so that a later
	// query dials again — by the route of that moment.
	until := time.Now().Add(10 * time.Second)
	for route.dials.Load() < 2 && time.Now().Before(until) {
		if lookup(s, "a.example.com", time.Second) == nil {
			t.Fatal("an answer through a silent route")
		}
	}
	if got := route.dials.Load(); got < 2 {
		t.Fatalf("dials: %d, want a new one after the first ended", got)
	}
	select {
	case <-route.closed:
	case <-time.After(time.Second):
		t.Fatal("the connection of the ended dial stayed open")
	}
}
