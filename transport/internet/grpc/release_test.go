package grpc

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"google.golang.org/grpc/connectivity"
)

// Each outbound's clients live as long as the outbound: releasing one stream config closes
// and forgets its clients and leaves the others connecting.
func TestReleaseClosesOnlyItsOwnClients(t *testing.T) {
	settings := func() *internet.MemoryStreamConfig {
		return &internet.MemoryStreamConfig{ProtocolName: protocolName, ProtocolSettings: &Config{ServiceName: "release"}}
	}
	closed, open := settings(), settings()
	dest := net.TCPDestination(net.LocalHostIP, net.Port(1))
	gone, err := getGrpcClient(context.Background(), dest, closed)
	if err != nil {
		t.Fatal(err)
	}
	kept, err := getGrpcClient(context.Background(), dest, open)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { internet.ReleaseStreamConfig(open) })

	internet.ReleaseStreamConfig(closed)

	if state := gone.GetState(); state != connectivity.Shutdown {
		t.Errorf("the released client is %v, not shut down", state)
	}
	if state := kept.GetState(); state == connectivity.Shutdown {
		t.Error("the other outbound's client is shut down too")
	}
	globalDialerAccess.Lock()
	_, cached := globalDialerMap[dialerConf{dest, closed}]
	_, stays := globalDialerMap[dialerConf{dest, open}]
	globalDialerAccess.Unlock()
	if cached || !stays {
		t.Errorf("cached after the release: the released %v, the other %v", cached, stays)
	}

	again, err := getGrpcClient(context.Background(), dest, open)
	if err != nil {
		t.Fatal(err)
	}
	if again != kept {
		t.Error("the other outbound dials a new client instead of its own")
	}
}
