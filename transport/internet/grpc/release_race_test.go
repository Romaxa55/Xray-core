package grpc

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"
	features_outbound "github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport/internet"
)

func cachedClients(serviceName string) int {
	globalDialerAccess.Lock()
	defer globalDialerAccess.Unlock()
	n := 0
	for conf := range globalDialerMap {
		if config, ok := conf.MemoryStreamConfig.ProtocolSettings.(*Config); ok && config.ServiceName == serviceName {
			n++
		}
	}
	return n
}

// An outbound closed while its dials are under way keeps no gRPC client: a dial that
// built its client after the close released the stream config frees what it built.
func TestCloseRacingDialsLeavesNoClient(t *testing.T) {
	base := runtime.NumGoroutine()
	dest := net.TCPDestination(net.LocalHostIP, net.Port(1))
	for i := 0; i < 100; i++ {
		serviceName := fmt.Sprintf("close-racing-dials-%d", i)
		v, _ := core.New(&core.Config{})
		v.AddFeature((features_outbound.Manager)(new(outbound.Manager)))
		ctx := context.WithValue(context.Background(), core.XrayKey(1), v)
		ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
		h, err := outbound.NewHandler(ctx, &core.OutboundHandlerConfig{
			Tag: serviceName,
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{StreamSettings: &internet.StreamConfig{
				ProtocolName: protocolName,
				TransportSettings: []*internet.TransportConfig{{
					ProtocolName: protocolName,
					Settings:     serial.ToTypedMessage(&Config{ServiceName: serviceName}),
				}},
			}}),
			ProxySettings: serial.ToTypedMessage(&freedom.Config{}),
		})
		if err != nil {
			t.Fatal(err)
		}
		handler := h.(*outbound.Handler)
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for d := 0; d < 4; d++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					dialCtx := session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
					if conn, err := handler.Dial(dialCtx, dest); err == nil {
						conn.Close()
					}
					time.Sleep(time.Duration(rand.Intn(200)) * time.Microsecond)
				}
			}()
		}
		time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
		h.Close()
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
		close(stop)
		wg.Wait()
		if n := cachedClients(serviceName); n != 0 {
			t.Fatalf("round %d: %d client(s) of a closed outbound stay in the cache", i, n)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > base+3 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base+3 {
		t.Fatalf("goroutines: %d before, %d after the closed outbounds", base, n)
	}
}
