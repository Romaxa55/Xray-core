package outbound_test

import (
	"context"
	"fmt"
	gonet "net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	. "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
)

func TestInterfaces(t *testing.T) {
	_ = (outbound.Handler)(new(Handler))
	_ = (outbound.Manager)(new(Manager))
}

const xrayKey core.XrayKey = 1

func TestOutboundWithoutStatCounter(t *testing.T) {
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{
				System: &policy.SystemPolicy{
					Stats: &policy.SystemPolicy_Stats{
						InboundUplink: true,
					},
				},
			}),
		},
	}

	v, _ := core.New(config)
	v.AddFeature((outbound.Manager)(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	h, _ := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag:           "tag",
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	conn, _ := h.(*Handler).Dial(ctx, net.TCPDestination(net.DomainAddress("localhost"), 13146))
	_, ok := conn.(*stat.CounterConnection)
	if ok {
		t.Errorf("Expected conn to not be CounterConnection")
	}
}

func TestOutboundWithStatCounter(t *testing.T) {
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{
				System: &policy.SystemPolicy{
					Stats: &policy.SystemPolicy_Stats{
						OutboundUplink:   true,
						OutboundDownlink: true,
					},
				},
			}),
		},
	}

	v, _ := core.New(config)
	v.AddFeature((outbound.Manager)(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	h, _ := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag:           "tag",
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	conn, _ := h.(*Handler).Dial(ctx, net.TCPDestination(net.DomainAddress("localhost"), 13146))
	_, ok := conn.(*stat.CounterConnection)
	if !ok {
		t.Errorf("Expected conn to be CounterConnection")
	}
}

func TestTagsCache(t *testing.T) {

	test_duration := 10 * time.Second
	threads_num := 50
	delay := 10 * time.Millisecond
	tags_prefix := "node"

	tags := sync.Map{}
	counter := atomic.Uint64{}

	ohm, err := New(context.Background(), &proxyman.OutboundConfig{})
	if err != nil {
		t.Error("failed to create outbound handler manager")
	}
	config := &core.Config{
		App: []*serial.TypedMessage{},
	}
	v, _ := core.New(config)
	v.AddFeature(ohm)
	ctx := context.WithValue(context.Background(), xrayKey, v)

	stop_add_rm := false
	wg_add_rm := sync.WaitGroup{}
	addHandlers := func() {
		defer wg_add_rm.Done()
		for !stop_add_rm {
			time.Sleep(delay)
			idx := counter.Add(1)
			tag := fmt.Sprintf("%s%d", tags_prefix, idx)
			cfg := &core.OutboundHandlerConfig{
				Tag:           tag,
				ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
			}
			if h, err := NewHandler(ctx, cfg); err == nil {
				if err := ohm.AddHandler(ctx, h); err == nil {
					// t.Log("add handler:", tag)
					tags.Store(tag, nil)
				} else {
					t.Error("failed to add handler:", tag)
				}
			} else {
				t.Error("failed to create handler:", tag)
			}
		}
	}

	rmHandlers := func() {
		defer wg_add_rm.Done()
		for !stop_add_rm {
			time.Sleep(delay)
			tags.Range(func(key interface{}, value interface{}) bool {
				if _, ok := tags.LoadAndDelete(key); ok {
					// t.Log("remove handler:", key)
					ohm.RemoveHandler(ctx, key.(string))
					return false
				}
				return true
			})
		}
	}

	selectors := []string{tags_prefix}
	wg_get := sync.WaitGroup{}
	stop_get := false
	getTags := func() {
		defer wg_get.Done()
		for !stop_get {
			time.Sleep(delay)
			_ = ohm.Select(selectors)
			// t.Logf("get tags: %v", tag)
		}
	}

	for i := 0; i < threads_num; i++ {
		wg_add_rm.Add(2)
		go rmHandlers()
		go addHandlers()
		wg_get.Add(1)
		go getTags()
	}

	time.Sleep(test_duration)
	stop_add_rm = true
	wg_add_rm.Wait()
	stop_get = true
	wg_get.Wait()
}

// A closed outbound frees what the transports keep for its stream settings, and dials no
// more: an observatory's probe that fires after the core closed must not dial a client
// back into a transport's cache.
func TestClosedOutboundReleasesItsStreamAndDialsNoMore(t *testing.T) {
	var mu sync.Mutex
	var released []*internet.MemoryStreamConfig
	internet.RegisterStreamReleaser(func(c *internet.MemoryStreamConfig) {
		mu.Lock()
		defer mu.Unlock()
		released = append(released, c)
	})
	v, _ := core.New(&core.Config{})
	v.AddFeature((outbound.Manager)(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	h, err := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag:            "closing",
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{}),
		ProxySettings:  serial.ToTypedMessage(&freedom.Config{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	listener, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dest := net.DestinationFromAddr(listener.Addr())
	if conn, err := h.(*Handler).Dial(ctx, dest); err != nil {
		t.Fatalf("an open outbound does not dial: %v", err)
	} else {
		conn.Close()
	}
	if accepted, err := listener.Accept(); err != nil {
		t.Fatal(err)
	} else {
		accepted.Close()
	}

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	got := append([]*internet.MemoryStreamConfig(nil), released...)
	mu.Unlock()
	if len(got) != 1 || got[0] == nil {
		t.Fatalf("released on close: %v", got)
	}
	conn, err := h.(*Handler).Dial(ctx, dest)
	if err == nil {
		conn.Close()
		t.Fatal("a closed outbound dialed")
	}
	listener.(*gonet.TCPListener).SetDeadline(time.Now().Add(200 * time.Millisecond))
	if accepted, err := listener.Accept(); err == nil {
		accepted.Close()
		t.Fatal("a closed outbound reached the server before it refused")
	}
}
