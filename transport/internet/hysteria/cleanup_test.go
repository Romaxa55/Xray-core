package hysteria

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet"
)

// Every Xray instance has its own *MemoryStreamConfig, so in a process that
// creates an instance per probe the client map keys never repeat. Clients of
// closed instances must leave the map, or the map grows until restart.
func TestManagerDropsClientsOfClosedInstances(t *testing.T) {
	running, err := core.New(&core.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := running.Start(); err != nil {
		t.Fatal(err)
	}
	defer running.Close()

	closed, err := core.New(&core.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Start(); err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}

	dest := net.UDPDestination(net.LocalHostIP, 1)
	keyRunning := dialerConf{dest, &internet.MemoryStreamConfig{}}
	keyClosed := dialerConf{dest, &internet.MemoryStreamConfig{}}
	closedClient := &client{lock: make(chan struct{}, 1), instance: closed, dest: dest}
	m := &clientManager{m: map[dialerConf]*client{
		keyRunning: {lock: make(chan struct{}, 1), instance: running, dest: dest},
		keyClosed:  closedClient,
	}}
	go m.clean()

	deadline := time.Now().Add(3*idleCleanupInterval + time.Second)
	for {
		m.RLock()
		_, hasClosed := m.m[keyClosed]
		m.RUnlock()
		if !hasClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("client of a closed instance is still in the map")
		}
		time.Sleep(50 * time.Millisecond)
	}

	m.RLock()
	_, hasRunning := m.m[keyRunning]
	m.RUnlock()
	if !hasRunning {
		t.Fatal("client of a running instance was dropped")
	}

	closedClient.lock <- struct{}{}
	err = closedClient.dial(context.Background())
	closedClient.unlock()
	if err == nil {
		t.Fatal("dial on a client of a closed instance succeeded")
	}
}
