package utils

import (
	"runtime"
	"testing"
	"time"
)

// The map does not grow with the values it outlived: an entry leaves with its value, the
// entry of a value still held stays and loads.
func TestWeakCacheMapForgetsCollectedValues(t *testing.T) {
	c := NewWeakCacheMap[int, [1024]byte]()
	kept := new([1024]byte)
	c.Store(0, kept)
	for i := 1; i <= 100; i++ {
		c.Store(i, new([1024]byte))
	}
	size := func() int {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.m)
	}
	for deadline := time.Now().Add(time.Second); size() > 1 && time.Now().Before(deadline); {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if n := size(); n != 1 {
		t.Fatalf("the map holds %d entries, want the 1 of the value still held", n)
	}
	if v, ok := c.Load(0); !ok || v != kept {
		t.Fatal("the value still held does not load")
	}
	runtime.KeepAlive(kept)
}
