package utils

import (
	"runtime"
	"testing"
	"time"
	"weak"
)

func (c *WeakCacheMap[K, V]) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

// The map does not grow with the values it outlived: an entry leaves with its value, the
// entry of a value still held stays and loads.
func TestWeakCacheMapForgetsCollectedValues(t *testing.T) {
	c := NewWeakCacheMap[int, [1024]byte]()
	kept := new([1024]byte)
	c.Store(0, kept)
	for i := 1; i <= 100; i++ {
		c.Store(i, new([1024]byte))
	}
	for deadline := time.Now().Add(time.Second); c.size() > 1 && time.Now().Before(deadline); {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if n := c.size(); n != 1 {
		t.Fatalf("the map holds %d entries, want the 1 of the value still held", n)
	}
	if v, ok := c.Load(0); !ok || v != kept {
		t.Fatal("the value still held does not load")
	}
	runtime.KeepAlive(kept)
}

// A key stored again keeps its new value when the value it held before is collected: that
// value's cleanup drops only the entry it was stored with.
func TestWeakCacheMapKeepsAKeyStoredAgain(t *testing.T) {
	c := NewWeakCacheMap[int, [1024]byte]()
	old := new([1024]byte)
	oldGone := weak.Make(old)
	c.Store(1, old)
	c.Store(2, new([1024]byte)) // collected with old: its entry leaving tells the cleanups ran
	kept := new([1024]byte)
	c.Store(1, kept)
	old = nil

	for deadline := time.Now().Add(time.Second); (oldGone.Value() != nil || c.size() > 1) && time.Now().Before(deadline); {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if oldGone.Value() != nil || c.size() > 1 {
		t.Fatal("the values nobody holds were not collected")
	}
	for range 5 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if v, ok := c.Load(1); !ok || v != kept {
		t.Fatal("the cleanup of the value a key held before dropped the key's new value")
	}
	runtime.KeepAlive(kept)
}
