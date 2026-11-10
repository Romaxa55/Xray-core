package internet

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/transport/pipe"
)

// A dialerProxy stream that never answers (the shape of redirect).
func silentStream() net.Conn {
	ur, uw := pipe.New()
	dr, dw := pipe.New()
	go func() {
		for {
			mb, err := ur.ReadMultiBuffer()
			buf.ReleaseMulti(mb)
			if err != nil {
				return
			}
		}
	}()
	return cnc.NewConnection(cnc.ConnectionInputMulti(uw), cnc.ConnectionOutputMultiUDP(dr),
		cnc.ConnectionOnClose(common.ChainedClosable{uw, dw}))
}

// A dialerProxy stream whose downlink ends after d: a chain failure (EOF, not net.ErrClosed), the fallback's trigger.
func failingStream(d time.Duration) net.Conn {
	ur, uw := pipe.New()
	dr, dw := pipe.New()
	go func() {
		for {
			mb, err := ur.ReadMultiBuffer()
			buf.ReleaseMulti(mb)
			if err != nil {
				return
			}
		}
	}()
	go func() {
		time.Sleep(d)
		dw.Close()
	}()
	return cnc.NewConnection(cnc.ConnectionInputMulti(uw), cnc.ConnectionOutputMultiUDP(dr),
		cnc.ConnectionOnClose(common.ChainedClosable{uw, dw}))
}

type countingConn struct {
	net.Conn
	closed atomic.Int32
}

func (c *countingConn) Close() error {
	c.closed.Add(1)
	return c.Conn.Close()
}

// MegaV#355: Read swaps the active conn on a fallback while another goroutine writes and sets deadlines (QUIC does
// both). Under -race this caught the unlocked c.Conn reads.
func TestFallbackSwapUnderConcurrentWrites(t *testing.T) {
	c := wrapFallback(context.Background(), failingStream(50*time.Millisecond), func() (net.Conn, error) {
		return silentStream(), nil
	})
	if _, err := c.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// One loop per call: an unlock right after the call under test would order its read before the swap and hide
	// the race from the detector.
	loop := func(call func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				call()
				time.Sleep(time.Millisecond)
			}
		}()
	}
	loop(func() { _, _ = c.Write(make([]byte, 10)) })
	loop(func() { _ = c.SetReadDeadline(time.Time{}) })
	go func() { _, _ = c.Read(make([]byte, 100)) }()
	deadline := time.Now().Add(2 * time.Second)
	fc := c.(*fallbackDialerConn)
	for {
		fc.mu.Lock()
		promoted := fc.promoted
		fc.mu.Unlock()
		if promoted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no fallback 2 s after the chain failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	_ = c.Close()
}

// MegaV#355: our Close lands while the fallback is being dialled. Nobody would ever close that conn: Read closes it.
func TestFallbackCloseDuringFallbackDial(t *testing.T) {
	var fb *countingConn
	var c net.Conn
	c = wrapFallback(context.Background(), failingStream(10*time.Millisecond), func() (net.Conn, error) {
		_ = c.Close()
		fb = &countingConn{Conn: silentStream()}
		return fb, nil
	})
	if _, err := c.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 100))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want the chain error from a closed wrapper")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read still blocked 2 s after the chain failed")
	}
	if fb == nil || fb.closed.Load() != 1 {
		t.Fatal("the fallback conn dialled after Close was not closed")
	}
}

// MegaV#355: closing the wrapper before its first successful Read unblocks that Read with an error. It is our own
// close, not a chain failure: no re-dial through the fallback (that conn would have nobody to close it).
func TestFallbackCloseDoesNotRedial(t *testing.T) {
	var calls atomic.Int32
	c := wrapFallback(context.Background(), silentStream(), func() (net.Conn, error) {
		calls.Add(1)
		return silentStream(), nil
	})
	if _, err := c.Write(make([]byte, 1200)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 2048))
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	_ = c.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want the Read to fail after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read still blocked 2 s after Close")
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("fallback dialled %d times after Close, want 0", n)
	}
}
