package udphop

import (
	"net"
	"runtime"
	"testing"
	"time"
)

// A read deadline in the past with nobody reading fills recvQueue with timeout
// errors. Close must still stop recvLoop instead of leaving it blocked on the send.
func TestRecvLoopExitsOnCloseWithFullQueue(t *testing.T) {
	listen := func(addr *net.UDPAddr) (net.PacketConn, error) {
		return net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	}
	first, err := listen(nil)
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()

	remote := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	c := NewUDPHopPacketConn([]net.Addr{remote}, time.Minute, time.Minute, listen, first, 0).(*UdpHopPacketConn)
	if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(c.recvQueue) < cap(c.recvQueue) {
		if time.Now().After(deadline) {
			t.Fatalf("recvQueue did not fill: %d/%d", len(c.recvQueue), cap(c.recvQueue))
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutines did not exit: %d > %d\n%s", runtime.NumGoroutine(), before,
				buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
