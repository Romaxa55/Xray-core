package burst_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/burst"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

// A core instance that closed must send no more probes: the round its
// scheduler laid out over the interval would otherwise ping through the
// instance's closed outbounds and hold the instance from the GC until its last
// probe fired — an app that restarts the core in its process every couple of
// minutes kept two or three dead instances alive this way.
func TestStopSchedulerCancelsScheduledProbes(t *testing.T) {
	var dials atomic.Int32
	saved := tagged.Dialer
	tagged.Dialer = func(context.Context, routing.Dispatcher, net.Destination, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("outbound is closed")
	}
	t.Cleanup(func() { tagged.Dialer = saved })

	hp := burst.NewHealthPing(context.Background(), nil, &burst.HealthPingConfig{
		Destination: "http://probe.test/generate_204",
		Timeout:     int64(time.Second),
	})
	// Below the 10 s floor NewHealthPing applies, to keep the test short: the
	// scheduled round spreads its probes over 2 s.
	hp.Settings.Interval = time.Second
	hp.Settings.SamplingCount = 2
	tags := make([]string, 50)
	for i := range tags {
		tags[i] = fmt.Sprintf("exit-%d", i)
	}
	window := hp.Settings.Interval * time.Duration(hp.Settings.SamplingCount)

	hp.StartScheduler(func() ([]string, error) { return tags, nil })
	// The initial round probes every tag at once: wait for it, not for a fixed
	// pause, so a loaded machine does not stop the scheduler before it.
	initial := int32(len(tags))
	for deadline := time.Now().Add(5 * time.Second); dials.Load() < initial; {
		if time.Now().After(deadline) {
			t.Fatalf("initial round: %d dials of %d", dials.Load(), initial)
		}
		time.Sleep(10 * time.Millisecond)
	}
	hp.StopScheduler()
	atStop := dials.Load()
	time.Sleep(window + 500*time.Millisecond)

	// Only a probe whose timer had fired by the stop may still dial; the
	// scheduled round's other probes — about 2 × 50 over the window — must not.
	const inFlight = 10
	if late := dials.Load() - atStop; late > inFlight {
		t.Fatalf("%d probes dialed after StopScheduler, want at most %d already in flight", late, inFlight)
	}
}
