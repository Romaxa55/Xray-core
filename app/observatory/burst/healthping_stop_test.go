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
	time.Sleep(100 * time.Millisecond)
	hp.StopScheduler()
	time.Sleep(window + 500*time.Millisecond)

	// The initial round probes every tag at once; of the scheduled round's
	// 2 × 50 probes only those due in the first 100 ms may go out.
	initial, scheduled := int32(len(tags)), int32(len(tags)*hp.Settings.SamplingCount)
	if got := dials.Load(); got < initial || got >= initial+scheduled/2 {
		t.Fatalf("dials = %d, want the %d initial probes and no scheduled ones after the stop", got, initial)
	}
}
