package splithttp

import (
	"context"
	"crypto/rand"
	"math"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
)

type XmuxConn interface {
	IsClosed() bool
}

type XmuxClient struct {
	XmuxConn     XmuxConn
	Running      atomic.Int32
	leftUsage    int32
	LeftRequests atomic.Int32
	UnreusableAt time.Time
	NotUsed      atomic.Bool
	lastUsed     atomic.Int64 // monoNow() at the last use
	// holds keeps the client open for a user that Running does not count (an
	// upload rotated onto it, a request in flight) without affecting the
	// choice of a client by maxConcurrency.
	holds atomic.Int32
}

var monoBase = time.Now()

// monoNow reads the monotonic clock, so a wall clock change does not make idle
// clients look fresh or stale.
func monoNow() time.Duration {
	return time.Since(monoBase)
}

func (c *XmuxClient) touch() {
	c.lastUsed.Store(int64(monoNow()))
}

func (c *XmuxClient) AddRunning() {
	c.Running.Add(1)
	c.touch()
}

func (c *XmuxClient) DoneRunning() {
	c.touch()
	c.Running.Add(-1)
	c.maybeClose()
}

func (c *XmuxClient) hold() {
	c.holds.Add(1)
	c.touch()
}

func (c *XmuxClient) release() {
	c.touch()
	c.holds.Add(-1)
	c.maybeClose()
}

func (c *XmuxClient) busy() bool {
	return c.Running.Load() > 0 || c.holds.Load() > 0
}

// close the XmuxConn if it is not used and has no running requests or holds
func (c *XmuxClient) maybeClose() {
	if c.NotUsed.Load() && !c.busy() {
		common.Close(c.XmuxConn)
	}
}

type XmuxManager struct {
	xmuxConfig  XmuxConfig
	concurrency int32
	connections int32
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
}

func NewXmuxManager(xmuxConfig XmuxConfig, newConnFunc func() XmuxConn) *XmuxManager {
	return &XmuxManager{
		xmuxConfig:  xmuxConfig,
		concurrency: xmuxConfig.GetNormalizedMaxConcurrency().rand(),
		connections: xmuxConfig.GetNormalizedMaxConnections().rand(),
		newConnFunc: newConnFunc,
		xmuxClients: make([]*XmuxClient, 0),
	}
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	xmuxClient := &XmuxClient{
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
	}
	if x := m.xmuxConfig.GetNormalizedCMaxReuseTimes().rand(); x > 0 {
		xmuxClient.leftUsage = x - 1
	}
	xmuxClient.LeftRequests.Store(math.MaxInt32)
	if x := m.xmuxConfig.GetNormalizedHMaxRequestTimes().rand(); x > 0 {
		xmuxClient.LeftRequests.Store(x)
	}
	if x := m.xmuxConfig.GetNormalizedHMaxReusableSecs().rand(); x > 0 {
		xmuxClient.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	xmuxClient.touch()
	m.xmuxClients = append(m.xmuxClients, xmuxClient)
	return xmuxClient
}

// removeIdle takes out the clients that are closed or have had no running
// requests for longer than idle (now and idle on the monoNow clock), marks them
// unused and appends them to removed; the caller closes them with maybeClose
// after it releases the lock that guards GetXmuxClient, which it holds here,
// because closing a QUIC connection can block. It reports whether the manager
// has no clients left.
func (m *XmuxManager) removeIdle(now, idle time.Duration, removed []*XmuxClient) ([]*XmuxClient, bool) {
	kept := m.xmuxClients[:0]
	for _, xmuxClient := range m.xmuxClients {
		if xmuxClient.XmuxConn.IsClosed() ||
			(!xmuxClient.busy() && now-time.Duration(xmuxClient.lastUsed.Load()) > idle) {
			xmuxClient.NotUsed.Store(true)
			removed = append(removed, xmuxClient)
			continue
		}
		kept = append(kept, xmuxClient)
	}
	clear(m.xmuxClients[len(kept):])
	m.xmuxClients = kept
	return removed, len(kept) == 0
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) *XmuxClient { // when locking
	for i := 0; i < len(m.xmuxClients); {
		xmuxClient := m.xmuxClients[i]
		if xmuxClient.XmuxConn.IsClosed() ||
			xmuxClient.leftUsage == 0 ||
			xmuxClient.LeftRequests.Load() <= 0 ||
			(xmuxClient.UnreusableAt != time.Time{} && time.Now().After(xmuxClient.UnreusableAt)) {
			errors.LogDebug(ctx, "XMUX: removing xmuxClient, IsClosed() = ", xmuxClient.XmuxConn.IsClosed(),
				", Running = ", xmuxClient.Running.Load(),
				", holds = ", xmuxClient.holds.Load(),
				", leftUsage = ", xmuxClient.leftUsage,
				", LeftRequests = ", xmuxClient.LeftRequests.Load(),
				", UnreusableAt = ", xmuxClient.UnreusableAt)
			xmuxClient.NotUsed.Store(true)
			xmuxClient.maybeClose()
			m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
		} else {
			i++
		}
	}

	if len(m.xmuxClients) == 0 {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because xmuxClients is empty")
		return m.newXmuxClient()
	}

	if m.connections > 0 && len(m.xmuxClients) < int(m.connections) {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because maxConnections was not hit, xmuxClients = ", len(m.xmuxClients))
		return m.newXmuxClient()
	}

	xmuxClients := make([]*XmuxClient, 0)
	if m.concurrency > 0 {
		for _, xmuxClient := range m.xmuxClients {
			if xmuxClient.Running.Load() < m.concurrency {
				xmuxClients = append(xmuxClients, xmuxClient)
			}
		}
	} else {
		xmuxClients = m.xmuxClients
	}

	if len(xmuxClients) == 0 {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because maxConcurrency was hit, xmuxClients = ", len(m.xmuxClients))
		return m.newXmuxClient()
	}

	i, _ := rand.Int(rand.Reader, big.NewInt(int64(len(xmuxClients))))
	xmuxClient := xmuxClients[i.Int64()]
	if xmuxClient.leftUsage > 0 {
		xmuxClient.leftUsage -= 1
	}
	xmuxClient.touch()
	return xmuxClient
}
