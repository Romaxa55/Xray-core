package grpc

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	c "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/platform"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/grpc/encoding"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	errors.LogInfo(ctx, "creating connection to ", dest)

	conn, err := dialgRPC(ctx, dest, streamSettings)
	if err != nil {
		return nil, errors.New("failed to dial gRPC").Base(err)
	}
	return stat.Connection(conn), nil
}

func init() {
	common.Must(internet.RegisterTransportDialer(protocolName, Dial))
}

type dialerConf struct {
	net.Destination
	*internet.MemoryStreamConfig
}

// grpcClient is a ClientConn shared by the tunnels of one dialer key.
type grpcClient struct {
	*grpc.ClientConn
	// Guarded by globalDialerAccess.
	active   int           // tunnels open on this ClientConn
	lastUsed time.Duration // monoNow() when a tunnel last opened or closed
	// instance is the Xray instance whose stream settings are in the key; nil
	// when the dial context carries none. Set once when the client is created
	// and read without globalDialerAccess.
	instance *core.Instance
}

var (
	globalDialerMap    map[dialerConf]*grpcClient
	globalDialerAccess sync.Mutex
	// dialerJanitorRunning is guarded by globalDialerAccess.
	dialerJanitorRunning bool
)

// The key of globalDialerMap holds the stream settings of one Xray instance, so
// a closed instance leaves its ClientConn behind for good: grpc-go keeps it
// reconnecting (backoff up to 19s) or holding an h2 connection until its own
// 30 min idle mode, and the map entry stays for the life of the process. A
// checker that starts an instance per probe grows by one ClientConn per gRPC
// probe, and an app keeps the previous server's connection after a switch.
// The janitor closes ClientConns with no open tunnels for dialerIdleTimeout
// (env xray.grpc.idle.timeout, seconds) and drops them from the map; it stops
// when the map is empty. An unset, zero or negative value means the default
// net.ConnIdleTimeout; the janitor cannot be turned off. Read on first use, not
// at package init: mobile wrappers set the environment after Go initializes the
// packages. It applies with idle_timeout/permit_without_stream as well: a
// long-lived instance pays one more handshake after dialerIdleTimeout with no
// tunnels, while an instance closed by its owner would otherwise never let go.
//
// The ClientConn of a stopped instance is closed even with tunnels open: a
// tunnel whose proxy blocks reading the first response (a VLESS encryption
// handshake to a server that accepted the stream and stays silent) never
// reaches Close, so the ClientConn would keep it and the h2 connection forever.
// Closing the ClientConn cancels its streams and the blocked read returns.
// Mux and XUDP tunnels dial with a context that carries no instance
// (common/mux/client.go), so for them only the idle rule applies.
var dialerIdleTimeout = sync.OnceValue(func() time.Duration {
	if s := platform.NewEnvFlag(platform.GRPCIdleTimeout).GetValueAsInt(0); s > 0 {
		return time.Duration(s) * time.Second
	}
	return net.ConnIdleTimeout
})

var monoBase = time.Now()

func monoNow() time.Duration {
	return time.Since(monoBase)
}

func dialerSweepInterval() time.Duration {
	return min(max(dialerIdleTimeout()/5, 5*time.Second), time.Minute)
}

func dialerJanitor() {
	ticker := time.NewTicker(dialerSweepInterval())
	defer ticker.Stop()
	for range ticker.C {
		if !sweepIdleDialers(monoNow()) {
			return
		}
	}
}

// stoppedInstanceClients returns the clients whose Xray instance is stopped.
// IsRunning takes the instance's statusLock, and Instance.Close holds it while
// closing handlers whose tunnels take globalDialerAccess on release, so it is
// called without globalDialerAccess held. The janitor waits for every
// instance's Start and Close, as upstream's Hysteria cleanup does: a goroutine
// dump with dialerJanitor blocked in IsRunning means one of them never returned.
func stoppedInstanceClients() map[*grpcClient]bool {
	globalDialerAccess.Lock()
	clients := make([]*grpcClient, 0, len(globalDialerMap))
	for _, client := range globalDialerMap {
		if client.instance != nil {
			clients = append(clients, client)
		}
	}
	globalDialerAccess.Unlock()

	var stopped map[*grpcClient]bool
	for _, client := range clients {
		if !client.instance.IsRunning() {
			if stopped == nil {
				stopped = make(map[*grpcClient]bool)
			}
			stopped[client] = true
		}
	}
	return stopped
}

// sweepIdleDialers reports whether the janitor should keep running.
func sweepIdleDialers(now time.Duration) bool {
	stopped := stoppedInstanceClients()
	var removed []*grpc.ClientConn

	globalDialerAccess.Lock()
	idle := dialerIdleTimeout()
	for key, client := range globalDialerMap {
		if stopped[client] || client.GetState() == connectivity.Shutdown || (client.active <= 0 && now-client.lastUsed > idle) {
			delete(globalDialerMap, key)
			removed = append(removed, client.ClientConn)
		}
	}
	keepRunning := len(globalDialerMap) > 0
	if !keepRunning {
		dialerJanitorRunning = false
	}
	globalDialerAccess.Unlock()

	for _, conn := range removed {
		conn.Close()
	}
	return keepRunning
}

// acquire is called with globalDialerAccess held, so the janitor cannot close
// the ClientConn between getGrpcClient and the tunnel it opens.
func (c *grpcClient) acquire() {
	c.active++
	c.lastUsed = monoNow()
}

func (c *grpcClient) release() {
	globalDialerAccess.Lock()
	c.active--
	c.lastUsed = monoNow()
	globalDialerAccess.Unlock()
}

func dialgRPC(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (net.Conn, error) {
	grpcSettings := streamSettings.ProtocolSettings.(*Config)

	gc, err := getGrpcClient(ctx, dest, streamSettings)
	if err != nil {
		return nil, errors.New("Cannot dial gRPC").Base(err)
	}
	// The tunnel's Close calls release (once): the janitor only closes a
	// ClientConn with no open tunnels.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(gc.release) }
	client := encoding.NewGRPCServiceClient(gc.ClientConn)
	if grpcSettings.MultiMode {
		errors.LogDebug(ctx, "using gRPC multi mode service name: `"+grpcSettings.getServiceName()+"` stream name: `"+grpcSettings.getTunMultiStreamName()+"`")
		grpcService, err := client.(encoding.GRPCServiceClientX).TunMultiCustomName(ctx, grpcSettings.getServiceName(), grpcSettings.getTunMultiStreamName())
		if err != nil {
			release()
			return nil, errors.New("Cannot dial gRPC").Base(err)
		}
		return encoding.NewMultiHunkConn(grpcService, release), nil
	}

	errors.LogDebug(ctx, "using gRPC tun mode service name: `"+grpcSettings.getServiceName()+"` stream name: `"+grpcSettings.getTunStreamName()+"`")
	grpcService, err := client.(encoding.GRPCServiceClientX).TunCustomName(ctx, grpcSettings.getServiceName(), grpcSettings.getTunStreamName())
	if err != nil {
		release()
		return nil, errors.New("Cannot dial gRPC").Base(err)
	}

	return encoding.NewHunkConn(grpcService, release), nil
}

func getGrpcClient(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (*grpcClient, error) {
	globalDialerAccess.Lock()
	defer globalDialerAccess.Unlock()

	if globalDialerMap == nil {
		globalDialerMap = make(map[dialerConf]*grpcClient)
	}
	tlsConfig := tls.ConfigFromStreamSettings(streamSettings)
	realityConfig := reality.ConfigFromStreamSettings(streamSettings)
	sockopt := streamSettings.SocketSettings
	grpcSettings := streamSettings.ProtocolSettings.(*Config)

	if client, found := globalDialerMap[dialerConf{dest, streamSettings}]; found && client.GetState() != connectivity.Shutdown {
		client.acquire()
		return client, nil
	}

	dialOptions := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  500 * time.Millisecond,
				Multiplier: 1.5,
				Jitter:     0.2,
				MaxDelay:   19 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithContextDialer(func(gctx context.Context, s string) (net.Conn, error) {
			select {
			case <-gctx.Done():
				return nil, gctx.Err()
			default:
			}

			rawHost, rawPort, err := net.SplitHostPort(s)
			if err != nil {
				return nil, err
			}
			if len(rawPort) == 0 {
				rawPort = "443"
			}
			port, err := net.PortFromString(rawPort)
			if err != nil {
				return nil, err
			}
			address := net.ParseAddress(rawHost)

			gctx = c.ContextWithID(gctx, c.IDFromContext(ctx))
			gctx = session.ContextWithOutbounds(gctx, session.OutboundsFromContext(ctx))
			gctx = session.ContextWithTimeoutOnly(gctx, true)

			c, err := internet.DialSystem(gctx, net.TCPDestination(address, port), sockopt)
			if err == nil {
				if streamSettings.TcpmaskManager != nil {
					newConn, err := streamSettings.TcpmaskManager.WrapConnClient(c)
					if err != nil {
						c.Close()
						return nil, errors.New("mask err").Base(err)
					}
					c = newConn
				}

				if tlsConfig != nil {
					config := tlsConfig.GetTLSConfig()
					if config.ServerName == "" && address.Family().IsDomain() {
						config.ServerName = address.Domain()
					}
					if fingerprint := tls.GetFingerprint(tlsConfig.Fingerprint); fingerprint != nil {
						return tls.UClient(c, config, fingerprint), nil
					} else { // Fallback to normal gRPC TLS
						return tls.Client(c, config), nil
					}
				}
				if realityConfig != nil {
					return reality.UClient(c, realityConfig, gctx, dest)
				}
			}
			return c, err
		}),
	}

	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

	authority := ""
	if grpcSettings.Authority != "" {
		authority = grpcSettings.Authority
	} else if tlsConfig != nil && tlsConfig.ServerName != "" {
		authority = tlsConfig.ServerName
	} else if realityConfig == nil && dest.Address.Family().IsDomain() {
		authority = dest.Address.Domain()
	}
	dialOptions = append(dialOptions, grpc.WithAuthority(authority))

	if grpcSettings.IdleTimeout > 0 || grpcSettings.HealthCheckTimeout > 0 || grpcSettings.PermitWithoutStream {
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * time.Duration(grpcSettings.IdleTimeout),
			Timeout:             time.Second * time.Duration(grpcSettings.HealthCheckTimeout),
			PermitWithoutStream: grpcSettings.PermitWithoutStream,
		}))
	}

	if grpcSettings.InitialWindowsSize > 0 {
		dialOptions = append(dialOptions, grpc.WithInitialWindowSize(grpcSettings.InitialWindowsSize))
	}

	var grpcDestHost string
	if dest.Address.Family().IsDomain() {
		grpcDestHost = dest.Address.Domain()
	} else {
		grpcDestHost = dest.Address.IP().String()
	}

	conn, err := grpc.NewClient(
		"passthrough:///"+net.JoinHostPort(grpcDestHost, dest.Port.String()),
		dialOptions...,
	)
	if err == nil {
		userAgent := grpcSettings.UserAgent
		// It's NOT recommended to set the UA of gRPC connections to that of real browsers, as they are fundamentally incapable of initiating real gRPC connections.
		switch userAgent {
		case "chrome", "":
			userAgent = utils.ChromeUA
		case "firefox":
			userAgent = utils.FirefoxUA
		case "edge":
			userAgent = utils.MSEdgeUA
		case "golang":
			userAgent = ""
		}
		setUserAgent(conn, userAgent)
		conn.Connect()
	}
	if err != nil {
		return nil, err
	}
	client := &grpcClient{ClientConn: conn, instance: core.FromContext(ctx)}
	client.acquire()
	globalDialerMap[dialerConf{dest, streamSettings}] = client
	if !dialerJanitorRunning {
		dialerJanitorRunning = true
		go dialerJanitor()
	}
	return client, nil
}

// setUserAgent overrides the user-agent on a ClientConn to remove the
// "grpc-go/version" suffix that grpc.WithUserAgent unconditionally appends.
func setUserAgent(conn *grpc.ClientConn, ua string) {
	if f := reflect.ValueOf(conn).Elem().FieldByName("dopts").FieldByName("copts").FieldByName("UserAgent"); f.IsValid() {
		*(*string)(f.Addr().UnsafePointer()) = ua
	}
}
