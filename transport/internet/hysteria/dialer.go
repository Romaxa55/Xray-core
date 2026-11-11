package hysteria

import (
	"context"
	go_tls "crypto/tls"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/hysteria/udphop"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type client struct {
	lock           chan struct{}
	instance       *core.Instance
	forced         bool
	dest           net.Destination
	config         *Config
	tlsConfig      *go_tls.Config
	socketConfig   *internet.SocketConfig
	udpmaskManager *finalmask.UdpmaskManager
	quicParams     *internet.QuicParams

	conn    *quic.Conn
	tr      *quic.Transport
	pktConn net.PacketConn
	udpSM   *udpSessionManager
}

func (c *client) status() status {
	if c.conn == nil {
		return StatusNull
	}
	select {
	case <-c.conn.Context().Done():
		return StatusInactive
	default:
		return StatusActive
	}
}

// closeQUIC closes a client's QUIC objects in the one order that works over a dialerProxy stream (MegaV#355): the
// PacketConn before the Transport. Transport.Close waits for its read loop to exit and only sets a read deadline to
// get there, which a cnc.Connection ignores, so the other order hangs until the proxy outbound drops the stream.
func closeQUIC(conn *quic.Conn, code quic.ApplicationErrorCode, pktConn net.PacketConn, tr *quic.Transport) {
	if conn != nil {
		_ = conn.CloseWithError(code, "")
	}
	if pktConn != nil {
		_ = pktConn.Close()
	}
	if tr != nil {
		_ = tr.Close()
	}
}

func (c *client) close() {
	closeQUIC(c.conn, closeErrCodeOK, c.pktConn, c.tr)
	c.conn = nil
	c.tr = nil
	c.pktConn = nil
	c.udpSM = nil
}

func (c *client) dial(ctx context.Context) error {
	if c.forced {
		return errors.New("client is closed")
	}

	switch c.status() {
	case StatusActive:
		return nil
	case StatusInactive:
		c.close()
	}

	quicParams := c.quicParams
	if quicParams == nil {
		quicParams = &internet.QuicParams{
			BbrProfile: string(bbr.ProfileStandard),
			UdpHop:     &internet.UdpHop{},
		}
	}

	quicConfig := &quic.Config{
		InitialStreamReceiveWindow:     quicParams.InitStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicParams.MaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicParams.InitConnReceiveWindow,
		MaxConnectionReceiveWindow:     quicParams.MaxConnReceiveWindow,
		MaxIdleTimeout:                 time.Duration(quicParams.MaxIdleTimeout) * time.Second,
		KeepAlivePeriod:                time.Duration(quicParams.KeepAlivePeriod) * time.Second,
		DisablePathMTUDiscovery:        quicParams.DisablePathMtuDiscovery || (runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin"),
		EnableDatagrams:                true,
		MaxDatagramFrameSize:           MaxDatagramFrameSize,
		// MegaV fork: upstream flips this to true after 2026-09-01, silently
		// changing the QUIC datagram frame behaviour for hysteria. Pin to the
		// pre-deadline value so behaviour never changes under us on a date.
		OmitMaxDatagramFrameSize: false,
		DisablePathManager:       true,
	}
	if quicParams.InitStreamReceiveWindow == 0 {
		quicConfig.InitialStreamReceiveWindow = 8388608
	}
	if quicParams.MaxStreamReceiveWindow == 0 {
		quicConfig.MaxStreamReceiveWindow = 8388608
	}
	if quicParams.InitConnReceiveWindow == 0 {
		quicConfig.InitialConnectionReceiveWindow = 8388608 * 5 / 2
	}
	if quicParams.MaxConnReceiveWindow == 0 {
		quicConfig.MaxConnectionReceiveWindow = 8388608 * 5 / 2
	}
	if quicParams.MaxIdleTimeout == 0 {
		quicConfig.MaxIdleTimeout = 30 * time.Second
	}
	// if quicParams.KeepAlivePeriod == 0 {
	// 	quicConfig.KeepAlivePeriod = 10 * time.Second
	// }

	udpHopDialer := func(addr *net.UDPAddr) (net.PacketConn, error) {
		conn, err := internet.DialSystem(ctx, net.UDPDestination(net.IPAddress(addr.IP), net.Port(addr.Port)), c.socketConfig)
		if err != nil {
			errors.LogInfoInner(context.Background(), err, "skip hop: failed to dial to dest")
			return nil, errors.New("")
		}
		pktConn, err := internet.UDPHopSocket(conn)
		if err != nil {
			errors.LogInfoInner(context.Background(), err, "skip hop")
			return nil, errors.New("")
		}
		return pktConn, nil
	}

	var pktConn net.PacketConn
	var udpAddr *net.UDPAddr
	viaProxy := len(c.socketConfig.GetDialerProxy()) > 0
	if len(quicParams.GetUdpHop().GetPorts()) > 0 { // nil UdpHop: a QuicParams not built from JSON
		index := rand.Intn(len(quicParams.UdpHop.Ports))
		c.dest.Port = net.Port(quicParams.UdpHop.Ports[index])
		conn, err := internet.DialSystem(ctx, c.dest, c.socketConfig)
		if err != nil {
			return errors.New("failed to dial to dest").Base(err)
		}
		var socket bool
		if pktConn, udpAddr, socket, err = internet.DialedPacketConn(conn, viaProxy); err != nil {
			if conn != nil {
				conn.Close()
			}
			return err
		}
		if socket {
			pktConn = udphop.NewUDPHopPacketConn(udphop.ToAddrs(udpAddr.IP, quicParams.UdpHop.Ports), time.Duration(quicParams.UdpHop.IntervalMin)*time.Second, time.Duration(quicParams.UdpHop.IntervalMax)*time.Second, udpHopDialer, pktConn, index)
		} else {
			// Through dialerProxy there is one stream to the proxy, nothing to hop between: stay on the chosen port.
			errors.LogInfo(ctx, "port hopping is off through dialerProxy, staying on port ", c.dest.Port)
		}
	} else {
		conn, err := internet.DialSystem(ctx, c.dest, c.socketConfig)
		if err != nil {
			return errors.New("failed to dial to dest").Base(err)
		}
		if pktConn, udpAddr, _, err = internet.DialedPacketConn(conn, viaProxy); err != nil {
			if conn != nil {
				conn.Close()
			}
			return err
		}
	}

	if c.udpmaskManager != nil {
		newConn, err := c.udpmaskManager.WrapPacketConnClient(pktConn)
		if err != nil {
			pktConn.Close()
			return errors.New("mask err").Base(err)
		}
		pktConn = newConn
	}

	tr := &quic.Transport{Conn: pktConn}

	var conn *quic.Conn
	rt := &http3.Transport{
		TLSClientConfig: c.tlsConfig,
		QUICConfig:      quicConfig,
		Dial: func(ctx context.Context, _ string, tlsCfg *go_tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			qc, err := tr.DialEarly(ctx, udpAddr, tlsCfg, cfg)
			if err != nil {
				return nil, err
			}
			conn = qc
			return qc, nil
		},
	}
	req := &http.Request{
		Method: http.MethodPost,
		URL: &url.URL{
			Scheme: "https",
			Host:   URLHost,
			Path:   URLPath,
		},
		Header: http.Header{
			RequestHeaderAuth:   []string{c.config.Auth},
			CommonHeaderCCRX:    []string{strconv.FormatUint(quicParams.BrutalDown, 10)},
			CommonHeaderPadding: []string{AuthRequestPadding.String()},
		},
	}
	resp, err := rt.RoundTrip(req.WithContext(ctx))
	if err != nil {
		_ = rt.Close()
		closeQUIC(conn, closeErrCodeProtocolError, pktConn, tr)
		return err
	}
	_ = resp.Body.Close()

	if resp.StatusCode != StatusAuthOK {
		_ = rt.Close()
		closeQUIC(conn, closeErrCodeProtocolError, pktConn, tr)
		return errors.New("auth failed code ", resp.StatusCode)
	}

	// udp, _ := strconv.ParseBool(resp.Header.Get(ResponseHeaderUDPEnabled))
	down, _ := strconv.ParseUint(resp.Header.Get(CommonHeaderCCRX), 10, 64)

	switch quicParams.Congestion {
	case "reno":
	case "bbr":
		congestion.UseBBR(conn, bbr.Profile(quicParams.BbrProfile))
	case "", "brutal":
		if quicParams.BrutalUp == 0 || down == 0 {
			congestion.UseBBR(conn, bbr.Profile(quicParams.BbrProfile))
		} else {
			congestion.UseBrutal(conn, min(quicParams.BrutalUp, down))
		}
	case "force-brutal":
		congestion.UseBrutal(conn, quicParams.BrutalUp)
	default:
		panic(quicParams.Congestion)
	}

	c.pktConn = pktConn
	c.tr = tr
	c.conn = conn
	c.udpSM = &udpSessionManager{
		conn: conn,
		m:    make(map[uint32]*InterConn),
		next: 1,
	}
	go c.udpSM.run()

	return nil
}

func (c *client) unlock() {
	<-c.lock
}

func (c *client) tcp(ctx context.Context) (stat.Connection, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.lock <- struct{}{}:
		defer c.unlock()
	}

	err := c.dial(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := c.conn.OpenStream()
	if err != nil {
		return nil, err
	}

	return &interConn{
		stream: stream,
		local:  c.conn.LocalAddr(),
		remote: c.conn.RemoteAddr(),

		client: true,
		lock:   make(chan struct{}, 1),
	}, nil
}

func (c *client) udp(ctx context.Context) (stat.Connection, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.lock <- struct{}{}:
		defer c.unlock()
	}

	err := c.dial(ctx)
	if err != nil {
		return nil, err
	}

	return c.udpSM.udp()
}

func (c *client) clean(force bool) {
	c.lock <- struct{}{}
	if force {
		c.forced = true
	}
	if status := c.status(); force && status != StatusNull || status == StatusInactive {
		c.close()
	}
	c.unlock()
}

type dialerConf struct {
	net.Destination
	*internet.MemoryStreamConfig
}

type clientManager struct {
	sync.RWMutex
	m map[dialerConf]*client
}

func (m *clientManager) clean() {
	ticker := time.NewTicker(idleCleanupInterval)
	for range ticker.C {
		m.RLock()
		entries := make(map[dialerConf]*client, len(m.m))
		for k, c := range m.m {
			entries[k] = c
		}
		m.RUnlock()
		for k, c := range entries {
			force := c.instance != nil && !c.instance.IsRunning()
			c.clean(force)
			if force {
				m.Lock()
				delete(m.m, k)
				m.Unlock()
			}
		}
	}
}

var manager *clientManager
var initmanager sync.Once

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	tlsConfig := tls.ConfigFromStreamSettings(streamSettings)
	if tlsConfig == nil {
		return nil, errors.New("tls config is nil")
	}

	datagram := DatagramFromContext(ctx)
	dest.Network = net.Network_UDP

	initmanager.Do(func() {
		manager = &clientManager{
			m: make(map[dialerConf]*client),
		}
		go manager.clean()
	})

	dialerConfKey := dialerConf{dest, streamSettings}

	manager.RLock()
	c := manager.m[dialerConfKey]
	manager.RUnlock()

	if c == nil {
		manager.Lock()
		c = manager.m[dialerConfKey]
		if c == nil {
			c = &client{
				lock:           make(chan struct{}, 1),
				instance:       core.FromContext(ctx),
				dest:           dest,
				config:         streamSettings.ProtocolSettings.(*Config),
				tlsConfig:      tlsConfig.GetTLSConfig(),
				socketConfig:   streamSettings.SocketSettings,
				udpmaskManager: streamSettings.UdpmaskManager,
				quicParams:     streamSettings.QuicParams,
			}
			manager.m[dialerConfKey] = c
		}
		manager.Unlock()
	}

	if datagram {
		return c.udp(ctx)
	}
	return c.tcp(ctx)
}

func init() {
	common.Must(internet.RegisterTransportDialer(protocolName, Dial))
}
