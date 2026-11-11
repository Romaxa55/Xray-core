package internet

import (
	"reflect"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
)

// DialedPacketConn turns what DialSystem returned for a UDP dest into the PacketConn a UDP transport runs on
// (hysteria, XHTTP over HTTP/3, mKCP and udp with a udpmask): the UDP socket itself (socket=true), or, with a
// dialerProxy (viaProxy), datagrams over its stream (cnc.Connection, or the fallback wrapper of dialer_fallback.go
// around it, which hides that type). Anything else is an error, not a panic: a panic there kills the whole
// process — the app on Windows, where the core runs inside it, the VPN service on Android (MegaV#355, MegaV#360).
func DialedPacketConn(conn net.Conn, viaProxy bool) (pktConn net.PacketConn, udpAddr *net.UDPAddr, socket bool, err error) {
	if conn == nil {
		return nil, nil, false, errors.New("no connection to dest")
	}
	if w, ok := conn.(*PacketConnWrapper); ok {
		if addr, ok := w.RemoteAddr().(*net.UDPAddr); ok && addr != nil {
			return w.PacketConn, addr, true, nil
		}
		return nil, nil, false, errors.New("unexpected remote address of a UDP socket: ", reflect.TypeOf(w.RemoteAddr()))
	}
	// A stream without a dialerProxy would be a plain TCP conn from a custom system dialer: no UDP transport runs on it.
	if !viaProxy {
		return nil, nil, false, errors.New("not a UDP socket: ", reflect.TypeOf(conn))
	}
	// FakePacketConn reads both addresses as *net.TCPAddr: check them here, not in its ReadFrom.
	remote, ok := conn.RemoteAddr().(*net.TCPAddr)
	local, lok := conn.LocalAddr().(*net.TCPAddr)
	if !ok || !lok || remote == nil || local == nil {
		return nil, nil, false, errors.New("unsupported connection for a UDP transport: ", reflect.TypeOf(conn))
	}
	return &FakePacketConn{Conn: conn}, &net.UDPAddr{IP: remote.IP, Port: remote.Port}, false, nil
}

// UDPHopSocket is DialedPacketConn for a port-hopping dialer: one more port is useful only as a UDP socket, so
// anything else is closed and skipped (nil, error) instead of a panic (MegaV#355, MegaV#360).
func UDPHopSocket(conn net.Conn) (net.PacketConn, error) {
	if w, ok := conn.(*PacketConnWrapper); ok {
		return w.PacketConn, nil
	}
	if conn != nil {
		conn.Close()
	}
	return nil, errors.New("not a UDP socket: ", reflect.TypeOf(conn))
}
