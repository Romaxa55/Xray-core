package internet

import (
	"reflect"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
)

// DialedPacketConn turns what DialSystem or FinalMask.DialUDP returned for a UDP dest into the PacketConn a UDP
// transport runs on (the finalmask dialer of mKCP and udp, hysteria, XHTTP over HTTP/3, masque, WireGuard): the UDP
// socket itself, or, with a dialerProxy (viaProxy), datagrams over its stream (cnc.Connection, or the fallback
// wrapper of dialer_fallback.go around it, which hides that type). Anything else is an error, not the upstream
// panic(reflect.TypeOf(c)): a panic there kills the whole process, the app on Windows, where the core runs inside
// it, and the VPN service on Android (MegaV#355, MegaV#360).
func DialedPacketConn(conn net.Conn, viaProxy bool) (net.PacketConn, net.Addr, error) {
	if conn == nil {
		return nil, nil, errors.New("no connection to dest")
	}
	if w, ok := conn.(*net.PacketConnWrapper); ok {
		return w.PacketConn, w.RemoteAddr(), nil
	}
	// A stream without a dialerProxy would be a plain TCP conn from a custom system dialer: no UDP transport runs on it.
	if !viaProxy {
		return nil, nil, errors.New("not a UDP socket: ", reflect.TypeOf(conn))
	}
	// FakePacketConn reads the remote address as *net.TCPAddr on every ReadFrom: check it here.
	if remote, ok := conn.RemoteAddr().(*net.TCPAddr); !ok || remote == nil {
		return nil, nil, errors.New("unsupported connection for a UDP transport: ", reflect.TypeOf(conn))
	}
	return &FakePacketConn{Conn: conn}, &net.UDPAddr{IP: []byte{0, 0, 0, 0}}, nil
}

// closeOnError closes what a dial returned next to an error.
func closeOnError(conn net.Conn) {
	if conn != nil {
		conn.Close()
	}
}

// DialedPacketConnOrClose is DialedPacketConn that closes conn when it cannot be used.
func DialedPacketConnOrClose(conn net.Conn, viaProxy bool) (net.PacketConn, net.Addr, error) {
	pc, addr, err := DialedPacketConn(conn, viaProxy)
	if err != nil {
		closeOnError(conn)
	}
	return pc, addr, err
}
