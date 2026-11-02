package websocket_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	. "github.com/xtls/xray-core/transport/internet/websocket"
)

// A server that answers and closes the connection sends a close frame with
// the normal status (1000) right after the data: the client reads the data,
// then io.EOF — the end of the stream, as a FIN is — never an error. An error
// there made the outbound interrupt the link and drop the answer the reader
// had not taken yet (an observatory probe saw "io: read/write on closed pipe").
func TestNormalCloseIsEOF(t *testing.T) {
	listenPort := tcp.PickPort()
	listen, err := ListenWS(context.Background(), net.LocalHostIP, listenPort, &internet.MemoryStreamConfig{
		ProtocolName:     "websocket",
		ProtocolSettings: &Config{Path: "ws"},
	}, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			var b [1024]byte
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := c.Read(b[:]); err != nil {
				return
			}
			common.Must2(c.Write([]byte("Response")))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(), net.TCPDestination(net.DomainAddress("localhost"), listenPort), &internet.MemoryStreamConfig{
		ProtocolName:     "websocket",
		ProtocolSettings: &Config{Path: "ws"},
	})
	common.Must(err)
	defer conn.Close()
	common.Must2(conn.Write([]byte("Request")))

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read after the server's normal close: %v", err)
	}
	if string(got) != "Response" {
		t.Fatalf("response: %q", got)
	}
}

// After the server's normal close the client has replied with its own close
// frame, so a write fails: with io.EOF, the end of the stream, never an error
// ("websocket: close sent") that interrupts the link as above.
func TestWriteAfterNormalCloseIsEOF(t *testing.T) {
	listenPort := tcp.PickPort()
	listen, err := ListenWS(context.Background(), net.LocalHostIP, listenPort, &internet.MemoryStreamConfig{
		ProtocolName:     "websocket",
		ProtocolSettings: &Config{Path: "ws"},
	}, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			common.Must2(c.Write([]byte("Response")))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(), net.TCPDestination(net.DomainAddress("localhost"), listenPort), &internet.MemoryStreamConfig{
		ProtocolName:     "websocket",
		ProtocolSettings: &Config{Path: "ws"},
	})
	common.Must(err)
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("read after the server's normal close: %v", err)
	}
	if _, err := conn.Write([]byte("More")); err != io.EOF {
		t.Fatalf("write after the close: %v, want io.EOF", err)
	}
}
