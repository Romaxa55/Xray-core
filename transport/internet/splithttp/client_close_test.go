package splithttp

import (
	"context"
	gotls "crypto/tls"
	"crypto/x509"
	"io"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

type closeTrackingConn struct {
	gonet.Conn
	closed atomic.Bool
}

func (c *closeTrackingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// newTrackedH2Client starts an h2 test server and returns a client whose
// connections report through dialed whether they were closed.
func newTrackedH2Client(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *DefaultDialerClient, chan *closeTrackingConn) {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	dialed := make(chan *closeTrackingConn, 4)
	transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, _ *gotls.Config) (gonet.Conn, error) {
			d := gotls.Dialer{Config: &gotls.Config{RootCAs: roots, NextProtos: []string{"h2"}}}
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			tracked := &closeTrackingConn{Conn: conn}
			dialed <- tracked
			return tracked, nil
		},
		IdleConnTimeout: time.Hour,
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := &DefaultDialerClient{transportConfig: &Config{}, client: &http.Client{Transport: transport}}
	return srv, client, dialed
}

// Close shuts an h2 connection that carries no requests instead of leaving it
// open with its read loop until the transport's IdleConnTimeout, and leaves a
// connection with a running request alone. Closing a response body removes its
// stream before it returns, so each check right after Close is exact.
func TestDefaultDialerClientCloseH2(t *testing.T) {
	release := make(chan struct{})
	srv, client, dialed := newTrackedH2Client(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-release
		}
		_, _ = io.WriteString(w, "ok")
	})
	get := func(path string) *http.Response {
		t.Helper()
		resp, err := client.client.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := get("/")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	idle := <-dialed
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if !idle.closed.Load() {
		t.Fatal("Close left an h2 connection with no requests open")
	}

	resp = get("/hold")
	held := <-dialed
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if held.closed.Load() {
		t.Fatal("Close shut an h2 connection with a running request")
	}
	close(release)
	if b, err := io.ReadAll(resp.Body); err != nil || string(b) != "ok" {
		t.Fatalf("running request after Close: body %q, err %v", b, err)
	}
	resp.Body.Close()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if !held.closed.Load() {
		t.Fatal("Close left the h2 connection open after its request finished")
	}
}

// A client XMUX retires while a stream still runs on it (hMaxRequestTimes,
// hMaxReusableSecs) is closed by that stream's conn.Close: the conn closes its
// streams before it reports done, so the client finds its h2 connection idle.
func TestSplitConnCloseRetiredClient(t *testing.T) {
	srv, client, dialed := newTrackedH2Client(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "x")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	xmuxClient := &XmuxClient{XmuxConn: client}
	xmuxClient.AddRunning()
	reader, _, _, err := client.OpenStream(context.Background(), srv.URL+"/", "session", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(reader, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	held := <-dialed
	_, writer := io.Pipe()
	conn := &splitConn{writer: writer, reader: reader, onClose: xmuxClient.DoneRunning}

	xmuxClient.NotUsed.Store(true) // retired as GetXmuxClient does
	xmuxClient.maybeClose()
	if held.closed.Load() {
		t.Fatal("XMUX closed a retired client's connection with a running stream")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if !held.closed.Load() {
		t.Fatal("conn.Close left the retired client's h2 connection open")
	}
}
