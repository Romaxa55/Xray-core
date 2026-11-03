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

// Close shuts an h2 connection that carries no requests instead of leaving it
// open with its read loop until the transport's IdleConnTimeout, and leaves a
// connection with a running request alone.
func TestDefaultDialerClientCloseH2(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-release
		}
		_, _ = io.WriteString(w, "ok")
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

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
	defer transport.CloseIdleConnections()
	client := &DefaultDialerClient{client: &http.Client{Transport: transport}}

	get := func(path string) *http.Response {
		t.Helper()
		resp, err := client.client.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	// The stream leaves the connection asynchronously after the body is read.
	closedByClose := func(conn *closeTrackingConn) bool {
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if conn.closed.Load() {
				return true
			}
		}
		return false
	}

	resp := get("/")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	idle := <-dialed
	if !closedByClose(idle) {
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
	if !closedByClose(held) {
		t.Fatal("Close left the h2 connection open after its request finished")
	}
}
