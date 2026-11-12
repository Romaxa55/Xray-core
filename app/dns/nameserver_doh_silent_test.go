package dns_test

import (
	"context"
	"io"
	gonet "net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/xtls/xray-core/app/dns"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	dns_feature "github.com/xtls/xray-core/features/dns"
	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// dohStub answers every A question with 192.0.2.1 over h2c; a name that
// starts with "slow." is answered only after its request is gone, and its
// arrival is told on the channel.
func dohStub(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	slowArrived := make(chan struct{}, 8)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		var msg dnsmessage.Message
		if err != nil || msg.Unpack(body) != nil || len(msg.Questions) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q := msg.Questions[0]
		if strings.HasPrefix(q.Name.String(), "slow.") {
			slowArrived <- struct{}{}
			<-r.Context().Done()
			return
		}
		resp := dnsmessage.Message{
			Header:    dnsmessage.Header{ID: msg.ID, Response: true},
			Questions: msg.Questions,
		}
		if q.Type == dnsmessage.TypeA {
			resp.Answers = []dnsmessage.Resource{{
				Header: dnsmessage.ResourceHeader{Name: q.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 60},
				Body:   &dnsmessage.AResource{A: [4]byte{192, 0, 2, 1}},
			}}
		}
		b, err := resp.Pack()
		common.Must(err)
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(b)
	})
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	srv := &http.Server{Handler: h2c.NewHandler(handler, &http2.Server{})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String(), slowArrived
}

// silencer relays connections to target; silence makes the connections open
// so far pass nothing more either way while they stay open — a proxy server
// that stopped relaying. Connections opened later relay as before: the new
// route of a dial after routing moved on.
type silencer struct {
	ln     gonet.Listener
	dials  atomic.Int32
	mu     sync.Mutex
	open   []*atomic.Bool
	closed chan struct{} // a silenced connection was closed by the client
}

func newSilencer(t *testing.T, target string) *silencer {
	t.Helper()
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	s := &silencer{ln: ln, closed: make(chan struct{}, 8)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			s.dials.Add(1)
			server, err := gonet.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			quiet := &atomic.Bool{}
			s.mu.Lock()
			s.open = append(s.open, quiet)
			s.mu.Unlock()
			t.Cleanup(func() { client.Close(); server.Close() })
			go s.relay(client, server, quiet, true)
			go s.relay(server, client, quiet, false)
		}
	}()
	return s
}

func (s *silencer) relay(from, to gonet.Conn, quiet *atomic.Bool, fromClient bool) {
	buf := make([]byte, 32<<10)
	for {
		n, err := from.Read(buf)
		if err != nil {
			if fromClient && quiet.Load() {
				s.closed <- struct{}{}
			}
			to.Close()
			return
		}
		if !quiet.Load() {
			if _, err := to.Write(buf[:n]); err != nil {
				return
			}
		}
	}
}

func (s *silencer) silence() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, quiet := range s.open {
		quiet.Store(true)
	}
}

func (s *silencer) server(t *testing.T) *DoHNameServer {
	t.Helper()
	u, err := url.Parse("h2c://" + s.ln.Addr().String() + "/dns-query")
	common.Must(err)
	return NewDoHNameServer(u, nil, true, true, false, 0, net.IP(nil))
}

func lookup(s *DoHNameServer, name string, within time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	_, _, err := s.QueryIP(ctx, name, dns_feature.IPOption{IPv4Enable: true})
	return err
}

func TestDOHNameServerDropsSilentConnection(t *testing.T) {
	addr, _ := dohStub(t)
	front := newSilencer(t, addr)
	s := front.server(t)

	common.Must(lookup(s, "a.example.com", 3*time.Second))
	front.silence()
	if err := lookup(s, "b.example.com", time.Second); err == nil {
		t.Fatal("a query through the silent connection got an answer")
	}
	select {
	case <-front.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("the connection silent since the failed query stayed open")
	}
	if err := lookup(s, "c.example.com", 3*time.Second); err != nil {
		t.Fatalf("no answer through a new connection: %v", err)
	}
	if got := front.dials.Load(); got != 2 {
		t.Fatalf("dials: %d, want 2", got)
	}
}

func TestDOHNameServerKeepsAnsweringConnection(t *testing.T) {
	addr, slowArrived := dohStub(t)
	front := newSilencer(t, addr)
	s := front.server(t)

	common.Must(lookup(s, "a.example.com", 3*time.Second))
	slow := make(chan error, 1)
	go func() { slow <- lookup(s, "slow.example.com", 3*time.Second) }()
	select {
	case <-slowArrived:
	case <-time.After(3 * time.Second):
		t.Fatal("the slow query never reached the server")
	}
	// Answered after the slow one was sent: the connection is alive.
	common.Must(lookup(s, "b.example.com", 3*time.Second))
	if err := <-slow; err == nil {
		t.Fatal("the slow name got an answer")
	}
	common.Must(lookup(s, "c.example.com", 3*time.Second))
	if got := front.dials.Load(); got != 1 {
		t.Fatalf("dials: %d, want 1: an answering connection was closed", got)
	}
}
