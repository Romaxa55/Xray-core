package geodata

import (
	"net"
	"runtime"
	"testing"
	"time"
	"weak"
)

// collected runs the GC until every pointer of gone is collected, a second at most: a cleanup
// that drops a registry's entry runs after the collection, on a goroutine of its own.
func collected[T any](gone []weak.Pointer[T]) bool {
	for deadline := time.Now().Add(time.Second); ; {
		runtime.GC()
		left := 0
		for _, w := range gone {
			if w.Value() != nil {
				left++
			}
		}
		if left == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A core restarted in the same process (an app that switches its exit) builds the matchers of
// its router, DNS and outbounds again, and the closed instance drops its own. The registries
// used to keep every matcher ever built for a GeoSite or GeoIP reload, so each restart left
// the closed instance's matchers on the heap for good. A matcher nobody holds is collected
// now; the one in use stays in the registry, matches, and reloads.
func TestRegistriesLetGoOfAClosedInstancesMatchers(t *testing.T) {
	domainRules, err := ParseDomainRules([]string{"domain:example.com", `regexp:^ads[0-9]+\.example\.org$`}, Domain_Domain)
	if err != nil {
		t.Fatal(err)
	}
	ipRules, err := ParseIPRules([]string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	domains, ips := newDomainRegistry(), newIPRegistry()
	keptDomains, err := domains.BuildDomainMatcher(domainRules)
	if err != nil {
		t.Fatal(err)
	}
	keptIPs, err := ips.BuildIPMatcher(ipRules)
	if err != nil {
		t.Fatal(err)
	}

	var goneDomains []weak.Pointer[DynamicDomainMatcher]
	var goneIPs []weak.Pointer[DynamicIPMatcher]
	for range 10 {
		d, err := domains.BuildDomainMatcher(domainRules)
		if err != nil {
			t.Fatal(err)
		}
		i, err := ips.BuildIPMatcher(ipRules)
		if err != nil {
			t.Fatal(err)
		}
		goneDomains = append(goneDomains, weak.Make(d.(*DynamicDomainMatcher)))
		goneIPs = append(goneIPs, weak.Make(i.(*DynamicIPMatcher)))
	}
	if !collected(goneDomains) {
		t.Fatal("the domain registry still holds the matchers of closed instances")
	}
	if !collected(goneIPs) {
		t.Fatal("the IP registry still holds the matchers of closed instances")
	}

	if err := domains.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := ips.Reload(); err != nil {
		t.Fatal(err)
	}
	if !keptDomains.MatchAny("www.example.com") || !keptDomains.MatchAny("ads12.example.org") {
		t.Error("the domain matcher in use no longer matches after a reload")
	}
	if !keptIPs.Match(net.ParseIP("10.1.2.3")) || keptIPs.Match(net.ParseIP("192.0.2.1")) {
		t.Error("the IP matcher in use no longer matches after a reload")
	}
	runtime.KeepAlive(keptDomains)
	runtime.KeepAlive(keptIPs)
}
