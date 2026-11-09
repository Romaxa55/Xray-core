package geodata

import (
	"net"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/xtls/xray-core/common/geodata/strmatcher"
	"github.com/xtls/xray-core/common/utils"
)

// collected runs the GC until none of gone points to a live value, a second at most: an
// object on the way may need more than one cycle.
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
// now; the one in use stays in the registry, matches, and reloads. Both factories are run:
// the compact one is what iOS and Android build with, whatever the OS of the test.
func TestDomainRegistryLetsGoOfAClosedInstancesMatchers(t *testing.T) {
	factories := map[string]func() DomainMatcherFactory{
		"compact": func() DomainMatcherFactory {
			return &CompactDomainMatcherFactory{shared: utils.NewWeakCacheMap[string, strmatcher.LinearAnyMatcher]()}
		},
		"mph": func() DomainMatcherFactory {
			return &MphDomainMatcherFactory{shared: utils.NewWeakCacheMap[string, strmatcher.MphValueMatcher]()}
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			rules, err := ParseDomainRules([]string{"domain:example.com", `regexp:^ads[0-9]+\.example\.org$`}, Domain_Domain)
			if err != nil {
				t.Fatal(err)
			}
			domains := newDomainRegistry()
			domains.factory = factory()
			kept, err := domains.BuildDomainMatcher(rules)
			if err != nil {
				t.Fatal(err)
			}

			var gone []weak.Pointer[DynamicDomainMatcher]
			for range 10 {
				d, err := domains.BuildDomainMatcher(rules)
				if err != nil {
					t.Fatal(err)
				}
				gone = append(gone, weak.Make(d.(*DynamicDomainMatcher)))
			}
			if !collected(gone) {
				t.Fatal("the domain registry still holds the matchers of closed instances")
			}

			if err := domains.Reload(); err != nil {
				t.Fatal(err)
			}
			if !kept.MatchAny("www.example.com") || !kept.MatchAny("ads12.example.org") || kept.MatchAny("example.org") {
				t.Error("the domain matcher in use no longer matches as before after a reload")
			}
			runtime.KeepAlive(kept)
		})
	}
}

func TestIPRegistryLetsGoOfAClosedInstancesMatchers(t *testing.T) {
	rules, err := ParseIPRules([]string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	ips := newIPRegistry()
	kept, err := ips.BuildIPMatcher(rules)
	if err != nil {
		t.Fatal(err)
	}

	var gone []weak.Pointer[DynamicIPMatcher]
	for range 10 {
		i, err := ips.BuildIPMatcher(rules)
		if err != nil {
			t.Fatal(err)
		}
		gone = append(gone, weak.Make(i.(*DynamicIPMatcher)))
	}
	if !collected(gone) {
		t.Fatal("the IP registry still holds the matchers of closed instances")
	}

	if err := ips.Reload(); err != nil {
		t.Fatal(err)
	}
	if !kept.Match(net.ParseIP("10.1.2.3")) || kept.Match(net.ParseIP("192.0.2.1")) {
		t.Error("the IP matcher in use no longer matches as before after a reload")
	}
	runtime.KeepAlive(kept)
}
