//go:build zeus_nogpl

package conf_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/shadowsocks"
)

// Without github.com/sagernet/sing (the zeus_nogpl tag) a Shadowsocks 2022 config fails with a plain error, outbound
// and inbound, and a classic method builds as it always did.
func TestShadowsocks2022IsNotInANoGPLBuild(t *testing.T) {
	for _, method := range []string{"2022-blake3-aes-128-gcm", "2022-blake3-aes-256-gcm", "2022-blake3-chacha20-poly1305"} {
		client := new(ShadowsocksClientConfig)
		if err := json.Unmarshal([]byte(`{"address": "127.0.0.1", "port": 1234, "method": "`+method+`", "password": "x"}`), client); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Build(); err == nil || !strings.Contains(err.Error(), "Shadowsocks 2022 is not in this build") {
			t.Errorf("%s outbound: %v", method, err)
		}
		server := new(ShadowsocksServerConfig)
		if err := json.Unmarshal([]byte(`{"method": "`+method+`", "password": "x"}`), server); err != nil {
			t.Fatal(err)
		}
		if _, err := server.Build(); err == nil || !strings.Contains(err.Error(), "Shadowsocks 2022 is not in this build") {
			t.Errorf("%s inbound: %v", method, err)
		}
	}
	client := new(ShadowsocksClientConfig)
	if err := json.Unmarshal([]byte(`{"address": "127.0.0.1", "port": 1234, "method": "aes-256-gcm", "password": "x"}`), client); err != nil {
		t.Fatal(err)
	}
	built, err := client.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := built.(*shadowsocks.ClientConfig); !ok {
		t.Fatalf("aes-256-gcm built %T", built)
	}
}
