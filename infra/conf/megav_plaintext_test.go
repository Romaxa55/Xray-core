package conf_test

import (
	"encoding/json"
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
)

// MegaV fork: VLESS and Trojan without TLS to a public address still build (upstream refuses them since
// XTLS/Xray-core#6303); a rebase that brings the refusal back fails here. megav.app, not a reserved name: upstream
// lets private and reserved domains through, and the case would pass with the refusal back.
func TestMegaVPlaintextOutboundToPublicAddressBuilds(t *testing.T) {
	cases := map[string]string{
		"vless ws none, public IP": `{
			"protocol": "vless",
			"settings": {"address": "1.1.1.1", "port": 80, "id": "27848739-7e62-4138-9fd3-098a63964b6b", "encryption": "none"},
			"streamSettings": {"network": "ws", "security": "none", "wsSettings": {"path": "/"}}
		}`,
		"vless raw, public domain": `{
			"protocol": "vless",
			"settings": {"vnext": [{"address": "megav.app", "port": 8080, "users": [{"id": "27848739-7e62-4138-9fd3-098a63964b6b", "encryption": "none"}]}]}
		}`,
		"trojan flat, public IP": `{
			"protocol": "trojan",
			"settings": {"address": "1.1.1.1", "port": 443, "password": "p"}
		}`,
		"trojan raw none, public domain": `{
			"protocol": "trojan",
			"settings": {"servers": [{"address": "megav.app", "port": 80, "password": "p"}]},
			"streamSettings": {"security": "none"}
		}`,
	}
	for name, raw := range cases {
		var c OutboundDetourConfig
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := c.Build(); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
}
