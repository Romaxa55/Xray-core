package conf_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
)

// Share links cut off with an ellipsis end the encryption value in padding
// with no key; that is a config error, not a panic (slice bounds out of range).
func TestVLessCutOffEncryption(t *testing.T) {
	for _, v := range []string{
		"mlkem768x25519plus.native.0rtt.…",
		"mlkem768x25519plus.xorpub.1rtt.pad",
		"mlkem768x25519plus.random.0rtt.a.b",
	} {
		out := new(VLessOutboundConfig)
		in := `{"vnext":[{"address":"example.com","port":443,"users":[{"id":"27848739-7e62-4138-9fd3-098a63964b6b","encryption":"` + v + `"}]}]}`
		if err := json.Unmarshal([]byte(in), out); err != nil {
			t.Fatal(err)
		}
		if _, err := out.Build(); err == nil || !strings.Contains(err.Error(), "unsupported \"encryption\"") {
			t.Fatalf("outbound %q: got %v, want unsupported encryption", v, err)
		}

		inb := new(VLessInboundConfig)
		in = `{"clients":[{"id":"27848739-7e62-4138-9fd3-098a63964b6b"}],"decryption":"` + strings.Replace(v, "rtt", "s", 1) + `"}`
		if err := json.Unmarshal([]byte(in), inb); err != nil {
			t.Fatal(err)
		}
		if _, err := inb.Build(); err == nil {
			t.Fatalf("inbound %q: want an error", v)
		}
	}
}
