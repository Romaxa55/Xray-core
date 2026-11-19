//go:build zeus_nogpl

// A build without github.com/sagernet/sing* (GPL-3.0-or-later), the zeus_nogpl tag: no Shadowsocks 2022. A config
// with one of its methods fails to build with a plain error instead of linking the GPL code.

package conf

import (
	"github.com/xtls/xray-core/common/errors"
	"google.golang.org/protobuf/proto"
)

// The methods of shadowaead_2022.List, named here so that a 2022 config is told apart without the package.
var shadowsocks2022Methods = map[string]bool{
	"2022-blake3-aes-128-gcm":       true,
	"2022-blake3-aes-256-gcm":       true,
	"2022-blake3-chacha20-poly1305": true,
}

func isShadowsocks2022(cipher string) bool {
	return shadowsocks2022Methods[cipher]
}

func errNoShadowsocks2022() error {
	return errors.New("Shadowsocks 2022 is not in this build (zeus_nogpl: built without GPL-3.0 github.com/sagernet/sing)")
}

func buildShadowsocks2022Client(*ShadowsocksServerTarget) (proto.Message, error) {
	return nil, errNoShadowsocks2022()
}

func buildShadowsocks2022(*ShadowsocksServerConfig) (proto.Message, error) {
	return nil, errNoShadowsocks2022()
}
