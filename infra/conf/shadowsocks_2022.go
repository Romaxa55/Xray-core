//go:build !zeus_nogpl

// Shadowsocks 2022 is built on github.com/sagernet/sing-shadowsocks and github.com/sagernet/sing (GPL-3.0-or-later).
// A build with the zeus_nogpl tag goes without them: shadowsocks_2022_nogpl.go.

package conf

import (
	"strings"

	"github.com/sagernet/sing-shadowsocks/shadowaead_2022"
	C "github.com/sagernet/sing/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/proxy/shadowsocks_2022"
	"google.golang.org/protobuf/proto"
)

func isShadowsocks2022(cipher string) bool {
	return C.Contains(shadowaead_2022.List, cipher)
}

func buildShadowsocks2022Client(server *ShadowsocksServerTarget) (proto.Message, error) {
	config := new(shadowsocks_2022.ClientConfig)
	config.Address = server.Address.Build()
	config.Port = uint32(server.Port)
	config.Method = server.Cipher
	config.Key = server.Password
	config.UdpOverTcp = server.UoT
	config.UdpOverTcpVersion = uint32(server.UoTVersion)
	return config, nil
}

func buildShadowsocks2022(v *ShadowsocksServerConfig) (proto.Message, error) {
	if len(v.Users) == 0 {
		config := new(shadowsocks_2022.ServerConfig)
		config.Method = v.Cipher
		config.Key = v.Password
		config.Network = v.NetworkList.Build()
		config.Email = v.Email
		return config, nil
	}

	if v.Cipher == "" {
		return nil, errors.New("shadowsocks 2022 (multi-user): missing server method")
	}
	if !strings.Contains(v.Cipher, "aes") {
		return nil, errors.New("shadowsocks 2022 (multi-user): only blake3-aes-*-gcm methods are supported")
	}

	if v.Users[0].Address == nil {
		config := new(shadowsocks_2022.MultiUserServerConfig)
		config.Method = v.Cipher
		config.Key = v.Password
		config.Network = v.NetworkList.Build()

		config.Users = make([]*protocol.User, len(v.Users))
		processUser := func(idx int) error {
			user := v.Users[idx]
			if user.Cipher != "" {
				return errors.New("shadowsocks 2022 (multi-user): users must have empty method")
			}
			account := &shadowsocks_2022.Account{
				Key: user.Password,
			}
			config.Users[idx] = &protocol.User{
				Email:   user.Email,
				Level:   uint32(user.Level),
				Account: serial.ToTypedMessage(account),
			}
			return nil
		}
		if err := task.ParallelForN(len(v.Users), processUser); err != nil {
			return nil, err
		}
		return config, nil
	}

	config := new(shadowsocks_2022.RelayServerConfig)
	config.Method = v.Cipher
	config.Key = v.Password
	config.Network = v.NetworkList.Build()
	for _, user := range v.Users {
		if user.Cipher != "" {
			return nil, errors.New("shadowsocks 2022 (relay): users must have empty method")
		}
		if user.Address == nil {
			return nil, errors.New("shadowsocks 2022 (relay): all users must have relay address")
		}
		config.Destinations = append(config.Destinations, &shadowsocks_2022.RelayDestination{
			Key:     user.Password,
			Email:   user.Email,
			Address: user.Address.Build(),
			Port:    uint32(user.Port),
		})
	}
	return config, nil
}
