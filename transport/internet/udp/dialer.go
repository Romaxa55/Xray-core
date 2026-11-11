package udp

import (
	"context"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func init() {
	common.Must(internet.RegisterTransportDialer(protocolName,
		func(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
			var sockopt *internet.SocketConfig
			if streamSettings != nil {
				sockopt = streamSettings.SocketSettings
			}
			conn, err := internet.DialSystem(ctx, dest, sockopt)
			if err != nil {
				return nil, err
			}

			if streamSettings != nil && streamSettings.UdpmaskManager != nil {
				// Through a dialerProxy this is a stream, also inside the fallback wrapper of dialer_fallback.go, which
				// hid the cnc type from the switch here and reached panic(reflect.TypeOf(c)): the whole process died
				// (MegaV#360).
				pktConn, udpAddr, _, err := internet.DialedPacketConn(conn, len(sockopt.GetDialerProxy()) > 0)
				if err != nil {
					if conn != nil {
						conn.Close()
					}
					return nil, errors.New("udp with udpmask").Base(err)
				}
				newConn, err := streamSettings.UdpmaskManager.WrapPacketConnClient(pktConn)
				if err != nil {
					pktConn.Close()
					return nil, errors.New("mask err").Base(err)
				}
				pktConn = newConn
				conn = &internet.PacketConnWrapper{
					PacketConn: pktConn,
					Dest:       udpAddr,
				}
			}

			return conn, nil
		}))
}
