//go:build zeus_nogpl

// A build without github.com/sagernet/sing (GPL-3.0-or-later), the zeus_nogpl tag: no UDP-over-TCP server side. Every
// destination is a plain one, as any destination but sing's magic UoT names is in uot.go.

package outbound

import (
	"context"
	"os"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func (h *Handler) getUoTConnection(context.Context, net.Destination) (stat.Connection, error) {
	return nil, os.ErrInvalid
}
