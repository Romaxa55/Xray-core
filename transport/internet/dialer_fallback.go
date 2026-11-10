// MegaV addition (2026-05-22): fallback на handshake-fail через dialerProxy.
//
// Когда `SocketConfig.DialerProxyFallbackTag` непустой, dial-результат через
// `dialer_proxy` оборачивается в `fallbackDialerConn`. Wrapper буферизует
// первый Write (= TLS ClientHello), и если первый Read возвращает error —
// re-dial'ит через указанный fallback-outbound (например `direct`) и
// replay'ит буфер.
//
// Use case: chain через trojan/CF bs не пускает Reality handshake (WS-close
// 1006). Без этого механизма exit становится навсегда dead в observatory.
// С механизмом — fallback на direct dial проходит, exit работает.
//
// Safety:
//   - Replay буфер = первый ClientHello, ~256-512 байт. Это публичные байты
//     TLS handshake, повторение к тому же серверу через другой путь —
//     нормальная ситуация (любой браузер делает retry).
//   - Анти-replay защита в proxy (trojan/Reality) защищает от **повторного
//     auth** к одному и тому же серверу через тот же путь. У нас тот же
//     destination, но другой network-путь — для target-сервера это первая
//     попытка.
//
// Limits:
//   - Буферизация только до первого Read. После первого успешного Read
//     handshake закончился — буфер сбрасывается, никакого retry больше нет.
//   - Если данных без Read'а написали > 64KB — отключаем retry (это не
//     handshake, это уже data-stream, replay небезопасен).

package internet

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	xrayerrors "github.com/xtls/xray-core/common/errors"
)

const (
	// Лимит буфера на первый Write до первого Read. После — отключаем retry.
	fallbackMaxBufferBytes = 64 * 1024
	// Лимит времени между dial и первым Read. После — отключаем retry.
	fallbackMaxBufferDuration = 10 * time.Second
)

type fallbackDialerConn struct {
	net.Conn // current active conn (primary или promoted fallback)

	ctx      context.Context
	fallback func() (net.Conn, error)

	mu        sync.Mutex
	startedAt time.Time
	bufSize   int
	writeBuf  [][]byte
	// state machine: armed → promoted | armed → committed | armed → expired
	armed     bool // true = ещё можем сделать fallback retry
	promoted  bool // true = уже переключились на fallback (для diag)
	committed bool // true = первый Read прошёл успешно, retry больше нельзя
	closed    bool // Close was called: a fallback dialled after it is closed at once (MegaV#355)
}

// Write — буферизуем пока armed, иначе passthrough.
func (c *fallbackDialerConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	if c.armed {
		// budget check
		if c.bufSize+len(b) > fallbackMaxBufferBytes ||
			time.Since(c.startedAt) > fallbackMaxBufferDuration {
			// disarmed — слишком много данных или слишком долго до Read
			c.armed = false
			c.writeBuf = nil
		} else {
			cp := make([]byte, len(b))
			copy(cp, b)
			c.writeBuf = append(c.writeBuf, cp)
			c.bufSize += len(b)
		}
	}
	conn := c.Conn // under the lock, see current
	c.mu.Unlock()
	return conn.Write(b)
}

// Read — на первый ошибке (если ещё armed) делаем retry через fallback.
func (c *fallbackDialerConn) Read(b []byte) (int, error) {
	n, err := c.current().Read(b)
	if err == nil {
		// успех первого Read → handshake прошёл, retry больше не нужен
		c.mu.Lock()
		if c.armed {
			c.armed = false
			c.committed = true
			c.writeBuf = nil // free memory
		}
		c.mu.Unlock()
		return n, err
	}

	// Чистый EOF / closed pipe — handshake может быть в порядке, просто peer
	// закрыл. Не делаем retry чтобы не повторять auth.
	if errors.Is(err, net.ErrClosed) {
		return n, err
	}

	c.mu.Lock()
	if !c.armed {
		c.mu.Unlock()
		return n, err
	}
	// armed && error → пробуем fallback
	c.armed = false // disable на время retry чтобы избежать рекурсии
	bufCopy := c.writeBuf
	c.writeBuf = nil
	c.mu.Unlock()

	xrayerrors.LogInfo(c.ctx, "dialerProxy chain failed on first Read, "+
		"retrying via fallback outbound (replay ", len(bufCopy), " writes): ", err)

	fb, ferr := c.fallback()
	if ferr != nil {
		xrayerrors.LogInfo(c.ctx, "fallback dial failed: ", ferr,
			" — returning original chain error")
		return n, err // оригинальная chain-ошибка
	}

	// replay буферизованных writes к новому conn
	for i, w := range bufCopy {
		if _, werr := fb.Write(w); werr != nil {
			xrayerrors.LogInfo(c.ctx, "fallback replay write #", i, " failed: ", werr)
			fb.Close()
			return n, err
		}
	}

	// success — заменяем conn под капотом. Closed meanwhile: nobody would ever close fb.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		fb.Close()
		return n, err
	}
	oldConn := c.Conn
	c.Conn = fb
	c.promoted = true
	c.mu.Unlock()

	// Закрываем primary в фоне (не блокируем Read).
	// MegaV 2026-05-22 (golang-pro audit): fire-and-forget с deadline.
	// Если oldConn.Close() виснет (CF Workers не отвечает на FIN/RST),
	// goroutine утечёт навсегда → growing baseline на каждый fallback.
	// Добавляем 3-sec deadline и recover от panic в Close.
	go func(c net.Conn) {
		defer func() {
			if r := recover(); r != nil {
				// ignore — close уже мог быть вызван
			}
		}()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = c.Close()
		}()
		select {
		case <-done:
			// closed gracefully
		case <-time.After(3 * time.Second):
			// hung — abandon, GC eventually
		}
	}(oldConn)

	// Теперь делаем Read через новый fb
	return fb.Read(b)
}

// current is the active conn. Read swaps c.Conn on a fallback, so every method takes it under the lock (MegaV#355).
func (c *fallbackDialerConn) current() net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn
}

func (c *fallbackDialerConn) LocalAddr() net.Addr           { return c.current().LocalAddr() }
func (c *fallbackDialerConn) RemoteAddr() net.Addr          { return c.current().RemoteAddr() }
func (c *fallbackDialerConn) SetDeadline(t time.Time) error { return c.current().SetDeadline(t) }
func (c *fallbackDialerConn) SetReadDeadline(t time.Time) error {
	return c.current().SetReadDeadline(t)
}
func (c *fallbackDialerConn) SetWriteDeadline(t time.Time) error {
	return c.current().SetWriteDeadline(t)
}

// Close disarms the fallback first (MegaV#355): the Read our own Close unblocks fails with an error that is no
// chain failure (io.ErrClosedPipe from cnc), and a re-dial on it would leave a conn nobody closes.
func (c *fallbackDialerConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.armed = false
	c.writeBuf = nil
	conn := c.Conn
	c.mu.Unlock()
	return conn.Close()
}

// wrapFallback создаёт обёртку. fallback — closure, который делает re-dial
// (например через direct). Caller должен убедиться что fallback не делает
// retry сам (бесконечная рекурсия).
func wrapFallback(ctx context.Context, primary net.Conn, fallback func() (net.Conn, error)) net.Conn {
	return &fallbackDialerConn{
		Conn:      primary,
		ctx:       ctx,
		fallback:  fallback,
		startedAt: time.Now(),
		armed:     true,
	}
}
