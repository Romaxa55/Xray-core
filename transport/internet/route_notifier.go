// MegaV addition (2026-05-22): route notification hook.
//
// Внешние слои (например libXray) могут зарегистрировать callback который
// будет вызываться при ИЗМЕНЕНИИ маршрута dial'а конкретного outbound'а.
// Используется для observability — Dart-сторона показывает на карте
// "exit-N идёт через bs-M" или "exit-N свалился на fallback direct".
//
// API дизайнен как push-нотификации (caller подписывается), а не pull —
// чтобы избежать lock'а на каждый Dial. Реализация callback'а должна быть
// быстрой (atomic update map) — она исполняется в hot path.

package internet

import "sync/atomic"

// RouteNotifier — функция, вызываемая при изменении маршрута outbound'а.
//   - outboundTag: имя outbound'а который dial'ит (= exit, например "server-391")
//   - route:
//       "via:<tag>" — chain активен, dial идёт через указанный outbound (= bs)
//       "fallback"  — chain свалился, dial пошёл через DialerProxyFallbackTag
//       "direct"    — нет dialer_proxy, прямой dial
//
// Поведение колл-сайтов:
//   - dialer.go вызывает с "via:<resolvedTag>" в момент успешного resolve
//     dialer_proxy → real outbound.
//   - dialer_fallback.go вызывает с "fallback" когда buffer-replay активировался.
//
// Multiple calls для одного outboundTag — last write wins. Тoescript
// гонок (две parallel'ные dial'а через server-391) — это нормально,
// читатель видит «текущий» маршрут (= последний).
type RouteNotifier func(outboundTag, route string)

// atomic.Value содержит RouteNotifier (или nil). atomic чтобы избежать
// lock'а на read path (hot, тысячи dial'ов в минуту).
var routeNotifier atomic.Value

// SetRouteNotifier регистрирует callback. nil = отключить notification.
// Только один listener — повторный вызов перезаписывает.
//
// Идемпотентно безопасно вызывать многократно (например при rebind libXray).
func SetRouteNotifier(f RouteNotifier) {
	if f == nil {
		// Сохраняем typed-nil чтобы atomic.Value не падал на разнотипных
		// store'ах.
		routeNotifier.Store(RouteNotifier(nil))
		return
	}
	routeNotifier.Store(f)
}

// notifyRoute — internal helper, безопасно вызвать даже если notifier nil.
func notifyRoute(outboundTag, route string) {
	if outboundTag == "" {
		return // dial без outbound-контекста — нечего трекать
	}
	v := routeNotifier.Load()
	if v == nil {
		return
	}
	f, ok := v.(RouteNotifier)
	if !ok || f == nil {
		return
	}
	f(outboundTag, route)
}
