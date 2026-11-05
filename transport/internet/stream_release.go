package internet

import "sync"

var (
	streamReleasersAccess sync.RWMutex
	streamReleasers       []func(*MemoryStreamConfig)
)

// RegisterStreamReleaser adds a function that frees what a transport keeps for one
// stream config past a dial, such as a gRPC client and the goroutines that keep it
// connected.
func RegisterStreamReleaser(release func(*MemoryStreamConfig)) {
	streamReleasersAccess.Lock()
	defer streamReleasersAccess.Unlock()
	streamReleasers = append(streamReleasers, release)
}

// ReleaseStreamConfig frees what every transport keeps for config. The outbound that
// owns config calls it when it closes: nothing dials with config after that.
func ReleaseStreamConfig(config *MemoryStreamConfig) {
	if config == nil {
		return
	}
	streamReleasersAccess.RLock()
	defer streamReleasersAccess.RUnlock()
	for _, release := range streamReleasers {
		release(config)
	}
}
