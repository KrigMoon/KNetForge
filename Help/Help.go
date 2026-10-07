package Help

import (
	"sync"
)

//region Метрики

type ChanMetrics struct {
	Len int `json:"len"`
	Cap int `json:"cap"`
}

//endregion

//region Однократное выполнение

type DoOnce struct {
	mu   sync.Mutex
	done bool
}

func (do *DoOnce) Do(f func()) {
	do.mu.Lock()
	defer do.mu.Unlock()
	if do.done {
		return
	}
	do.done = true
	f()
}

func (do *DoOnce) Reset() {
	do.mu.Lock()
	defer do.mu.Unlock()
	do.done = false
}

//endregion
