package server

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type KeepAliveManager struct {
	mu       sync.Mutex
	messages map[string]jetstream.Msg
	ticker   *time.Ticker
	done     chan struct{}
}

func NewKeepAliveManager(interval time.Duration) *KeepAliveManager {
	km := &KeepAliveManager{
		messages: make(map[string]jetstream.Msg),
		ticker:   time.NewTicker(interval),
		done:     make(chan struct{}),
	}
	go km.run()
	return km
}

func (k *KeepAliveManager) run() {
	for {
		select {
		case <-k.ticker.C:
			k.mu.Lock()
			for _, msg := range k.messages {
				_ = msg.InProgress()
			}
			k.mu.Unlock()
		case <-k.done:
			return
		}
	}
}

func (k *KeepAliveManager) Add(id string, msg jetstream.Msg) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.messages[id] = msg
}

func (k *KeepAliveManager) Remove(id string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.messages, id)
}

func (k *KeepAliveManager) Stop() {
	close(k.done)
	k.ticker.Stop()
}
