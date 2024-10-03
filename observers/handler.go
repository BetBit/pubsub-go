package observers

import (
	"context"
	"github.com/BetBit/pubsub-go/metadata"
	pb "github.com/BetBit/pubsub-go/proto"
	"sync"
)

type observers struct {
	rw        sync.RWMutex
	callbacks map[string]func(ctx context.Context, payload []byte)
}

func (o *observers) Add(id string, cb func(ctx context.Context, payload []byte)) {
	o.rw.Lock()
	o.callbacks[id] = cb
	o.rw.Unlock()
}

func (o *observers) Delete(id string) bool {
	o.rw.Lock()
	defer o.rw.Unlock()
	delete(o.callbacks, id)
	return len(o.callbacks) == 0
}

func (o *observers) Handle(msg *pb.Event) {
	o.rw.RLock()
	var wg sync.WaitGroup
	for _, cb := range o.callbacks {
		wg.Add(1)
		ctx := metadata.CreateContext(msg)
		go func(c func(ctx context.Context, payload []byte), m *pb.Event) {
			defer wg.Done()
			c(ctx, m.Payload)
		}(cb, msg)
	}
	wg.Wait()
	o.rw.RUnlock()
}
