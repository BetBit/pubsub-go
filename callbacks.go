package pubsub

import (
	"context"
	pb "github.com/BetBit/pubsub-go/proto"
	"sync"
)

func newCallbacks() *callbacks {
	return &callbacks{
		list: make(map[string]*handler),
	}
}

type callbacks struct {
	rw   sync.RWMutex
	list map[string]*handler
}

func (c *callbacks) Handle(msg *pb.Event) {
	cb, ok := c.get(msg.Name)
	if !ok {
		return
	}
	go cb.handle(msg)
}

func (c *callbacks) Add(event string, fn func(ctx context.Context, payload []byte)) string {
	cb, ok := c.get(event)
	if !ok {
		cb = &handler{}
		c.add(event, cb)
	}

	id := cb.add(fn)
	return id
}

func (c *callbacks) Delete(event, id string) {
	cb, ok := c.get(event)
	if !ok {
		return
	}
	cb.delete(id)
}

func (c *callbacks) get(event string) (*handler, bool) {
	c.rw.RLock()
	defer c.rw.RUnlock()
	cb, ok := c.list[event]
	return cb, ok
}

func (c *callbacks) add(event string, cb *handler) {
	c.rw.Lock()
	defer c.rw.Unlock()
	c.list[event] = cb
}
