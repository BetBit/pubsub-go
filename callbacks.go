package pubsub

import (
	pb "social-casino-platform/g1/pkg/pubsub/proto"
	"sync"
)

func newCallbacks() *callbacks {
	return &callbacks{
		handlers: make(map[string]chan *pb.Event),
	}
}

type callbacks struct {
	rw       sync.RWMutex
	handlers map[string]chan *pb.Event
}

func (c *callbacks) Add(id string, msg chan *pb.Event) {
	c.rw.Lock()
	c.handlers[id] = msg
	c.rw.Unlock()
}

func (c *callbacks) Delete(id string) {
	c.rw.Lock()
	delete(c.handlers, id)
	c.rw.Unlock()
}

func (c *callbacks) Handle(msg *pb.Event) {
	c.rw.RLock()
	defer c.rw.RUnlock()

	if ch, ok := c.handlers[msg.Id]; ok {
		ch <- msg
	}
}
