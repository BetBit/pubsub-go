package observers

import (
	"context"
	pb "github.com/BetBit/pubsub-go/proto"
	"github.com/google/uuid"
	"sync"
)

func New() *Store {
	return &Store{
		handlers: make(map[string]*observers),
	}
}

type Store struct {
	rw       sync.RWMutex
	handlers map[string]*observers
}

func (s *Store) Add(event string, cb func(ctx context.Context, payload []byte)) string {
	id := uuid.New().String()
	h, ok := s.get(event)
	if !ok {
		h = &observers{
			callbacks: make(map[string]func(ctx context.Context, payload []byte)),
		}
		s.rw.Lock()
		s.handlers[event] = h
		s.rw.Unlock()
	}
	h.Add(id, cb)
	return id
}

func (s *Store) Handle(msg *pb.Event) {
	h, ok := s.get(msg.Name)
	if !ok {
		return
	}
	h.Handle(msg)
}

func (s *Store) Delete(event, id string) {
	h, ok := s.get(event)
	if !ok {
		return
	}
	ok = h.Delete(id)
	if ok {
		s.rw.Lock()
		delete(s.handlers, event)
		s.rw.Unlock()
	}
}

func (s *Store) get(event string) (*observers, bool) {
	s.rw.RLock()
	h, ok := s.handlers[event]
	s.rw.RUnlock()
	return h, ok
}
