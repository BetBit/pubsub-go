package pubsub

import (
	"fmt"
	pb "github.com/BetBit/pubsub-go/proto"
	"sync"
)

type subscribers struct {
	rw   sync.RWMutex
	list map[string]chan *pb.Event
}

func (s *subscribers) Add(id string, res chan *pb.Event) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.list[id] = res
}

func (s *subscribers) Delete(id string) {
	s.rw.Lock()
	defer s.rw.Unlock()
	delete(s.list, id)
}

func (s *subscribers) Handle(msg *pb.Event) {
	s.rw.Lock()
	defer s.rw.Unlock()
	cbId := fmt.Sprintf("%s:%s", msg.Name, msg.Id)
	if res, ok := s.list[cbId]; ok {
		res <- msg
		delete(s.list, cbId)
	}
}
