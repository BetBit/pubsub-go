package pubsub

import (
	"context"
	pb "event-source-platform/office/pkg/pubsub/proto"
	"github.com/google/uuid"
	"sync"
)

type handler struct {
	list []struct {
		id string
		fn func(ctx context.Context, payload []byte)
	}
}

func (h *handler) add(handler func(ctx context.Context, payload []byte)) string {
	id := uuid.New().String()
	h.list = append(h.list, struct {
		id string
		fn func(ctx context.Context, payload []byte)
	}{
		id: id,
		fn: handler,
	})

	return id
}

func (h *handler) delete(id string) {
	for i, v := range h.list {
		if v.id == id {
			h.list = append(h.list[:i], h.list[i+1:]...)
		}
	}
}

func (h *handler) handle(msg *pb.Event) {
	ctx := createContext(msg)

	var wg sync.WaitGroup
	for _, handler := range h.list {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.fn(ctx, msg.Payload)
		}()
	}
	wg.Wait()
}
