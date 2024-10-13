package pubsub

import (
	"context"
	"fmt"
	pb "github.com/BetBit/pubsub-go/proto"
	"github.com/google/uuid"
)

type Request struct {
	name        string
	connector   *connector
	payload     []byte
	subscribers *subscribers
	context     context.Context
}

func (r *Request) Do() {
	md := fromContext(r.context)
	var id string
	var brand string
	var errMsg string
	if md == nil {
		id = uuid.New().String()
	} else {
		id = md.eventId
		brand = md.brand
		if md.error != nil {
			errMsg = md.error.Error()
		}
	}

	r.connector.Pub(&pb.Event{
		Id:      id,
		Name:    r.name,
		Brand:   brand,
		Payload: r.payload,
		Error:   errMsg,
	})
}

func (r *Request) Sub(event string) ([]byte, error) {
	res := make(chan *pb.Event, 1)
	defer func() {
		close(res)
	}()

	id := uuid.New().String()
	r.connector.Pub(&pb.Event{
		Name:    r.name,
		Payload: r.payload,
		Id:      id,
	})

	cbId := fmt.Sprintf("%s:%s", event, id)
	r.subscribers.Add(cbId, res)

	var payload []byte
	var err error
	select {
	case msg := <-res:
		payload = msg.Payload

	case <-r.context.Done():
		r.subscribers.Delete(cbId)
		err = r.context.Err()
	}

	return payload, err
}
