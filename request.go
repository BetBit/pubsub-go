package pubsub

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"social-casino-platform/g1/pkg/pubsub/metadata"
	pb "social-casino-platform/g1/pkg/pubsub/proto"
	"time"
)

func NewRequest(ctx context.Context, callbacks *callbacks, stream pb.PubSub_ChannelClient, event string, payload []byte) *Request {
	md := metadata.FromContext(ctx)
	var id string
	var brand string
	var err error
	if md == nil {
		id = uuid.New().String()
	} else {
		id = md.EventId
		brand = md.Brand
		err = md.Error
	}

	return &Request{
		context:   ctx,
		id:        id,
		event:     event,
		brand:     brand,
		payload:   payload,
		callbacks: callbacks,
		stream:    stream,
		error:     err,
	}
}

type Request struct {
	id        string
	event     string
	payload   []byte
	context   context.Context
	stream    pb.PubSub_ChannelClient
	callbacks *callbacks
	brand     string
	error     error
}

// Sub - Subscribe to event
func (r *Request) Sub(event string, cb func(ctx context.Context, payload []byte)) error {
	msgChan := make(chan *pb.Event)
	defer close(msgChan)

	var errMsg string
	if r.error != nil {
		errMsg = r.error.Error()
	}

	r.callbacks.Add(r.id, msgChan)
	_ = r.stream.Send(&pb.Event{
		Id:        r.id,
		Brand:     r.brand,
		Name:      r.event,
		Payload:   r.payload,
		Error:     errMsg,
		Timestamp: time.Now().Unix(),
	})

	var err error
	var msg *pb.Event
	select {
	case msg = <-msgChan:
	case <-r.context.Done():
		err = r.context.Err()
		break
	}

	r.callbacks.Delete(r.id)
	ctx := metadata.CreateContext(msg)
	if msg.Error != "" {
		return fmt.Errorf(msg.Error)
	}

	cb(ctx, msg.Payload)

	return err
}

// Do - Send event
func (r *Request) Do() {
	var errMsg string
	if r.error != nil {
		errMsg = r.error.Error()
	}
	r.stream.Send(&pb.Event{
		Id:        r.id,
		Name:      r.event,
		Brand:     r.brand,
		Payload:   r.payload,
		Error:     errMsg,
		Timestamp: time.Now().Unix(),
	})
}
