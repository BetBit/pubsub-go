package pubsub

import (
	"context"
	"social-casino-platform/g1/pkg/pubsub/observers"
)

type Response struct {
	event    string
	handlers *observers.Store
	callback func(ctx context.Context, payload []byte)
}

// Do - Do response
func (r *Response) Do() {

}
