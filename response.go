package pubsub

import (
	"context"
	"github.com/BetBit/pubsub-go/observers"
)

type Response struct {
	event    string
	handlers *observers.Store
	callback func(ctx context.Context, payload []byte)
}

// Do - Do response
func (r *Response) Do() {

}
