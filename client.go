package pubsub

import (
	"context"
	pb "event-source-platform/office/pkg/pubsub/proto"
)

type Options struct {
	Address     []string
	Token       string
	ID          string
	Brand       string
	Subscribers []string
	Publishers  []string
}

func New(opt Options) *Client {
	reqPool := make(chan *pb.Event, 1)
	subs := &subscribers{
		list: make(map[string]chan *pb.Event),
	}
	cbs := newCallbacks()
	cli := &Client{
		connector: NewConnector(OptionsConnector{
			Address:     opt.Address,
			Token:       opt.Token,
			ID:          opt.ID,
			Brand:       opt.Brand,
			Subscribers: opt.Subscribers,
			Publishers:  opt.Publishers,
			ReqPool:     reqPool,
		}),
		subscribers: subs,
		callbacks:   cbs,
	}

	go func() {
		for msg := range reqPool {
			subs.Handle(msg)
			cbs.Handle(msg)
		}
	}()

	return cli
}

type Client struct {
	connector   *connector
	subscribers *subscribers
	callbacks   *callbacks
}

// WithBrand - Set brand to event
func (c *Client) WithBrand(ctx context.Context, brand string) context.Context {
	return withBrand(ctx, brand)
}

// Brand - Get brand from event
func (c *Client) Brand(ctx context.Context) string {
	return fromContext(ctx).brand
}

// Error - Check and error in event
func (c *Client) Error(ctx context.Context) error {
	return metaError(ctx)
}

// WithError - Send error with event
func (c *Client) WithError(ctx context.Context, err error) context.Context {
	return withError(ctx, err)
}

func (c *Client) Pub(ctx context.Context, event string, bytes []byte) *Request {
	r := &Request{
		context:     ctx,
		name:        event,
		connector:   c.connector,
		payload:     bytes,
		subscribers: c.subscribers,
	}
	return r
}

func (c *Client) Sub(ctx context.Context, event string, handler func(ctx context.Context, payload []byte)) {
	id := c.callbacks.Add(event, handler)
	go func() {
		<-ctx.Done()
		c.callbacks.Delete(event, id)
	}()
}
