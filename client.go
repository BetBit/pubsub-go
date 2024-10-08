package pubsub

import (
	"context"
	"fmt"
	"github.com/BetBit/pubsub-go/metadata"
	"github.com/BetBit/pubsub-go/observers"
	pb "github.com/BetBit/pubsub-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type Options struct {
	Token       string
	ID          string
	Brand       string
	Publishers  []string
	Subscribers []string
	Connect     *grpc.ClientConn
}

func New(opt Options) *Client {
	cli := &Client{
		conn:        opt.Connect,
		token:       opt.Token,
		clientId:    opt.ID,
		brand:       opt.Brand,
		publishers:  opt.Publishers,
		subscribers: opt.Subscribers,
		observers:   observers.New(),
		callbacks:   newCallbacks(),
		waiters:     newWaiter(),
	}
	cli.connect()

	return cli
}

type Client struct {
	conn        *grpc.ClientConn
	token       string
	clientId    string
	brand       string
	publishers  []string
	subscribers []string
	attempts    int

	waiters   *waiters
	callbacks *callbacks
	observers *observers.Store
	stream    pb.PubSub_ChannelClient
}

func (c *Client) WithBrand(ctx context.Context, brand string) context.Context {
	return metadata.WithBrand(ctx, brand)
}

// Brand - Get brand from event
func (c *Client) Brand(ctx context.Context) string {
	return metadata.FromContext(ctx).Brand
}

// Error - Check and error in event
func (c *Client) Error(ctx context.Context) error {
	return metadata.Error(ctx)
}

// WithError - Send error with event
func (c *Client) WithError(ctx context.Context, err error) context.Context {
	return metadata.WithError(ctx, err)
}

// Pub - Publish event
func (c *Client) Pub(ctx context.Context, event string, payload []byte) *Request {
	r := NewRequest(ctx, c.callbacks, c.stream, event, payload)
	if c.stream == nil {
		c.waiters.Wait()
	}
	return r
}

// Sub - Subscribe to event
func (c *Client) Sub(ctx context.Context, event string, cb func(ctx context.Context, payload []byte)) {
	r := &Response{
		event:    event,
		callback: cb,
		handlers: c.observers,
	}
	id := r.handlers.Add(r.event, r.callback)
	go func() {
		<-ctx.Done()
		r.handlers.Delete(r.event, id)
	}()
}

func (c *Client) observe(stream pb.PubSub_ChannelClient) {
	go func() {
		defer stream.CloseSend()
	LOOP:
		for {
			msg, err := stream.Recv()
			if msg != nil {
				c.handleMessage(msg)
			}

			if err != nil {
				c.handleError(err)
				break LOOP
			}
		}
	}()

	go func() {
		c.stream = stream
		<-stream.Context().Done()
		c.stream = nil
	}()
}

func (c *Client) connect() {
	fmt.Println(fmt.Sprintf("Servise %s connecting...", c.clientId))

	cli := pb.NewPubSubClient(c.conn)
	ctx := context.Background()

	md := &auth{
		ClientId:    c.clientId,
		Brand:       c.brand,
		Publishers:  c.publishers,
		Subscribers: c.subscribers,
	}
	md = createAuth(md, c.token)

	stream, err := cli.Channel(md.WithContext(ctx))
	if err != nil {
		c.handleError(err)
		return
	}

	fmt.Println(fmt.Sprintf("Servise %s connect", c.clientId))
	c.attempts = 1
	c.observe(stream)
}

func (c *Client) handleError(err error) {
	fmt.Println("Error connection:", err)
	switch status.Code(err) {
	case codes.Unavailable:
		c.reconnect()
	default:
		c.reconnect()
	}
}

func (c *Client) handleMessage(msg *pb.Event) {
	switch msg.Name {
	case "_.hello":
		c.waiters.Done()
	case "_.event.forbidden":
		panic(fmt.Sprintf("Event forbidden: %s", msg.Payload))
	default:
		c.callbacks.Handle(msg)
		c.observers.Handle(msg)
	}
}

func (c *Client) reconnect() {
	go func() {
		if c.attempts > 0 {
			fmt.Println("Reconnect.", c.attempts, "seconds")
		}
		time.Sleep(time.Second * time.Duration(c.attempts))
		if c.attempts == 0 {
			c.attempts = 1
		} else {
			c.attempts = c.attempts * 2
			if c.attempts > 8 {
				c.attempts = 8
			}
		}
		c.connect()
	}()
}
