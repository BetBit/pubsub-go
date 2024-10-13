package pubsub

import (
	"context"
	"fmt"
	pb "github.com/BetBit/pubsub-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type observer struct {
	ClientID    string
	Addr        string
	Token       string
	Brand       string
	Publishers  []string
	Subscribers []string

	stream   pb.PubSub_ChannelClient
	conn     *grpc.ClientConn
	cancel   context.CancelFunc
	reqPool  chan *pb.Event
	pause    chan struct{}
	start    chan struct{}
	attempts int
	OnlyRoot bool
}

func (o *observer) Pub(msg *pb.Event) {
	if o.stream == nil {
		return
	}
	msg.Timestamp = time.Now().UnixMilli()
	err := o.stream.Send(msg)
	if err != nil {
		o.cancel()
	}
}

func (o *observer) observe(stream pb.PubSub_ChannelClient) {
	ctx, cancel := context.WithCancel(stream.Context())
	o.cancel = cancel
	var errMsg error
	go func() {
		defer stream.CloseSend()
	LOOP:
		for {
			msg, err := stream.Recv()
			if msg != nil {
				o.handleMessage(msg)
			}

			if err != nil {
				errMsg = err
				break LOOP
			}
		}
	}()

	go func() {
		<-ctx.Done()
		o.pause <- struct{}{}
		time.Sleep(2 * time.Second)
		o.handleError(errMsg)
	}()

	o.stream = stream

	go func() {
		o.start <- struct{}{}
		o.attempts = 0
	}()
}

func (o *observer) connect() {
	//kacp := keepalive.ClientParameters{
	//	Time:                10 * time.Minute, // Time after which the client sends a keepalive ping.
	//	Timeout:             20 * time.Second, // Time to wait for a keepalive ping ack before closing the connection.
	//	PermitWithoutStream: true,             // Allow pings even when there are no active streams.
	//}

	conn, err := grpc.Dial(
		o.Addr,
		grpc.WithInsecure(),
		//grpc.WithKeepaliveParams(kacp),
	)

	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	cli := pb.NewPubSubClient(conn)

	md := &auth{
		ClientId:    o.ClientID,
		Brand:       o.Brand,
		Publishers:  o.Publishers,
		Subscribers: o.Subscribers,
		OnlyRoot:    o.OnlyRoot,
		Address:     o.Addr,
	}
	md = createAuth(md, o.Token)

	stream, err := cli.Channel(md.WithContext(ctx))
	if err != nil {
		o.handleError(err)
		return
	}

	fmt.Println("Connected to", o.Addr)

	o.observe(stream)
}

func (o *observer) handleMessage(msg *pb.Event) {
	switch msg.Name {
	case "_.event.forbidden":
		panic(fmt.Sprintf("Event forbidden: %s", msg.Payload))
	default:
		o.reqPool <- msg
	}
}

func (o *observer) handleError(err error) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		panic(fmt.Sprintf("Invalid argument: %s", err.Error()))
	case codes.Unauthenticated:
		panic(fmt.Sprintf("Unauthenticated: %s", err.Error()))
	default:
		o.reconnect()
		return
	}
}

func (o *observer) reconnect() {
	if o.attempts > 0 {
		fmt.Println(fmt.Sprintf("Reconnect after %d seconds to: %s", o.attempts, o.Addr))
	}
	time.Sleep(time.Second * time.Duration(o.attempts))
	if o.attempts == 0 {
		o.attempts = 1
	} else {
		o.attempts = o.attempts * 2
		if o.attempts > 8 {
			o.attempts = 8
		}
	}

	o.connect()
}

func (o *observer) Init() {
	o.pause = make(chan struct{})
	o.start = make(chan struct{})
	o.connect()
}
