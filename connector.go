package pubsub

import (
	pb "event-source-platform/office/pkg/pubsub/proto"
	"sync"
)

type OptionsConnector struct {
	Token       string
	Brand       string
	Address     []string
	Publishers  []string
	Subscribers []string
	ID          string
	ReqPool     chan *pb.Event
}

func NewConnector(opt OptionsConnector) *connector {
	var clients []*observer
	for _, addr := range opt.Address {
		cli := &observer{
			ClientID:    opt.ID,
			Addr:        addr,
			Token:       opt.Token,
			Brand:       opt.Brand,
			Publishers:  opt.Publishers,
			Subscribers: opt.Subscribers,
			reqPool:     opt.ReqPool,
		}

		cli.Init()
		clients = append(clients, cli)
	}

	c := &connector{
		clients: clients,
		wait:    make(chan struct{}, 1),
		pool:    make(chan *pb.Event, 1),
	}

	c.observe()

	return c
}

type connector struct {
	index   int
	wait    chan struct{}
	rw      sync.RWMutex
	clients []*observer
	pool    chan *pb.Event
}

func (c *connector) Pub(msg *pb.Event) {
	c.pool <- msg
}

func (c *connector) observe() {
	for _, cli := range c.clients {
		go func() {
			cli.pause <- struct{}{}
		}()

		go func(cli *observer) {
			for {
				select {
				case <-cli.pause:
					<-cli.start
				case msg := <-c.pool:
					cli.Pub(msg)
				}
			}
		}(cli)
	}
}
