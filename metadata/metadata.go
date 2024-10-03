package metadata

import (
	"context"
	"fmt"
	pb "github.com/BetBit/pubsub-go/proto"
)

type Metadata struct {
	Brand     string
	Timestamp int64
	EventName string
	EventId   string
	Error     error
}

func WithError(ctx context.Context, err error) context.Context {
	md := FromContext(ctx)
	if md == nil {
		md = &Metadata{
			Error: err,
		}
	} else {
		md.Error = err
	}
	return context.WithValue(ctx, "metadata", md)
}

func CreateContext(msg *pb.Event) context.Context {
	var err error
	if msg.Error != "" {
		err = fmt.Errorf(msg.Error)
	}

	return context.WithValue(context.Background(), "metadata", &Metadata{
		Brand:     msg.Brand,
		Timestamp: msg.Timestamp,
		EventName: msg.Name,
		EventId:   msg.Id,
		Error:     err,
	})
}

func FromContext(ctx context.Context) *Metadata {
	md, ok := ctx.Value("metadata").(*Metadata)
	if !ok {
		return nil
	}
	return md
}

func Error(ctx context.Context) error {
	md := FromContext(ctx)
	if md == nil {
		return nil
	}
	return md.Error
}
