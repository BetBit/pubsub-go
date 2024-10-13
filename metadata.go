package pubsub

import (
	"context"
	"fmt"
	pb "github.com/BetBit/pubsub-go/proto"
)

type metadata struct {
	brand     string
	timestamp int64
	eventName string
	eventId   string
	error     error
}

func withBrand(ctx context.Context, brand string) context.Context {
	md := fromContext(ctx)
	if md == nil {
		md = &metadata{
			brand: brand,
		}
	} else {
		md.brand = brand
	}
	return context.WithValue(ctx, "metadata", md)
}

func withError(ctx context.Context, err error) context.Context {
	md := fromContext(ctx)
	if md == nil {
		md = &metadata{
			error: err,
		}
	} else {
		md.error = err
	}
	return context.WithValue(ctx, "metadata", md)
}

func createContext(msg *pb.Event) context.Context {
	var err error
	if msg.Error != "" {
		err = fmt.Errorf(msg.Error)
	}

	return context.WithValue(context.Background(), "metadata", &metadata{
		brand:     msg.Brand,
		timestamp: msg.Timestamp,
		eventName: msg.Name,
		eventId:   msg.Id,
		error:     err,
	})
}

func fromContext(ctx context.Context) *metadata {
	md, ok := ctx.Value("metadata").(*metadata)
	if !ok {
		return nil
	}
	return md
}

func metaError(ctx context.Context) error {
	md := fromContext(ctx)
	if md == nil {
		return nil
	}
	return md.error
}
