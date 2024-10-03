# PubSub client 
## Example
```go
package main

import (
	"github.com/BetBit/pubsub-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial(
            "broker-host", 
            grpc.WithTransportCredentials(
                insecure.NewCredentials(),
            ),
        )
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	
	ps := pubsub.New(pubsub.Options{
		Connect:     conn,
		Token:       "token",
		ID:          "client-id",
		Brand:       "brand-id",
		Subscribers: []string{}, // Welcome interface
		Publishers:  []string{}, // Welcome interface
	})
}
```