package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	stream, err := js.Stream(ctx, "AI_ACTIONS")
	if err != nil {
		log.Fatalf("Error getting AI stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("AI_ACTIONS Stream: Messages=%d\n", info.State.Msgs)
	
	cons, err := stream.Consumer(ctx, "ai_service_workers")
	if err != nil {
		fmt.Println("No ai_service_workers consumer")
	} else {
		cinfo, _ := cons.Info(ctx)
		fmt.Printf("Consumer Pending=%d AckPending=%d\n", cinfo.NumPending, cinfo.NumAckPending)
	}
}
