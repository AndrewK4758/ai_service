package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/ai_service/internal/server"
)

type mockMsg struct {
	data    []byte
	headers nats.Header
}

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) { return &jetstream.MsgMetadata{}, nil }
func (m *mockMsg) Data() []byte                              { return m.data }
func (m *mockMsg) Headers() nats.Header                      { return m.headers }
func (m *mockMsg) Subject() string                           { return "worker.ai.inference" }
func (m *mockMsg) Reply() string                             { return "" }
func (m *mockMsg) Ack() error                                { return nil }
func (m *mockMsg) DoubleAck(context.Context) error           { return nil }
func (m *mockMsg) Nak() error                                { return nil }
func (m *mockMsg) NakWithDelay(d time.Duration) error        { return nil }
func (m *mockMsg) InProgress() error                         { return nil }
func (m *mockMsg) Term() error                               { return nil }
func (m *mockMsg) TermWithReason(string) error               { return nil }

func main() {
	// Let's just run it!
}
