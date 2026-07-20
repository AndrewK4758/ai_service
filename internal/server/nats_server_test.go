package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type mockAnalyzer struct {
	err error
}

func (m *mockAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
	return "{}", m.err
}

func (m *mockAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
	return "{}", m.err
}

type mockMsg struct {
	mu           sync.Mutex
	data         []byte
	metadata     *jetstream.MsgMetadata
	metaErr      error
	nakDelay     time.Duration
	termCalled   bool
	ackCalled    bool
}

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.metadata, m.metaErr }
func (m *mockMsg) Data() []byte                              { return m.data }
func (m *mockMsg) Headers() nats.Header                      { return nil }
func (m *mockMsg) Subject() string                           { return "test.subject" }
func (m *mockMsg) Reply() string                             { return "" }
func (m *mockMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackCalled = true
	return nil
}
func (m *mockMsg) DoubleAck(context.Context) error { return nil }
func (m *mockMsg) Nak() error                      { return nil }
func (m *mockMsg) NakWithDelay(d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakDelay = d
	return nil
}
func (m *mockMsg) InProgress() error {
	return nil
}
func (m *mockMsg) Term() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.termCalled = true
	return nil
}
func (m *mockMsg) TermWithReason(string) error { return m.Term() }

type mockJS struct {
	jetstream.JetStream
}

func (m *mockJS) Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return &jetstream.PubAck{}, nil
}

func TestNatsServer_processMessage_RateLimit(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{err: fmt.Errorf("429 Too Many Requests")}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	req := &pb.PerformActionRequest{
		InputText: "test data",
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 2,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// processMessage blocks until it handles the message (and starts a keep-alive goroutine)
	server.processMessage(ctx, msg)

	// Since we mock InProgress with no sleep, we just need to check the sync calls.
	msg.mu.Lock()
	defer msg.mu.Unlock()

	if msg.termCalled {
		t.Errorf("expected msg not to be terminated")
	}
	if msg.ackCalled {
		t.Errorf("expected msg not to be acked")
	}
	if msg.nakDelay == 0 {
		t.Errorf("expected msg to be nak'd with delay")
	}
}

func TestNatsServer_processMessage_MaxRetries(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{err: fmt.Errorf("429 Too Many Requests")}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	req := &pb.PerformActionRequest{
		InputText: "test data",
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 6, // Over the limit of 5
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server.processMessage(ctx, msg)

	msg.mu.Lock()
	defer msg.mu.Unlock()

	if !msg.termCalled {
		t.Errorf("expected msg to be terminated due to max retries")
	}
}

func TestNatsServer_processMessage_PoisonPill(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	// Invalid protobuf
	msg := &mockMsg{
		data: []byte("invalid data"),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server.processMessage(ctx, msg)

	msg.mu.Lock()
	defer msg.mu.Unlock()

	if !msg.termCalled {
		t.Errorf("expected msg to be terminated due to unparseable data")
	}
}
