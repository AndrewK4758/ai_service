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

func (m *mockAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	return "{}", m.err
}

func (m *mockAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	return "{}", m.err
}

func (m *mockAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return []float32{}, m.err
}

type mockMsg struct {
	mu         sync.Mutex
	data       []byte
	headers    nats.Header
	metadata   *jetstream.MsgMetadata
	metaErr    error
	nakDelay   time.Duration
	nakCalled  bool
	termCalled bool
	ackCalled  bool
}

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.metadata, m.metaErr }
func (m *mockMsg) Data() []byte                              { return m.data }
func (m *mockMsg) Headers() nats.Header                      { return m.headers }
func (m *mockMsg) Subject() string                           { return "test.subject" }
func (m *mockMsg) Reply() string                             { return "" }
func (m *mockMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackCalled = true
	return nil
}
func (m *mockMsg) DoubleAck(context.Context) error { return nil }
func (m *mockMsg) Nak() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakCalled = true
	return nil
}
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

type failingMockJS struct {
	jetstream.JetStream
}

func (m *failingMockJS) Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, fmt.Errorf("simulated NATS publish failure")
}

func TestNatsServer_processMessage_RateLimit(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{err: fmt.Errorf("429 Too Many Requests")}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	req := &pb.ExecuteWorkflowNodeRequest{}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 2,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server.processMessage(ctx, msg)

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

	req := &pb.ExecuteWorkflowNodeRequest{}
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

func TestNatsServer_processMessage_PoisonPill_SendErrorReplyFails(t *testing.T) {
	js := &failingMockJS{}
	analyzer := &mockAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.subject")

	// Invalid protobuf with reply subject header where publish will fail
	msg := &mockMsg{
		data:    []byte("corrupt protobuf payload"),
		headers: hdr,
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
		t.Errorf("expected poison pill msg to be terminated via msg.Term() even when sendErrorReply fails")
	}
	if msg.nakDelay != 0 {
		t.Errorf("expected no NakWithDelay on poison pill unmarshal failure, got %v", msg.nakDelay)
	}
}

func TestNatsServer_processMessage_NonRetryableAIError(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{err: fmt.Errorf("400 Bad Request: invalid parameter count")}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	req := &pb.ExecuteWorkflowNodeRequest{}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
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
		t.Errorf("expected non-retryable AI error msg to be terminated via msg.Term()")
	}
	if msg.nakDelay != 0 {
		t.Errorf("expected no NakWithDelay on non-retryable AI error, got %v", msg.nakDelay)
	}
}

func TestNatsServer_processMessage_NonRetryableAIError_SendErrorReplyFails(t *testing.T) {
	js := &failingMockJS{}
	analyzer := &mockAnalyzer{err: fmt.Errorf("400 Bad Request: invalid prompt")}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.subject")

	req := &pb.ExecuteWorkflowNodeRequest{}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data:    data,
		headers: hdr,
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
		t.Errorf("expected non-retryable AI error msg to be terminated via msg.Term() even when sendErrorReply fails")
	}
	if msg.nakDelay != 0 {
		t.Errorf("expected no NakWithDelay on non-retryable error when reply fails, got %v", msg.nakDelay)
	}
}

func TestNatsServer_processMessage_ContextCanceled(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	req := &pb.ExecuteWorkflowNodeRequest{}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel context prior to processing

	server.processMessage(ctx, msg)

	msg.mu.Lock()
	defer msg.mu.Unlock()

	if !msg.nakCalled {
		t.Errorf("expected msg to be Nak'd when context is canceled")
	}
	if msg.termCalled {
		t.Errorf("expected msg NOT to be terminated when context is canceled")
	}
}

func TestNatsServer_GetMaxAckPending(t *testing.T) {
	t.Run("default fallback", func(t *testing.T) {
		got := getMaxAckPending(5)
		if got != 5 {
			t.Errorf("getMaxAckPending(5) = %d; want 5", got)
		}
	})

	t.Run("MAX_ACK_PENDING env set", func(t *testing.T) {
		t.Setenv("MAX_ACK_PENDING", "20")
		got := getMaxAckPending(5)
		if got != 20 {
			t.Errorf("getMaxAckPending(5) with MAX_ACK_PENDING=20 = %d; want 20", got)
		}
	})

	t.Run("NATS_MAX_ACK_PENDING env set", func(t *testing.T) {
		t.Setenv("NATS_MAX_ACK_PENDING", "15")
		got := getMaxAckPending(5)
		if got != 15 {
			t.Errorf("getMaxAckPending(5) with NATS_MAX_ACK_PENDING=15 = %d; want 15", got)
		}
	})
}

func TestNatsServer_ResolveModelID(t *testing.T) {
	t.Setenv("MODEL_ROUTE_CHUNKING", "model-chunking-v1")
	t.Setenv("MODEL_ROUTE_INFERENCE", "model-inference-v2")
	t.Setenv("MODEL_ROUTE_DOC_SUMMARY", "model-summary-v3")
	t.Setenv("MODEL_ROUTE_DEFAULT", "model-default-v0")
	t.Setenv("MODEL_EMBEDDING", "text-embedding-3-small")
	t.Setenv("MODEL_CUSTOM_TASK", "custom-direct-model")

	server := &NatsServer{}

	tests := []struct {
		subject  string
		expected string
	}{
		{"worker.doc.chunking", "model-chunking-v1"},
		{"worker.ai.inference", "model-inference-v2"},
		{"worker.custom.dept.doc-summary", "model-summary-v3"}, // tests hyphen normalization
		{"worker.unknown.action", "model-default-v0"},          // tests default fallback
		{"", "model-default-v0"},                               // tests empty subject
		{"   ", "model-default-v0"},                            // tests whitespace subject
		{"worker.doc.chunking.", "model-chunking-v1"},          // tests trailing dot
		{"worker.doc.embedding", "text-embedding-3-small"},     // tests embedding token resolution
		{"worker.ai.embed", "text-embedding-3-small"},          // tests embed token resolution
		{"worker.ai.custom_task", "custom-direct-model"},       // tests direct MODEL_CUSTOM_TASK fallback
	}

	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			got := server.resolveModelID(tt.subject)
			if got != tt.expected {
				t.Errorf("resolveModelID(%q) = %q; want %q", tt.subject, got, tt.expected)
			}
		})
	}

	t.Run("No default set returns empty string", func(t *testing.T) {
		t.Setenv("MODEL_ROUTE_DEFAULT", "")
		got := server.resolveModelID("worker.nonexistent.action")
		if got != "" {
			t.Errorf("resolveModelID(\"worker.nonexistent.action\") without default = %q; want \"\"", got)
		}
	})
}

