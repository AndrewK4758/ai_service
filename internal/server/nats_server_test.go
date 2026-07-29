package server

import (
	"os"

	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/workflowparser"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type mockAnalyzer struct {
	err error
}

func (m *mockAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	return "{}", m.err
}

func (m *mockAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
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

	t.Setenv("MODEL_ROUTE_SUBJECT", "worker.ai.inference")
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


func TestNatsServer_ResolveModelID(t *testing.T) {
	t.Setenv("MODEL_ROUTE_CHUNKING", "model-chunking-v1")
	t.Setenv("MODEL_ROUTE_INFERENCE", "model-inference-v2")
	t.Setenv("MODEL_ROUTE_DOC_SUMMARY", "model-summary-v3")
	t.Setenv("MODEL_ROUTE_DEFAULT", "model-default-v0")
	t.Setenv("MODEL_EMBEDDING", "text-embedding-3-small")
	t.Setenv("MODEL_CUSTOM_TASK", "custom-direct-model")
	t.Setenv("MODEL_ROUTE_PRECEDENCE", "route-precedence-model")
	t.Setenv("MODEL_PRECEDENCE", "direct-precedence-model")
	t.Setenv("MODEL_ROUTE_DOC_SUMMARY_V2_", "model-summary-v2-special")

	server := &NatsServer{}

	tests := []struct {
		subject  string
		expected string
	}{
		{"worker.doc.chunking", "model-chunking-v1"},
		{"worker.ai.inference", "model-inference-v2"},
		{"worker.custom.dept.doc-summary", "model-summary-v3"},
		{"worker.doc.embedding", "text-embedding-3-small"},
		{"worker.ai.embed", "text-embedding-3-small"},
	}

	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			got := server.resolveModelID(tt.subject)
			if got != tt.expected {
				t.Errorf("resolveModelID(%q) = %q; want %q", tt.subject, got, tt.expected)
			}
		})
	}


}

func TestNatsServer_processMessage_WithNativeIdentity(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, 1, "test", 1, 1)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant-123",
			AppId:    "app-456",
			JobId:    "job-789",
		},
	}
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

	if !msg.ackCalled {
		t.Errorf("expected msg with native identity to be acknowledged")
	}
}

func TestNatsServer_ConcurrencySemaphoreAndCancellation(t *testing.T) {
	workerPoolSize := 2
	sem := make(chan struct{}, workerPoolSize)
	ctx, cancel := context.WithCancel(context.Background())

	// Fill pool capacity
	sem <- struct{}{}
	sem <- struct{}{}

	nakCalled := make(chan bool, 1)
	mock := &mockMsg{}

	go func() {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			_ = mock.Nak()
			nakCalled <- true
		}
	}()

	// Give goroutine time to block on semaphore
	time.Sleep(10 * time.Millisecond)

	// Cancel context while goroutine is waiting
	cancel()

	select {
	case <-nakCalled:
		// Success: goroutine unblocked and NAK'd message
	case <-time.After(1 * time.Second):
		t.Fatalf("expected message to be NAK'd on context cancellation when semaphore is full")
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if !mock.nakCalled {
		t.Errorf("expected mock.nakCalled to be true")
	}
}



func init() {
	os.Setenv("WORKER_ACK_WAIT", "1m")
	os.Setenv("NATS_RATE_LIMIT_DELAY", "500ms")
	os.Setenv("NATS_NACK_ERROR_TIMEOUT", "1s")
	os.Setenv("NATS_REPLY_DELAY", "10ms")
	os.Setenv("NATS_RESPONSE_STREAM", "WORKFLOW_RESPONSES")
	os.Setenv("SERVICE_NAME", "ai_service")
	os.Setenv("MODEL_ROUTE_DEFAULT", "model-default-v0")
	os.Setenv("MODEL_ROUTE_SUBJECT", "worker.ai.inference")
}
