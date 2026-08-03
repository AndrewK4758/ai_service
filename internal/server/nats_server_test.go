package server

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/workflowparser"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

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

	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case <-nakCalled:
	case <-time.After(1 * time.Second):
		t.Fatalf("expected message to be NAK'd on context cancellation when semaphore is full")
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if !mock.nakCalled {
		t.Errorf("expected mock.nakCalled to be true")
	}
}

// --- JIT FETCH AND RESILIENCY TESTS ---

type mockStorageManager struct {
	fileBytes []byte
	err       error
	called    bool
	lastUri   string
}

func (m *mockStorageManager) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) ([]byte, error) {
	m.called = true
	m.lastUri = relativePath
	if m.err != nil {
		return nil, m.err
	}
	return m.fileBytes, nil
}

func (m *mockStorageManager) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data []byte) (string, error) {
	return relativePath, nil
}

type trackingAnalyzer struct {
	mu           sync.Mutex
	textCalled   bool
	imageCalled  bool
	lastText     string
	lastImages   [][]byte
	lastModelID  string
	lastCallType string
	err          error
}

func (m *trackingAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.textCalled = true
	m.lastText = text
	m.lastModelID = modelID
	m.lastCallType = "text"
	return `{"result":"success"}`, m.err
}

func (m *trackingAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.imageCalled = true
	m.lastImages = imageBytes
	m.lastModelID = modelID
	m.lastCallType = "image"
	return `{"result":"success"}`, m.err
}

func (m *trackingAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastModelID = modelID
	m.lastCallType = "embedding"
	return []float32{}, m.err
}

func TestNatsServer_JITFetch_PdfTextExtraction_Success(t *testing.T) {
	js := &mockJS{}
	analyzer := &trackingAnalyzer{}
	mockStore := &mockStorageManager{
		fileBytes: []byte("sample text file content"),
	}
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/document.txt",
		},
		StepId: "TEXT_PARSE_STEP",
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

	if !mockStore.called {
		t.Errorf("expected storage manager FetchFile to be called")
	}
	if mockStore.lastUri != "internal-store://t1/a1/j1/document.txt" {
		t.Errorf("expected URI internal-store://t1/a1/j1/document.txt, got %s", mockStore.lastUri)
	}
	if !analyzer.textCalled {
		t.Errorf("expected AnalyzeText to be called for text file JIT fetch")
	}
	if analyzer.lastText != "sample text file content" {
		t.Errorf("expected text 'sample text file content', got '%s'", analyzer.lastText)
	}
}

func TestNatsServer_JITFetch_RawImage_Success(t *testing.T) {
	js := &mockJS{}
	analyzer := &trackingAnalyzer{}
	mockStore := &mockStorageManager{
		fileBytes: []byte("fake_image_bytes"),
	}
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/photo.png",
		},
		StepId: "VISION_STEP",
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

	if !mockStore.called {
		t.Errorf("expected storage manager FetchFile to be called")
	}
	if !analyzer.imageCalled {
		t.Errorf("expected AnalyzeImage to be called for PNG image")
	}
	if len(analyzer.lastImages) != 1 || string(analyzer.lastImages[0]) != "fake_image_bytes" {
		t.Errorf("expected image bytes 'fake_image_bytes', got %v", analyzer.lastImages)
	}
}

type capturingJS struct {
	jetstream.JetStream
	lastPublishedData []byte
}

func (c *capturingJS) Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	c.lastPublishedData = data
	return &jetstream.PubAck{}, nil
}

func TestNatsServer_JITFetch_StorageGatewayDown_EnrichedError(t *testing.T) {
	js := &capturingJS{}
	analyzer := &mockAnalyzer{}
	mockStore := &mockStorageManager{
		err: fmt.Errorf("connection refused"),
	}
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.subject")

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/document.pdf",
		},
		StepId: "FETCH_STEP",
	}
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

	if js.lastPublishedData == nil {
		t.Fatalf("expected error reply to be published")
	}

	resp := &pb.PerformActionResponse{}
	if err := proto.Unmarshal(js.lastPublishedData, resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Success {
		t.Errorf("expected response success to be false")
	}
	if resp.ErrorContract == nil {
		t.Fatalf("expected ErrorContract to be populated")
	}
	if resp.ErrorContract.Code != "STORAGE_FETCH_FAILED" {
		t.Errorf("expected error code STORAGE_FETCH_FAILED, got %s", resp.ErrorContract.Code)
	}
	expectedMsgPrefix := "[Step: FETCH_STEP, URI: internal-store://t1/a1/j1/document.pdf]"
	if !strings.HasPrefix(resp.ErrorContract.Message, expectedMsgPrefix) {
		t.Errorf("expected error message to start with %q, got %q", expectedMsgPrefix, resp.ErrorContract.Message)
	}
}

func TestNatsServer_ErrorContract_ContextFormatting_TableDriven(t *testing.T) {
	tests := []struct {
		name               string
		req                *pb.ExecuteWorkflowNodeRequest
		storeErr           error
		analyzerErr        error
		expectedCode       string
		expectedStepInMsg  string
		expectedUriInMsg   string
	}{
		{
			name: "Storage fetch failure",
			req: &pb.ExecuteWorkflowNodeRequest{
				Storage: &pb.StorageContract{DocumentUri: "internal-store://doc.pdf"},
				StepId:          "STEP_FETCH",
			},
			storeErr:          fmt.Errorf("storage offline"),
			expectedCode:      "STORAGE_FETCH_FAILED",
			expectedStepInMsg: "STEP_FETCH",
			expectedUriInMsg:  "internal-store://doc.pdf",
		},
		{
			name: "AI analysis failure with URI",
			req: &pb.ExecuteWorkflowNodeRequest{
				Storage: &pb.StorageContract{DocumentUri: "internal-store://doc.txt"},
				StepId:          "STEP_AI",
			},
			analyzerErr:       fmt.Errorf("model error"),
			expectedCode:      "AI_PROCESSING_FAILED",
			expectedStepInMsg: "STEP_AI",
			expectedUriInMsg:  "internal-store://doc.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := &capturingJS{}
			analyzer := &mockAnalyzer{err: tt.analyzerErr}
			mockStore := &mockStorageManager{err: tt.storeErr, fileBytes: []byte("sample")}
			server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

			hdr := make(nats.Header)
			hdr.Set("Reply-Subject", "test.reply.subject")

			data, _ := proto.Marshal(tt.req)
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

			if js.lastPublishedData == nil {
				t.Fatalf("expected error reply to be published")
			}

			resp := &pb.PerformActionResponse{}
			if err := proto.Unmarshal(js.lastPublishedData, resp); err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}

			if resp.ErrorContract == nil {
				t.Fatalf("expected ErrorContract to be present")
			}
			if resp.ErrorContract.Code != tt.expectedCode {
				t.Errorf("expected error code %s, got %s", tt.expectedCode, resp.ErrorContract.Code)
			}
			expectedSub := fmt.Sprintf("[Step: %s, URI: %s]", tt.expectedStepInMsg, tt.expectedUriInMsg)
			if !strings.Contains(resp.ErrorContract.Message, expectedSub) {
				t.Errorf("expected error message to contain %q, got %q", expectedSub, resp.ErrorContract.Message)
			}
		})
	}
}

func TestEstimateDocumentTokens(t *testing.T) {
	tests := []struct {
		name           string
		text           string
		images         [][]byte
		expectedTokens int
	}{
		{
			name:           "empty input",
			text:           "",
			images:         nil,
			expectedTokens: 0,
		},
		{
			name:           "short text",
			text:           "hello world",
			images:         nil,
			expectedTokens: 3,
		},
		{
			name:           "exact 4 char text",
			text:           "1234",
			images:         nil,
			expectedTokens: 1,
		},
		{
			name:           "vision images",
			text:           "",
			images:         [][]byte{[]byte("img1"), []byte("img2")},
			expectedTokens: 1530,
		},
		{
			name:           "combined text and images",
			text:           "1234",
			images:         [][]byte{[]byte("img1")},
			expectedTokens: 766,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EstimateDocumentTokens(tt.text, tt.images)
			if got != tt.expectedTokens {
				t.Errorf("EstimateDocumentTokens(%q, %d images) = %d; want %d", tt.text, len(tt.images), got, tt.expectedTokens)
			}
		})
	}
}

func TestNatsServer_LargeDocumentGuard_Exceeded(t *testing.T) {
	js := &capturingJS{}
	analyzer := &trackingAnalyzer{}
	largeText := strings.Repeat("A", 500)
	mockStore := &mockStorageManager{
		fileBytes: []byte(largeText),
	}
	maxTokensLimit := 100
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, maxTokensLimit)

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.subject")

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/large_doc.txt",
		},
		StepId: "PROCESS_STEP",
	}
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
	termCalled := msg.termCalled
	msg.mu.Unlock()

	if !termCalled {
		t.Errorf("expected message to be terminated via msg.Term() when document token count exceeds limit")
	}

	if analyzer.textCalled {
		t.Errorf("expected AI analyzer NOT to be invoked when document exceeds token limit")
	}

	if js.lastPublishedData == nil {
		t.Fatalf("expected error reply to be published")
	}

	resp := &pb.PerformActionResponse{}
	if err := proto.Unmarshal(js.lastPublishedData, resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Success {
		t.Errorf("expected response success to be false")
	}
	if resp.ErrorContract == nil {
		t.Fatalf("expected ErrorContract to be populated")
	}
	if resp.ErrorContract.Code != "LARGE_DOCUMENT_EXCEEDED" {
		t.Errorf("expected error code LARGE_DOCUMENT_EXCEEDED, got %s", resp.ErrorContract.Code)
	}

	expectedSub := "[Step: PROCESS_STEP, URI: internal-store://t1/a1/j1/large_doc.txt] document token count (125) exceeds max token limit (100)"
	if resp.ErrorContract.Message != expectedSub {
		t.Errorf("expected error message %q, got %q", expectedSub, resp.ErrorContract.Message)
	}
}

func TestNatsServer_LargeDocumentGuard_WithinLimit(t *testing.T) {
	js := &capturingJS{}
	analyzer := &trackingAnalyzer{}
	smallText := "This is a small document text under 100."
	mockStore := &mockStorageManager{
		fileBytes: []byte(smallText),
	}
	maxTokensLimit := 100
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, maxTokensLimit)

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.subject")

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/small_doc.txt",
		},
		StepId: "PROCESS_STEP",
	}
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

	if !analyzer.textCalled {
		t.Errorf("expected AI analyzer to be invoked when document is within token limit")
	}

	msg.mu.Lock()
	ackCalled := msg.ackCalled
	msg.mu.Unlock()

	if !ackCalled {
		t.Errorf("expected message to be acked when execution succeeds within token limit")
	}
}

func TestNatsServer_LargeDocumentGuard_VisionExceeded(t *testing.T) {
	js := &capturingJS{}
	analyzer := &trackingAnalyzer{}
	mockStore := &mockStorageManager{
		fileBytes: []byte("raw_image_data"),
	}
	maxTokensLimit := 500
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, maxTokensLimit)

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.subject")

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/photo.jpg",
		},
		StepId: "VISION_STEP",
	}
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

	if analyzer.imageCalled {
		t.Errorf("expected vision analyzer NOT to be invoked when image token count exceeds limit")
	}

	if js.lastPublishedData == nil {
		t.Fatalf("expected error reply to be published")
	}

	resp := &pb.PerformActionResponse{}
	if err := proto.Unmarshal(js.lastPublishedData, resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.ErrorContract == nil || resp.ErrorContract.Code != "LARGE_DOCUMENT_EXCEEDED" {
		t.Fatalf("expected ErrorContract code LARGE_DOCUMENT_EXCEEDED, got %v", resp.ErrorContract)
	}

	expectedSub := "[Step: VISION_STEP, URI: internal-store://t1/a1/j1/photo.jpg] document token count (765) exceeds max token limit (500)"
	if resp.ErrorContract.Message != expectedSub {
		t.Errorf("expected error message %q, got %q", expectedSub, resp.ErrorContract.Message)
	}
}

func TestNatsServer_LargeDocumentGuard_ExactBoundary(t *testing.T) {
	// 400 characters = (400 + 3) / 4 = 100 tokens.
	exactText := strings.Repeat("A", 400)
	mockStore := &mockStorageManager{
		fileBytes: []byte(exactText),
	}
	maxTokensLimit := 100

	// Case 1: Exact boundary (100 tokens == 100 limit) -> SHOULD PASS
	{
		js := &capturingJS{}
		analyzer := &trackingAnalyzer{}
		server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, maxTokensLimit)
		hdr := make(nats.Header)
		hdr.Set("Reply-Subject", "test.reply.subject")
		req := &pb.ExecuteWorkflowNodeRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
			Storage:  &pb.StorageContract{DocumentUri: "internal-store://t1/a1/j1/exact_doc.txt"},
			StepId:   "EXACT_STEP",
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{data: data, headers: hdr, metadata: &jetstream.MsgMetadata{NumDelivered: 1}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		server.processMessage(ctx, msg)

		if !analyzer.textCalled {
			t.Errorf("expected analyzer to be called on exact boundary (100 tokens == 100 limit)")
		}
	}

	// Case 2: One character over boundary (401 chars = 101 tokens > 100 limit) -> SHOULD REJECT
	{
		js := &capturingJS{}
		analyzer := &trackingAnalyzer{}
		oversizedStore := &mockStorageManager{fileBytes: []byte(strings.Repeat("A", 401))}
		server := NewNatsServer(nil, js, analyzer, oversizedStore, 1, "test", 1, 1, maxTokensLimit)
		hdr := make(nats.Header)
		hdr.Set("Reply-Subject", "test.reply.subject")
		req := &pb.ExecuteWorkflowNodeRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
			Storage:  &pb.StorageContract{DocumentUri: "internal-store://t1/a1/j1/over_doc.txt"},
			StepId:   "OVER_STEP",
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{data: data, headers: hdr, metadata: &jetstream.MsgMetadata{NumDelivered: 1}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		server.processMessage(ctx, msg)

		if analyzer.textCalled {
			t.Errorf("expected analyzer NOT to be called when token count (101) exceeds limit (100)")
		}
		if js.lastPublishedData == nil {
			t.Fatalf("expected error reply when 101 tokens exceeds 100 limit")
		}
		resp := &pb.PerformActionResponse{}
		_ = proto.Unmarshal(js.lastPublishedData, resp)
		if resp.ErrorContract == nil || resp.ErrorContract.Code != "LARGE_DOCUMENT_EXCEEDED" {
			t.Fatalf("expected LARGE_DOCUMENT_EXCEEDED error code, got %v", resp.ErrorContract)
		}
		expectedErr := "[Step: OVER_STEP, URI: internal-store://t1/a1/j1/over_doc.txt] document token count (101) exceeds max token limit (100)"
		if resp.ErrorContract.Message != expectedErr {
			t.Errorf("expected error message %q, got %q", expectedErr, resp.ErrorContract.Message)
		}
	}
}

func TestNatsServer_LargeDocumentGuard_ZeroLimitBypass(t *testing.T) {
	// Large text (125 tokens), but maxDocumentTokens = 0 -> Guard disabled/bypassed
	js := &capturingJS{}
	analyzer := &trackingAnalyzer{}
	largeText := strings.Repeat("A", 500)
	mockStore := &mockStorageManager{
		fileBytes: []byte(largeText),
	}

	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 0) // Limit = 0

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.subject")

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		Storage:  &pb.StorageContract{DocumentUri: "internal-store://t1/a1/j1/bypass_doc.txt"},
		StepId:   "BYPASS_STEP",
	}
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

	if !analyzer.textCalled {
		t.Errorf("expected AI analyzer to be invoked when maxDocumentTokens is 0 (bypass mode)")
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
	os.Setenv("OPENAI_VISION_MODEL", "glm-ocr")
	os.Setenv("OPENAI_TEXT_MODEL", "gemma4-extended")
}

func TestExtractIsScannedFlag(t *testing.T) {
	// 1. Nil parser
	{
		isScanned, hasFlag := extractIsScannedFlag(nil)
		if hasFlag || isScanned {
			t.Errorf("expected false, false for nil parser, got %v, %v", isScanned, hasFlag)
		}
	}

	// 2. req.Metadata with "IsScanned": "true"
	{
		req := &pb.ExecuteWorkflowNodeRequest{
			Metadata: map[string]string{"IsScanned": "true"},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if !hasFlag || !isScanned {
			t.Errorf("expected true, true for Metadata IsScanned=true, got %v, %v", isScanned, hasFlag)
		}
	}

	// 3. req.Metadata with "is_scanned": "false"
	{
		req := &pb.ExecuteWorkflowNodeRequest{
			Metadata: map[string]string{"is_scanned": "false"},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if !hasFlag || isScanned {
			t.Errorf("expected false, true for Metadata is_scanned=false, got %v, %v", isScanned, hasFlag)
		}
	}

	// 4. GlobalState.Functional with "IsScanned": true (bool)
	{
		funcStruct, _ := structpb.NewStruct(map[string]any{"IsScanned": true})
		req := &pb.ExecuteWorkflowNodeRequest{
			GlobalState: &pb.GlobalState{Functional: funcStruct},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if !hasFlag || !isScanned {
			t.Errorf("expected true, true for Functional IsScanned=true, got %v, %v", isScanned, hasFlag)
		}
	}

	// 5. GlobalState.Functional with "is_scanned": "false" (string)
	{
		funcStruct, _ := structpb.NewStruct(map[string]any{"is_scanned": "false"})
		req := &pb.ExecuteWorkflowNodeRequest{
			GlobalState: &pb.GlobalState{Functional: funcStruct},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if !hasFlag || isScanned {
			t.Errorf("expected false, true for Functional is_scanned=false, got %v, %v", isScanned, hasFlag)
		}
	}

	// 6. GlobalState.NonFunctional with "IsScanned": true (bool)
	{
		nonFuncStruct, _ := structpb.NewStruct(map[string]any{"IsScanned": true})
		req := &pb.ExecuteWorkflowNodeRequest{
			GlobalState: &pb.GlobalState{NonFunctional: nonFuncStruct},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if !hasFlag || !isScanned {
			t.Errorf("expected true, true for NonFunctional IsScanned=true, got %v, %v", isScanned, hasFlag)
		}
	}

	// 7. GlobalState.NonFunctional with "is_scanned": "false" (string)
	{
		nonFuncStruct, _ := structpb.NewStruct(map[string]any{"is_scanned": "false"})
		req := &pb.ExecuteWorkflowNodeRequest{
			GlobalState: &pb.GlobalState{NonFunctional: nonFuncStruct},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if !hasFlag || isScanned {
			t.Errorf("expected false, true for NonFunctional is_scanned=false, got %v, %v", isScanned, hasFlag)
		}
	}

	// 8. Invalid string value "not_a_bool"
	{
		req := &pb.ExecuteWorkflowNodeRequest{
			Metadata: map[string]string{"IsScanned": "not_a_bool"},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if hasFlag || isScanned {
			t.Errorf("expected false, false for invalid string IsScanned, got %v, %v", isScanned, hasFlag)
		}
	}

	// 9. No flag present anywhere
	{
		req := &pb.ExecuteWorkflowNodeRequest{
			Metadata: map[string]string{"OtherKey": "val"},
		}
		parser := workflowparser.NewNodeRequestParser(req)
		isScanned, hasFlag := extractIsScannedFlag(parser)
		if hasFlag || isScanned {
			t.Errorf("expected false, false when IsScanned flag is absent, got %v, %v", isScanned, hasFlag)
		}
	}
}

const sampleTestPDF = `%PDF-1.4
1 0 obj <</Type /Catalog /Pages 2 0 R>> endobj
2 0 obj <</Type /Pages /Count 1 /Kids [3 0 R]>> endobj
3 0 obj <</Type /Page /Parent 2 0 R /Resources <</Font <</F1 <</Type /Font /Subtype /Type1 /BaseFont /Helvetica>>>>>> /MediaBox [0 0 612 792] /Contents 4 0 R>> endobj
4 0 obj <</Length 55>> stream
BT
/F1 12 Tf
100 700 Td
(Scanned PDF Test) Tj
ET
endstream
endobj
xref
0 5
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000266 00000 n 
trailer <</Size 5 /Root 1 0 R>>
startxref
361
%%EOF`

func TestNatsServer_ModelRoutingOverride_ScannedPDF(t *testing.T) {
	t.Setenv("OPENAI_VISION_MODEL", "glm-ocr")
	t.Setenv("OPENAI_TEXT_MODEL", "gemma4-extended")

	js := &mockJS{}
	analyzer := &trackingAnalyzer{}
	mockStore := &mockStorageManager{
		fileBytes: []byte(sampleTestPDF),
	}
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/document.pdf",
		},
		Metadata: map[string]string{
			"IsScanned": "true",
		},
		StepId: "VISION_ROUTED_STEP",
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

	analyzer.mu.Lock()
	imageCalled := analyzer.imageCalled
	lastModelID := analyzer.lastModelID
	lastCallType := analyzer.lastCallType
	analyzer.mu.Unlock()

	if !imageCalled {
		t.Errorf("expected AnalyzeImage to be called for scanned PDF, call type was %q", lastCallType)
	}
	if lastModelID != "glm-ocr" {
		t.Errorf("expected modelID OPENAI_VISION_MODEL 'glm-ocr', got %q", lastModelID)
	}
}

func TestNatsServer_ModelRoutingOverride_StructuredTextPDF(t *testing.T) {
	t.Setenv("OPENAI_VISION_MODEL", "glm-ocr")
	t.Setenv("OPENAI_TEXT_MODEL", "gemma4-extended")

	js := &mockJS{}
	analyzer := &trackingAnalyzer{}
	mockStore := &mockStorageManager{
		fileBytes: []byte(sampleTestPDF),
	}
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/document.pdf",
		},
		Metadata: map[string]string{
			"IsScanned": "false",
		},
		StepId: "TEXT_ROUTED_STEP",
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

	analyzer.mu.Lock()
	textCalled := analyzer.textCalled
	lastModelID := analyzer.lastModelID
	lastCallType := analyzer.lastCallType
	analyzer.mu.Unlock()

	if !textCalled {
		t.Errorf("expected AnalyzeText to be called for structured text PDF, call type was %q", lastCallType)
	}
	if lastModelID != "gemma4-extended" {
		t.Errorf("expected modelID OPENAI_TEXT_MODEL 'gemma4-extended', got %q", lastModelID)
	}
}

func TestNatsServer_ModelRoutingOverride_HasFlagFalse_Fallback(t *testing.T) {
	t.Setenv("MODEL_ROUTE_DEFAULT", "model-default-v0")
	t.Setenv("OPENAI_VISION_MODEL", "glm-ocr")
	t.Setenv("OPENAI_TEXT_MODEL", "gemma4-extended")

	js := &mockJS{}
	analyzer := &trackingAnalyzer{}
	mockStore := &mockStorageManager{
		fileBytes: []byte(sampleTestPDF),
	}
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "j1",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://t1/a1/j1/document.pdf",
		},
		StepId: "FALLBACK_STEP",
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

	analyzer.mu.Lock()
	textCalled := analyzer.textCalled
	lastModelID := analyzer.lastModelID
	analyzer.mu.Unlock()

	if !textCalled {
		t.Errorf("expected AnalyzeText to be called on fallback for standard PDF")
	}
	expectedModelID := server.resolveModelID(msg.Subject())
	if lastModelID != expectedModelID {
		t.Errorf("expected fallback modelID %q from resolveModelID, got %q", expectedModelID, lastModelID)
	}
}
