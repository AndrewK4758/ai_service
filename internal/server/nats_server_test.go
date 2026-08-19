package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockAnalyzer struct {
	err error
}

func (m *mockAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	return "{}", m.err
}

func (m *mockAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	return "{}", m.err
}

func (m *mockAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return []float32{}, m.err
}

type trackingAnalyzer struct {
	mu           sync.Mutex
	textCalled   bool
	visionCalled bool
	imageCalled  bool
	lastModelID  string
	lastCallType string
}

func (t *trackingAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.textCalled = true
	t.lastModelID = modelID
	t.lastCallType = "text"
	return `{"result":"text_success"}`, nil
}

func (t *trackingAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.visionCalled = true
	t.imageCalled = true
	t.lastModelID = modelID
	t.lastCallType = "image"
	return `{"result":"vision_success"}`, nil
}

func (t *trackingAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return []float32{}, nil
}

type mockStorageManager struct {
	err       error
	fileBytes []byte
}

func (m *mockStorageManager) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) (io.Reader, error) {
	if m.err != nil {
		return nil, m.err
	}
	return bytes.NewReader(m.fileBytes), nil
}

func (m *mockStorageManager) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data io.Reader) (string, error) {
	return "", m.err
}

type mockMsg struct {
	mu         sync.Mutex
	data       []byte
	subject    string
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
func (m *mockMsg) Headers() nats.Header {
	if m.headers == nil {
		m.headers = make(nats.Header)
	}
	if m.headers.Get("X-Test-No-Defaults") == "true" {
		delete(m.headers, "X-Test-No-Defaults")
		return m.headers
	}
	if m.headers.Get("X-Page-Limit") == "" {
		m.headers.Set("X-Page-Limit", "10")
	}
	if m.headers.Get("X-Tenant-Id") == "" {
		m.headers.Set("X-Tenant-Id", "t1")
	}
	return m.headers
}
func (m *mockMsg) Subject() string {
	if m.subject != "" {
		return m.subject
	}
	return "worker.ai.default.a1.t1.j1"
}
func (m *mockMsg) Reply() string { return "" }
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
	m.nakCalled = true
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

	payload, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{},
		"global_state": map[string]any{
			"functional": map[string]any{},
		},
	})
	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.ai.default",
		Payload:     payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"X-Page-Limit": []string{"10"},
			"X-Tenant-Id":  []string{"t1"},
		},
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}

	server.processMessage(context.Background(), msg)
	if !msg.nakCalled {
		t.Errorf("expected Nak to be called for rate limit error")
	}
}

func TestExtractIsScannedFlag(t *testing.T) {
	st, _ := structpb.NewStruct(map[string]any{

		"node_config": map[string]any{},
		"storage":     map[string]any{"document_uri": "internal-store://t/a/j/doc.pdf"},
		"global_state": map[string]any{
			"functional": map[string]any{
				"is_scanned": true,
			},
		},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: st,
	}
	isScanned, hasFlag, _ := extractIsScannedFlag(req)
	if !hasFlag || !isScanned {
		t.Errorf("expected true, got %v, %v", isScanned, hasFlag)
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
	t.Setenv("VISION_MODELS", "glm-ocr")

	js := &mockJS{}
	analyzer := &trackingAnalyzer{}
	mockStore := &mockStorageManager{
		fileBytes: []byte(sampleTestPDF),
	}
	server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

	st, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{},
		"storage":     map[string]any{"document_uri": "internal-store://t/a/j/doc.pdf"},
		"global_state": map[string]any{
			"functional": map[string]any{
				"is_scanned": true,
			},
		},
	})
	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.ai.default",
		Payload:     st,
		StepId:      "VISION_ROUTED_STEP",
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"X-Page-Limit": []string{"10"},
			"X-Tenant-Id":  []string{"t1"},
		},
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

	st, _ := structpb.NewStruct(map[string]any{

		"node_config": map[string]any{},
		"storage":     map[string]any{"document_uri": "internal-store://t/a/j/doc.pdf"},
		"global_state": map[string]any{
			"functional": map[string]any{
				"is_scanned": false,
			},
		},
	})
	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.ai.default",
		Payload:     st,
		StepId:      "TEXT_ROUTED_STEP",
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"X-Page-Limit": []string{"10"},
			"X-Tenant-Id":  []string{"t1"},
		},
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

	st, _ := structpb.NewStruct(map[string]any{

		"node_config": map[string]any{},
		"storage":     map[string]any{"document_uri": "internal-store://t/a/j/doc.pdf"},
		"global_state": map[string]any{
			"functional": map[string]any{},
		},
	})
	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.ai.default",
		StepId:      "FALLBACK_STEP",
		Payload:     st,
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"X-Page-Limit": []string{"10"},
			"X-Tenant-Id":  []string{"t1"},
		},
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
	expectedModelID, _ := server.resolveModelID(req.TaskSubject)
	if lastModelID != expectedModelID {
		t.Errorf("expected fallback modelID %q from resolveModelID, got %q", expectedModelID, lastModelID)
	}
}

func TestNatsServer_processMessage_InvalidIdentity_FailFast(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

	req := &pb.ExecuteTaskRequest{
		TaskSubject: "ai.task.execute",
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"X-Page-Limit": []string{"10"},
			"X-Tenant-Id":  []string{"t1"},
		},
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic for invalid identity")
		}
	}()

	server.processMessage(context.Background(), msg)
}

func TestNatsServer_processMessage_MissingTaskSubject_FailFast(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"X-Page-Limit": []string{"10"},
			"X-Tenant-Id":  []string{"t1"},
		},
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic for missing task subject")
		}
	}()

	server.processMessage(context.Background(), msg)
}

func TestNatsServer_handleMessage_PoisonPillPanic_NakWithDelay(t *testing.T) {
	js := &mockJS{}
	analyzer := &mockAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

	// Create a request missing TaskSubject which will cause processMessage to panic
	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"X-Page-Limit": []string{"10"},
			"X-Tenant-Id":  []string{"t1"},
		},
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}

	sem := make(chan struct{}, 1)

	// Set a timeout environment variable for NakWithDelay
	t.Setenv("NATS_NACK_ERROR_TIMEOUT", "10")

	// Call handleMessage directly. It should recover from the panic and call NakWithDelay.
	server.handleMessage(context.Background(), msg, sem)

	if !msg.nakCalled {
		t.Errorf("expected NakWithDelay to be called when processMessage panics")
	}
	
	if msg.nakDelay != 10*time.Second {
		t.Errorf("expected NakWithDelay to be 10s, got %v", msg.nakDelay)
	}
}

type capturingJS struct {
	jetstream.JetStream
	mu        sync.Mutex
	published []struct {
		Subject string
		Data    []byte
	}
}

func (c *capturingJS) Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.published = append(c.published, struct {
		Subject string
		Data    []byte
	}{Subject: subj, Data: data})
	return &jetstream.PubAck{}, nil
}

func TestNatsServer_VisionProgressTelemetry(t *testing.T) {
	t.Setenv("MODEL_ROUTE_VISION", "glm-ocr")
	js := &capturingJS{}
	analyzer := &trackingAnalyzer{}
	storage := &mockStorageManager{
		fileBytes: []byte("fake_png_data"),
	}
	server := NewNatsServer(nil, js, analyzer, storage, 1, "test", 1, 1, 8192)

	schema, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"result": map[string]any{"type": "string"},
		},
	})

	payload, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": schema.AsMap(),
		},
		"storage": map[string]any{
			"document_uri": "internal-store://t1/a1/j1/page.png",
		},
		"global_state": map[string]any{
			"functional": map[string]any{},
		},
	})

	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.ai.vision",
		StepId:      "STEP_VISION_TABLE",
		Payload:     payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"Reply-Subject": []string{"reply.to.client"},
		},
		metadata: &jetstream.MsgMetadata{NumDelivered: 1},
	}

	server.processMessage(context.Background(), msg)

	if !analyzer.visionCalled {
		t.Errorf("expected vision analyzer to be called")
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	foundStarted := false
	foundCompleted := false
	for _, p := range js.published {
		if p.Subject == "trace.ai.t1.a1" {
			var evt VisionTraceEvent
			if err := json.Unmarshal(p.Data, &evt); err == nil {
				if evt.EventType == "VISION_PAGE_STARTED" {
					foundStarted = true
					if evt.CurrentPage != 1 || evt.TotalPages != 1 || evt.StepID != "STEP_VISION_TABLE" {
						t.Errorf("unexpected started event: %+v", evt)
					}
				}
				if evt.EventType == "VISION_PAGE_COMPLETED" {
					foundCompleted = true
					if evt.CurrentPage != 1 || evt.TotalPages != 1 || evt.StepID != "STEP_VISION_TABLE" {
						t.Errorf("unexpected completed event: %+v", evt)
					}
				}
			}
		}
	}

	if !foundStarted {
		t.Errorf("expected VISION_PAGE_STARTED telemetry event to be published")
	}
	if !foundCompleted {
		t.Errorf("expected VISION_PAGE_COMPLETED telemetry event to be published")
	}
}

func TestNatsServer_DynamicModelOverride(t *testing.T) {
	js := &capturingJS{}
	analyzer := &trackingAnalyzer{}
	server := NewNatsServer(nil, js, analyzer, nil, 1, "test", 1, 1, 8192)

	schema, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"result": map[string]any{"type": "string"},
		},
	})

	payload, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": schema.AsMap(),
			"model_override":        "custom-vision-model-v2",
		},
		"global_state": map[string]any{
			"functional": map[string]any{},
		},
	})

	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.ai.vision",
		Payload:     payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data: data,
		headers: nats.Header{
			"Reply-Subject": []string{"reply.to.client"},
		},
		metadata: &jetstream.MsgMetadata{NumDelivered: 1},
	}

	server.processMessage(context.Background(), msg)

	if analyzer.lastModelID != "custom-vision-model-v2" {
		t.Errorf("expected model override custom-vision-model-v2, got %q", analyzer.lastModelID)
	}
}

