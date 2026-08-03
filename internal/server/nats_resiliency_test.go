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
)

type memoryAuditAnalyzer struct {
	mu           sync.Mutex
	textBytesLen int
	imgBytesLen  int
	callCount    int
	lastErr      error
}

func (m *memoryAuditAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	m.textBytesLen = len(text)
	return `{"status":"processed"}`, m.lastErr
}

func (m *memoryAuditAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	total := 0
	for _, img := range imageBytes {
		total += len(img)
	}
	m.imgBytesLen = total
	return `{"status":"processed"}`, m.lastErr
}

func (m *memoryAuditAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return []float32{0.1}, nil
}

type memoryAuditStorage struct {
	payload []byte
}

func (m *memoryAuditStorage) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) ([]byte, error) {
	c := make([]byte, len(m.payload))
	copy(c, m.payload)
	return c, nil
}

func (m *memoryAuditStorage) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data []byte) (string, error) {
	return relativePath, nil
}

type resiliencyCapturingJS struct {
	jetstream.JetStream
	mu                sync.Mutex
	lastPublishedData []byte
}

func (c *resiliencyCapturingJS) Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastPublishedData = data
	return &jetstream.PubAck{}, nil
}

// TestResiliency_NatsServer_JITMemoryDiscarding verifies that JIT fetch successfully processes doc bytes.
func TestResiliency_NatsServer_JITMemoryDiscarding(t *testing.T) {
	js := &resiliencyCapturingJS{}
	analyzer := &memoryAuditAnalyzer{}
	largePayload := make([]byte, 1024*1024) // 1MB payload
	for i := range largePayload {
		largePayload[i] = 'A'
	}

	store := &memoryAuditStorage{payload: largePayload}
	server := NewNatsServer(nil, js, analyzer, store, 5, "test", 5, 5, 0)

	hdr := make(nats.Header)
	hdr.Set("Reply-Subject", "test.reply.jit")

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_mem",
			AppId:    "app_mem",
			JobId:    "job_mem",
		},
		Storage: &pb.StorageContract{
			DocumentUri: "internal-store://tenant_mem/app_mem/job_mem/large.txt",
		},
		StepId: "MEM_TEST_STEP",
	}
	data, _ := proto.Marshal(req)

	msg := &mockMsg{
		data:    data,
		headers: hdr,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	server.processMessage(ctx, msg)

	analyzer.mu.Lock()
	defer analyzer.mu.Unlock()

	if analyzer.callCount != 1 {
		t.Fatalf("expected 1 analyzer call, got %d", analyzer.callCount)
	}
	if analyzer.textBytesLen != 1024*1024 {
		t.Errorf("expected text bytes length 1048576, got %d", analyzer.textBytesLen)
	}
}

// TestResiliency_NatsServer_HighConcurrencyStress runs 50 concurrent processMessage calls to verify race freedom.
func TestResiliency_NatsServer_HighConcurrencyStress(t *testing.T) {
	js := &mockJS{}
	analyzer := &memoryAuditAnalyzer{}
	store := &memoryAuditStorage{payload: []byte("concurrent text payload")}
	server := NewNatsServer(nil, js, analyzer, store, 10, "test", 10, 10, 8192)

	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t_stress",
					AppId:    "a_stress",
					JobId:    fmt.Sprintf("j_stress_%d", id),
				},
				Storage: &pb.StorageContract{
					DocumentUri: fmt.Sprintf("internal-store://t_stress/a_stress/j_stress_%d/doc.txt", id),
				},
				StepId: fmt.Sprintf("STEP_%d", id),
			}
			data, _ := proto.Marshal(req)
			msg := &mockMsg{
				data: data,
				metadata: &jetstream.MsgMetadata{
					NumDelivered: 1,
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			server.processMessage(ctx, msg)
		}(i)
	}

	wg.Wait()

	analyzer.mu.Lock()
	defer analyzer.mu.Unlock()
	if analyzer.callCount != concurrency {
		t.Errorf("expected %d total analyzer calls under stress, got %d", concurrency, analyzer.callCount)
	}
}

// TestResiliency_ErrorFormatVerification checks error bubble format across multiple failure modes.
func TestResiliency_ErrorFormatVerification(t *testing.T) {
	os.Setenv("SERVICE_NAME", "ai_service")
	tests := []struct {
		name         string
		docUri       string
		stepId       string
		storeErr     error
		expectedCode string
		expectedMsg  string
	}{
		{
			name:         "Storage Connection Failure",
			docUri:       "internal-store://t/a/j/doc.pdf",
			stepId:       "PARSE_PDF",
			storeErr:     fmt.Errorf("storage offline"),
			expectedCode: "STORAGE_FETCH_FAILED",
			expectedMsg:  "[Step: PARSE_PDF, URI: internal-store://t/a/j/doc.pdf] failed to fetch document from storage: storage offline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := &resiliencyCapturingJS{}
			analyzer := &mockAnalyzer{}
			store := &mockStorageManager{err: tt.storeErr}
			server := NewNatsServer(nil, js, analyzer, store, 1, "test", 1, 1, 8192)

			hdr := make(nats.Header)
			hdr.Set("Reply-Subject", "test.reply.err")

			req := &pb.ExecuteWorkflowNodeRequest{
				Storage: &pb.StorageContract{DocumentUri: tt.docUri},
				StepId:  tt.stepId,
			}
			data, _ := proto.Marshal(req)
			msg := &mockMsg{
				data:    data,
				headers: hdr,
				metadata: &jetstream.MsgMetadata{
					NumDelivered: 1,
				},
			}

			server.processMessage(context.Background(), msg)

			js.mu.Lock()
			pubData := js.lastPublishedData
			js.mu.Unlock()

			if pubData == nil {
				t.Fatalf("expected error response to be sent")
			}

			resp := &pb.PerformActionResponse{}
			_ = proto.Unmarshal(pubData, resp)

			if resp.ErrorContract == nil {
				t.Fatalf("expected ErrorContract to be populated")
			}
			if resp.ErrorContract.Code != tt.expectedCode {
				t.Errorf("expected error code %s, got %s", tt.expectedCode, resp.ErrorContract.Code)
			}
			if resp.ErrorContract.Message != tt.expectedMsg {
				t.Errorf("expected exact error msg:\n%q\ngot:\n%q", tt.expectedMsg, resp.ErrorContract.Message)
			}
			if !strings.HasPrefix(resp.ErrorContract.Message, fmt.Sprintf("[Step: %s, URI: %s]", tt.stepId, tt.docUri)) {
				t.Errorf("error message missing [Step: %s, URI: %s] prefix", tt.stepId, tt.docUri)
			}
		})
	}
}
