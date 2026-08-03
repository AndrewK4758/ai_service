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

// TestChallenger_NatsServer_ErrorContractFormat_Comprehensive verifies all error paths enrich ErrorContract with [Step: %s, URI: %s].
func TestChallenger_NatsServer_ErrorContractFormat_Comprehensive(t *testing.T) {
	os.Setenv("SERVICE_NAME", "ai_service")

	tests := []struct {
		name           string
		req            *pb.ExecuteWorkflowNodeRequest
		store          storageManagerMock
		analyzerErr    error
		expectedCode   string
		expectedSubstr string
	}{
		{
			name: "Nil Storage Manager",
			req: &pb.ExecuteWorkflowNodeRequest{
				Storage: &pb.StorageContract{DocumentUri: "internal-store://t/a/j/test.pdf"},
				StepId:  "STEP_NIL_STORE",
			},
			store:          nil,
			expectedCode:   "STORAGE_FETCH_FAILED",
			expectedSubstr: "[Step: STEP_NIL_STORE, URI: internal-store://t/a/j/test.pdf] storage manager is not configured",
		},
		{
			name: "Storage Fetch Failed Error",
			req: &pb.ExecuteWorkflowNodeRequest{
				Storage: &pb.StorageContract{DocumentUri: "internal-store://t/a/j/missing.pdf"},
				StepId:  "STEP_FETCH_FAIL",
			},
			store:          &failingStore{err: fmt.Errorf("connection refused")},
			expectedCode:   "STORAGE_FETCH_FAILED",
			expectedSubstr: "[Step: STEP_FETCH_FAIL, URI: internal-store://t/a/j/missing.pdf] failed to fetch document from storage: connection refused",
		},
		{
			name: "AI Analyzer Execution Failure",
			req: &pb.ExecuteWorkflowNodeRequest{
				Storage: &pb.StorageContract{DocumentUri: "internal-store://t/a/j/doc.txt"},
				StepId:  "STEP_AI_FAIL",
			},
			store:          &workingStore{data: []byte("sample text")},
			analyzerErr:    fmt.Errorf("model context length exceeded"),
			expectedCode:   "AI_PROCESSING_FAILED",
			expectedSubstr: "[Step: STEP_AI_FAIL, URI: internal-store://t/a/j/doc.txt] failed to perform action on payload: model context length exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := &resiliencyCapturingJS{}
			analyzer := &mockAnalyzerWithErr{err: tt.analyzerErr}
			server := NewNatsServer(nil, js, analyzer, tt.store, 1, "test", 1, 1, 8192)

			hdr := make(nats.Header)
			hdr.Set("Reply-Subject", "test.reply.err")

			data, _ := proto.Marshal(tt.req)
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
				t.Fatalf("expected response published, got nil")
			}

			resp := &pb.PerformActionResponse{}
			if err := proto.Unmarshal(pubData, resp); err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}

			if resp.ErrorContract == nil {
				t.Fatalf("expected ErrorContract, got nil")
			}

			if resp.ErrorContract.Code != tt.expectedCode {
				t.Errorf("expected error code %s, got %s", tt.expectedCode, resp.ErrorContract.Code)
			}

			if !strings.Contains(resp.ErrorContract.Message, tt.expectedSubstr) {
				t.Errorf("expected message to contain %q, got %q", tt.expectedSubstr, resp.ErrorContract.Message)
			}
		})
	}
}

type storageManagerMock interface {
	FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) ([]byte, error)
	SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data []byte) (string, error)
}

type failingStore struct {
	err error
}

func (f *failingStore) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) ([]byte, error) {
	return nil, f.err
}

func (f *failingStore) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data []byte) (string, error) {
	return "", f.err
}

type workingStore struct {
	data []byte
}

func (w *workingStore) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) ([]byte, error) {
	return w.data, nil
}

func (w *workingStore) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data []byte) (string, error) {
	return relativePath, nil
}

type mockAnalyzerWithErr struct {
	err error
}

func (m *mockAnalyzerWithErr) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return `{"status":"ok"}`, nil
}

func (m *mockAnalyzerWithErr) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return `{"status":"ok"}`, nil
}

func (m *mockAnalyzerWithErr) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return []float32{0.1}, nil
}

// TestChallenger_NatsServer_ConcurrentProcessingStress 100 concurrent workers processing messages.
func TestChallenger_NatsServer_ConcurrentProcessingStress(t *testing.T) {
	js := &mockJS{}
	analyzer := &memoryAuditAnalyzer{}
	store := &workingStore{data: []byte("concurrent test payload")}
	server := NewNatsServer(nil, js, analyzer, store, 20, "test", 20, 20, 8192)

	concurrency := 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t_challenger",
					AppId:    "a_challenger",
					JobId:    fmt.Sprintf("j_challenger_%d", id),
				},
				Storage: &pb.StorageContract{
					DocumentUri: fmt.Sprintf("internal-store://t_challenger/a_challenger/j_challenger_%d/doc.txt", id),
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			server.processMessage(ctx, msg)
		}(i)
	}

	wg.Wait()

	analyzer.mu.Lock()
	defer analyzer.mu.Unlock()

	if analyzer.callCount != concurrency {
		t.Errorf("expected %d analyzer calls under concurrent stress, got %d", concurrency, analyzer.callCount)
	}
}
