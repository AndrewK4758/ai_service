package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestChallenger_NatsServer_ErrorContractFormat_Comprehensive verifies all error paths enrich ErrorContract with [Step: %s, URI: %s].
func TestChallenger_NatsServer_ErrorContractFormat_Comprehensive(t *testing.T) {
	os.Setenv("SERVICE_NAME", "ai_service")

	st := func(uri string) *structpb.Struct {
		s, _ := structpb.NewStruct(map[string]any{
			"node_config": map[string]any{},
			"global_state": map[string]any{
				"functional": map[string]any{},
			},
			"storage": map[string]any{"document_uri": uri},
		})
		return s
	}

	tests := []struct {
		name           string
		req            *pb.ExecuteTaskRequest
		store          storageManagerMock
		analyzerErr    error
		expectedCode   string
		expectedSubstr string
	}{
		{
			name: "Nil Storage Manager",
			req: &pb.ExecuteTaskRequest{
				Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
				TaskSubject: "worker.ai.default",
				StepId:      "STEP_NIL_STORE",
				Payload:     st("internal-store://t/a/j/test.pdf"),
			},
			store:          nil,
			expectedCode:   "STORAGE_FETCH_FAILED",
			expectedSubstr: "[Step: STEP_NIL_STORE, URI: internal-store://t/a/j/test.pdf] storage manager is not configured",
		},
		{
			name: "Storage Fetch Failed Error",
			req: &pb.ExecuteTaskRequest{
				Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
				TaskSubject: "worker.ai.default",
				StepId:      "STEP_FETCH_FAIL",
				Payload:     st("internal-store://t/a/j/missing.pdf"),
			},
			store:          &failingStore{err: fmt.Errorf("connection refused")},
			expectedCode:   "STORAGE_FETCH_FAILED",
			expectedSubstr: "[Step: STEP_FETCH_FAIL, URI: internal-store://t/a/j/missing.pdf] failed to fetch document from storage: connection refused",
		},
		{
			name: "AI Analyzer Execution Failure",
			req: &pb.ExecuteTaskRequest{
				Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
				TaskSubject: "worker.ai.default",
				StepId:      "STEP_AI_FAIL",
				Payload:     st("internal-store://t/a/j/doc.txt"),
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
	FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) (io.Reader, error)
	SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data io.Reader) (string, error)
}

type failingStore struct {
	err error
}

func (f *failingStore) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) (io.Reader, error) {
	return nil, f.err
}

func (f *failingStore) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data io.Reader) (string, error) {
	return "", f.err
}

type workingStore struct {
	data []byte
}

func (w *workingStore) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) (io.Reader, error) {
	return bytes.NewReader(w.data), nil
}

func (w *workingStore) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data io.Reader) (string, error) {
	return relativePath, nil
}

type mockAnalyzerWithErr struct {
	err error
}

func (m *mockAnalyzerWithErr) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return `{"status":"ok"}`, nil
}

func (m *mockAnalyzerWithErr) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
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

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			st, _ := structpb.NewStruct(map[string]any{
				"node_config": map[string]any{},
				"storage":     map[string]any{"document_uri": "internal-store://edge/doc.txt"},
				"global_state": map[string]any{
					"functional": map[string]any{},
				},
			})
			req := &pb.ExecuteTaskRequest{
				Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
				TaskSubject: "worker.ai.default",
				StepId:      fmt.Sprintf("STEP_%d", id),
				Payload:     st,
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
