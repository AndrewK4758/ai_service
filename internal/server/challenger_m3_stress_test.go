package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/workflowparser"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type modelTrackingAnalyzer struct {
	mu                  sync.Mutex
	visionCalls         int64
	textCalls           int64
	defaultCalls        int64
	lastModelByJob      map[string]string
}

func newModelTrackingAnalyzer() *modelTrackingAnalyzer {
	return &modelTrackingAnalyzer{
		lastModelByJob: make(map[string]string),
	}
}

func (m *modelTrackingAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobID := "unknown"
	if workflowCtx != nil {
		_, _, jobID = workflowCtx.ExtractIdentity()
	}
	m.lastModelByJob[jobID] = modelID

	switch modelID {
	case "gpt-4o-vision-test":
		atomic.AddInt64(&m.visionCalls, 1)
	case "gemma4-extended-test":
		atomic.AddInt64(&m.textCalls, 1)
	case "default-llm-test":
		atomic.AddInt64(&m.defaultCalls, 1)
	}

	return `{"status":"processed","model":"` + modelID + `"}`, nil
}

func (m *modelTrackingAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobID := "unknown"
	if workflowCtx != nil {
		_, _, jobID = workflowCtx.ExtractIdentity()
	}
	m.lastModelByJob[jobID] = modelID

	switch modelID {
	case "gpt-4o-vision-test":
		atomic.AddInt64(&m.visionCalls, 1)
	case "gemma4-extended-test":
		atomic.AddInt64(&m.textCalls, 1)
	case "default-llm-test":
		atomic.AddInt64(&m.defaultCalls, 1)
	}

	return `{"status":"processed_vision","model":"` + modelID + `"}`, nil
}

func (m *modelTrackingAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return []float32{0.5, 0.5}, nil
}

// TestChallenger_DynamicModelRouting_RaceAndConcurrency tests dynamic model routing under race conditions with 300 concurrent requests.
func TestChallenger_DynamicModelRouting_RaceAndConcurrency(t *testing.T) {
	os.Setenv("OPENAI_VISION_MODEL", "gpt-4o-vision-test")
	os.Setenv("OPENAI_TEXT_MODEL", "gemma4-extended-test")
	os.Setenv("MODEL_ROUTE_DEFAULT", "default-llm-test")
	os.Setenv("MODEL_ROUTE_SUBJECT", "default-llm-test")
	os.Setenv("SERVICE_NAME", "ai_service")
	os.Setenv("WORKER_ACK_WAIT", "1m")

	js := &mockJS{}
	analyzer := newModelTrackingAnalyzer()
	store := &workingStore{data: []byte("sample pdf or text content")}
	server := NewNatsServer(nil, js, analyzer, store, 50, "test", 50, 50, 8192)

	concurrency := 300
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()

			jobID := fmt.Sprintf("job_stress_%d", id)
			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "tenant_stress",
					AppId:    "app_stress",
					JobId:    jobID,
				},
				Storage: &pb.StorageContract{
					DocumentUri: fmt.Sprintf("internal-store://tenant_stress/app_stress/%s/doc.txt", jobID),
				},
				StepId: fmt.Sprintf("STEP_%d", id),
			}

			// Alternate between scanned, structured text, and unflagged requests
			switch id % 3 {
			case 0:
				req.Metadata = map[string]string{"IsScanned": "true"}
			case 1:
				req.Metadata = map[string]string{"IsScanned": "false"}
			case 2:
				// No IsScanned flag -> should fall back to resolveModelID(subject)
			}

			data, err := proto.Marshal(req)
			if err != nil {
				t.Errorf("failed to marshal req: %v", err)
				return
			}

			hdr := make(nats.Header)
			hdr.Set("Reply-Subject", fmt.Sprintf("reply.subject.%d", id))

			msg := &mockMsg{
				data:    data,
				headers: hdr,
				metadata: &jetstream.MsgMetadata{
					NumDelivered: 1,
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			server.processMessage(ctx, msg)
		}(i)
	}

	wg.Wait()

	if analyzer.visionCalls != int64(concurrency/3) {
		t.Errorf("expected %d vision calls (scanned), got %d", concurrency/3, analyzer.visionCalls)
	}
	if analyzer.textCalls != int64(concurrency/3) {
		t.Errorf("expected %d text calls (structured), got %d", concurrency/3, analyzer.textCalls)
	}
	if analyzer.defaultCalls != int64(concurrency/3) {
		t.Errorf("expected %d default calls (unflagged), got %d", concurrency/3, analyzer.defaultCalls)
	}
}

// TestChallenger_DynamicModelRouting_EdgeCaseFlags tests all variations of IsScanned metadata & functional state formats.
func TestChallenger_DynamicModelRouting_EdgeCaseFlags(t *testing.T) {
	os.Setenv("OPENAI_VISION_MODEL", "vision-model-override")
	os.Setenv("OPENAI_TEXT_MODEL", "text-model-override")
	os.Setenv("MODEL_ROUTE_DEFAULT", "default-model")
	os.Setenv("MODEL_ROUTE_SUBJECT", "default-model")
	os.Setenv("SERVICE_NAME", "ai_service")

	testCases := []struct {
		name          string
		metadata      map[string]string
		funcState     map[string]any
		nonFuncState  map[string]any
		expectedModel string
	}{
		{
			name:          "Metadata IsScanned = true",
			metadata:      map[string]string{"IsScanned": "true"},
			expectedModel: "vision-model-override",
		},
		{
			name:          "Metadata is_scanned = true",
			metadata:      map[string]string{"is_scanned": "TRUE "},
			expectedModel: "vision-model-override",
		},
		{
			name:          "Metadata IsScanned = false",
			metadata:      map[string]string{"IsScanned": "false"},
			expectedModel: "text-model-override",
		},
		{
			name:          "Metadata is_scanned = false",
			metadata:      map[string]string{"is_scanned": "FALSE"},
			expectedModel: "text-model-override",
		},
		{
			name:          "Functional State IsScanned bool true",
			funcState:     map[string]any{"IsScanned": true},
			expectedModel: "vision-model-override",
		},
		{
			name:          "Functional State is_scanned bool false",
			funcState:     map[string]any{"is_scanned": false},
			expectedModel: "text-model-override",
		},
		{
			name:          "Non-Functional State IsScanned string true",
			nonFuncState:  map[string]any{"IsScanned": "true"},
			expectedModel: "vision-model-override",
		},
		{
			name:          "Malformed string flag ignored (falls back to default)",
			metadata:      map[string]string{"IsScanned": "not_a_bool"},
			expectedModel: "default-model",
		},
		{
			name:          "Empty metadata & state (falls back to default)",
			expectedModel: "default-model",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			js := &mockJS{}
			analyzer := newModelTrackingAnalyzer()
			store := &workingStore{data: []byte("test doc payload")}
			server := NewNatsServer(nil, js, analyzer, store, 1, "test", 1, 1, 8192)

			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t_edge",
					AppId:    "a_edge",
					JobId:    "j_edge",
				},
				Storage: &pb.StorageContract{
					DocumentUri: "internal-store://t_edge/a_edge/j_edge/doc.txt",
				},
				StepId:   "STEP_EDGE",
				Metadata: tc.metadata,
			}

			if tc.funcState != nil {
				st, err := structpb.NewStruct(tc.funcState)
				if err != nil {
					t.Fatalf("failed to create structpb: %v", err)
				}
				if req.GlobalState == nil {
					req.GlobalState = &pb.GlobalState{}
				}
				req.GlobalState.Functional = st
			}

			if tc.nonFuncState != nil {
				st, err := structpb.NewStruct(tc.nonFuncState)
				if err != nil {
					t.Fatalf("failed to create structpb: %v", err)
				}
				if req.GlobalState == nil {
					req.GlobalState = &pb.GlobalState{}
				}
				req.GlobalState.NonFunctional = st
			}

			data, _ := proto.Marshal(req)
			hdr := make(nats.Header)
			hdr.Set("Reply-Subject", "reply.edge")

			msg := &mockMsg{
				data:    data,
				headers: hdr,
				metadata: &jetstream.MsgMetadata{
					NumDelivered: 1,
				},
			}

			server.processMessage(context.Background(), msg)

			analyzer.mu.Lock()
			modelUsed := analyzer.lastModelByJob["j_edge"]
			analyzer.mu.Unlock()

			if modelUsed != tc.expectedModel {
				t.Errorf("tc %s: expected model %s, got %s", tc.name, tc.expectedModel, modelUsed)
			}
		})
	}
}
