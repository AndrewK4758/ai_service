package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestChallenger2_AmbiguousMetadataAndPrecedence tests priority and handling of ambiguous metadata
func TestChallenger2_AmbiguousMetadataAndPrecedence(t *testing.T) {
	os.Setenv("OPENAI_VISION_MODEL", "vision-model-target")
	os.Setenv("OPENAI_TEXT_MODEL", "text-model-target")
	os.Setenv("MODEL_ROUTE_DEFAULT", "default-model-target")
	os.Setenv("SERVICE_NAME", "ai_service")

	testCases := []struct {
		name          string
		metadata      map[string]string
		funcState     map[string]any
		nonFuncState  map[string]any
		expectedModel string
	}{
		{
			name:          "Metadata takes precedence over Functional State when both exist",
			metadata:      map[string]string{"IsScanned": "true"},
			funcState:     map[string]any{"is_scanned": false},
			expectedModel: "vision-model-target",
		},
		{
			name:          "Metadata false takes precedence over Functional State true",
			metadata:      map[string]string{"IsScanned": "false"},
			funcState:     map[string]any{"IsScanned": true},
			expectedModel: "text-model-target",
		},
		{
			name:          "Functional State takes precedence over NonFunctional State",
			metadata:      nil,
			funcState:     map[string]any{"is_scanned": true},
			nonFuncState:  map[string]any{"IsScanned": false},
			expectedModel: "vision-model-target",
		},
		{
			name:          "Ambiguous string values '1' vs '0'",
			metadata:      map[string]string{"IsScanned": "1"},
			expectedModel: "vision-model-target",
		},
		{
			name:          "Ambiguous string values '0'",
			metadata:      map[string]string{"is_scanned": "0"},
			expectedModel: "text-model-target",
		},
		{
			name:          "Ambiguous string values 'yes' vs 'no'",
			metadata:      map[string]string{"IsScanned": "yes"},
			expectedModel: "vision-model-target",
		},
		{
			name:          "Ambiguous string values 'no'",
			metadata:      map[string]string{"is_scanned": "no"},
			expectedModel: "text-model-target",
		},
		{
			name:          "Invalid string fallback to default model",
			metadata:      map[string]string{"IsScanned": "invalid_value"},
			expectedModel: "default-model-target",
		},
		{
			name:          "Nil Metadata and empty state falls back to default model",
			metadata:      nil,
			expectedModel: "default-model-target",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			js := &mockJS{}
			analyzer := newModelTrackingAnalyzer()
			store := &workingStore{data: []byte("sample pdf content")}
			server := NewNatsServer(nil, js, analyzer, store, 1, "test", 1, 1, 8192)

			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t_ambig",
					AppId:    "a_ambig",
					JobId:    "j_ambig",
				},
				Storage: &pb.StorageContract{
					DocumentUri: "internal-store://t_ambig/a_ambig/j_ambig/doc.txt",
				},
				StepId:   "STEP_AMBIG",
				Metadata: tc.metadata,
			}

			if tc.funcState != nil {
				st, err := structpb.NewStruct(tc.funcState)
				if err != nil {
					t.Fatalf("failed to build structpb: %v", err)
				}
				if req.GlobalState == nil {
					req.GlobalState = &pb.GlobalState{}
				}
				req.GlobalState.Functional = st
			}

			if tc.nonFuncState != nil {
				st, err := structpb.NewStruct(tc.nonFuncState)
				if err != nil {
					t.Fatalf("failed to build structpb: %v", err)
				}
				if req.GlobalState == nil {
					req.GlobalState = &pb.GlobalState{}
				}
				req.GlobalState.NonFunctional = st
			}

			data, _ := proto.Marshal(req)
			hdr := make(nats.Header)
			hdr.Set("Reply-Subject", "reply.ambig")

			msg := &mockMsg{
				data:    data,
				headers: hdr,
				metadata: &jetstream.MsgMetadata{
					NumDelivered: 1,
				},
			}

			server.processMessage(context.Background(), msg)

			analyzer.mu.Lock()
			modelUsed := analyzer.lastModelByJob["j_ambig"]
			analyzer.mu.Unlock()

			if modelUsed != tc.expectedModel {
				t.Errorf("%s: expected %s, got %s", tc.name, tc.expectedModel, modelUsed)
			}
		})
	}
}

// TestChallenger2_RapidConcurrentPDFSwitching stress-tests 500 concurrent workers toggling scanned vs text PDF payloads
func TestChallenger2_RapidConcurrentPDFSwitching(t *testing.T) {
	os.Setenv("OPENAI_VISION_MODEL", "vision-rapid")
	os.Setenv("OPENAI_TEXT_MODEL", "text-rapid")
	os.Setenv("MODEL_ROUTE_DEFAULT", "default-rapid")
	os.Setenv("SERVICE_NAME", "ai_service")

	js := &mockJS{}
	analyzer := newModelTrackingAnalyzer()
	store := &workingStore{data: []byte("sample pdf content")}
	server := NewNatsServer(nil, js, analyzer, store, 100, "test", 100, 100, 8192)

	const totalRequests = 500
	var wg sync.WaitGroup
	wg.Add(totalRequests)

	for i := 0; i < totalRequests; i++ {
		go func(id int) {
			defer wg.Done()

			jobID := fmt.Sprintf("job_rapid_%d", id)
			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t_rapid",
					AppId:    "a_rapid",
					JobId:    jobID,
				},
				Storage: &pb.StorageContract{
					DocumentUri: fmt.Sprintf("internal-store://t_rapid/a_rapid/%s/doc.txt", jobID),
				},
				StepId: fmt.Sprintf("STEP_%d", id),
			}

			if id%2 == 0 {
				req.Metadata = map[string]string{"IsScanned": "true"}
			} else {
				req.Metadata = map[string]string{"IsScanned": "false"}
			}

			data, _ := proto.Marshal(req)
			hdr := make(nats.Header)
			hdr.Set("Reply-Subject", fmt.Sprintf("reply.rapid.%d", id))

			msg := &mockMsg{
				data:    data,
				headers: hdr,
				metadata: &jetstream.MsgMetadata{
					NumDelivered: 1,
				},
			}

			server.processMessage(context.Background(), msg)
		}(i)
	}

	wg.Wait()

	if analyzer.visionCalls != int64(totalRequests/2) {
		t.Errorf("Expected %d vision calls, got %d", totalRequests/2, analyzer.visionCalls)
	}
	if analyzer.textCalls != int64(totalRequests/2) {
		t.Errorf("Expected %d text calls, got %d", totalRequests/2, analyzer.textCalls)
	}
}
