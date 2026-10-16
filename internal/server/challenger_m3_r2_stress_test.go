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

func TestChallenger2_AmbiguousMetadataAndPrecedence(t *testing.T) {
	os.Setenv("OPENAI_VISION_MODEL", "vision-model-target")
	os.Setenv("OPENAI_TEXT_MODEL", "text-model-target")
	os.Setenv("MODEL_ROUTE_DEFAULT", "default-model-target")
	os.Setenv("MODEL_ROUTE_SUBJECT", "default-model-target")
	os.Setenv("SERVICE_NAME", "ai_service")

	testCases := []struct {
		name          string
		nodeConfig    map[string]any
		funcState     map[string]any
		storage       map[string]any
		expectedModel string
	}{
		{
			name:          "global_state.functional true uses vision model",
			nodeConfig:    nil,
			funcState:     map[string]any{"is_scanned": true},
			expectedModel: "vision-model-target",
		},
		{
			name:          "global_state.functional false uses text model",
			nodeConfig:    nil,
			funcState:     map[string]any{"is_scanned": false},
			expectedModel: "text-model-target",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			js := &mockJS{}
			analyzer := newModelTrackingAnalyzer()
			store := &workingStore{data: []byte("sample pdf content")}
			server := NewNatsServer(nil, js, analyzer, store, 1, "test", 1, 1, 8192)

			st := buildTestPayload(tc.nodeConfig, tc.funcState, tc.storage)

			req := &pb.ExecuteTaskRequest{
				StepId:      "STEP_AMBIG",
				Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j_ambig"},
				TaskSubject: "worker.ai.default",
				Payload:     st,
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

func TestChallenger2_RapidConcurrentPDFSwitching(t *testing.T) {
	os.Setenv("OPENAI_VISION_MODEL", "gpt-4o-vision-test")
	os.Setenv("OPENAI_TEXT_MODEL", "gemma4-extended-test")
	os.Setenv("MODEL_ROUTE_DEFAULT", "default-llm-test")
	os.Setenv("SERVICE_NAME", "ai_service")

	js := &mockJS{}
	analyzer := newModelTrackingAnalyzer()
	store := &workingStore{data: []byte("sample pdf content")}
	server := NewNatsServer(nil, js, analyzer, store, 100, "test", 100, 100, 8192)

	const totalRequests = 500
	var wg sync.WaitGroup
	wg.Add(totalRequests)

	for i := range totalRequests {
		go func(id int) {
			defer wg.Done()

			isScanned := id%2 == 0
			payload, _ := structpb.NewStruct(map[string]any{
				"node_config": map[string]any{},
				"storage":     map[string]any{"document_uri": "internal-store://edge/doc.txt"},
				"global_state": map[string]any{
					"functional": map[string]any{
						"is_scanned": isScanned,
					},
				},
			})

			req := &pb.ExecuteTaskRequest{
				Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: fmt.Sprintf("j_%d", id)},
				TaskSubject: "worker.ai.default",
				StepId:      fmt.Sprintf("STEP_%d", id),
				Payload:     payload,
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
