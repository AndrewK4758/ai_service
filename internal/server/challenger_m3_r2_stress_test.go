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
	os.Setenv("MODEL_ROUTE_SUBJECT", "default-model-target")
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
			funcState:     map[string]any{"IsScanned": false},
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
			funcState:     map[string]any{"IsScanned": true},
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
			metadata:      map[string]string{"IsScanned": "0"},
			expectedModel: "text-model-target",
		},
		{
			name:          "Ambiguous string values 'yes' vs 'no'",
			metadata:      map[string]string{"IsScanned": "yes"},
			expectedModel: "vision-model-target",
		},
		{
			name:          "Ambiguous string values 'no'",
			metadata:      map[string]string{"IsScanned": "no"},
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

			payloadMap := make(map[string]any)
			for k, v := range tc.nonFuncState {
				payloadMap[k] = v
			}
			for k, v := range tc.funcState {
				payloadMap[k] = v
			}
			for k, v := range tc.metadata {
				if v == "true" {
					payloadMap[k] = true
				} else if v == "false" {
					payloadMap[k] = false
				} else {
					payloadMap[k] = v
				}
			}
			st, _ := structpb.NewStruct(payloadMap)

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

// TestChallenger2_RapidConcurrentPDFSwitching stress-tests 500 concurrent workers toggling scanned vs text PDF payloads
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

	for i := 0; i < totalRequests; i++ {
		go func(id int) {
			defer wg.Done()

			payload, _ := structpb.NewStruct(map[string]any{
				"IsScanned":   id%2 == 0,
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
