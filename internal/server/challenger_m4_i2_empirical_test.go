package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestChallengerM4I2_PanicFailFastScenarios empirically validates all panic fail-fast rules in ai_service.
func TestChallengerM4I2_PanicFailFastScenarios(t *testing.T) {
	os.Setenv("MODEL_ROUTE_INFERENCE", "model-inference-v1")
	os.Setenv("MODEL_ROUTE_DEFAULT", "model-default-v1")
	os.Setenv("OPENAI_TEXT_MODEL", "gpt-4o-text")
	os.Setenv("OPENAI_VISION_MODEL", "gpt-4o-vision")
	os.Setenv("EMBEDDING_MODEL", "text-embedding-3")

	js := &mockJS{}
	analyzer := &mockAnalyzer{}
	store := &mockStorageManager{fileBytes: []byte("sample text content")}
	server := NewNatsServer(nil, js, analyzer, store, 1, "test", 1, 1, 8192)

	t.Run("1. Malformed Proto Data Panics", func(t *testing.T) {
		msg := &mockMsg{
			data: []byte("invalid binary proto header {[[}"),
			metadata: &jetstream.MsgMetadata{
				NumDelivered: 1,
			},
		}
		assert.Panics(t, func() {
			server.processMessage(context.Background(), msg)
		})
	})

	t.Run("2. Missing Identity Panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			TaskSubject: "worker.ai.inference",
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{
			data:     data,
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assert.Panics(t, func() {
			server.processMessage(context.Background(), msg)
		})
	})

	t.Run("3. Incomplete Identity TenantID Panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "", AppId: "app1", JobId: "job1"},
			TaskSubject: "worker.ai.inference",
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{
			data:     data,
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assert.Panics(t, func() {
			server.processMessage(context.Background(), msg)
		})
	})

	t.Run("4. Missing TaskSubject Panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "tenant1", AppId: "app1", JobId: "job1"},
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{
			data:     data,
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assert.Panics(t, func() {
			server.processMessage(context.Background(), msg)
		})
	})

	t.Run("5. Missing NodeConfig / Dynamic Input Panics", func(t *testing.T) {
		payload, _ := structpb.NewStruct(map[string]any{
			"storage": map[string]any{"DocumentUri": "internal-store://tenant1/app1/job1/file.pdf"},
		})
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "tenant1", AppId: "app1", JobId: "job1"},
			TaskSubject: "worker.ai.inference",
			Payload:     payload,
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{
			data:     data,
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assert.Panics(t, func() {
			server.processMessage(context.Background(), msg)
		})
	})

	t.Run("6. Invalid is_scanned Flag Value Panics", func(t *testing.T) {
		payload, _ := structpb.NewStruct(map[string]any{})
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "tenant1", AppId: "app1", JobId: "job1"},
			Payload:  payload,
		}
		assert.Panics(t, func() {
			extractIsScannedFlag(req)
		})
	})

	t.Run("8. resolveModelID Panics on Empty Subject", func(t *testing.T) {
		assert.Panics(t, func() {
			server.resolveModelID("")
		})
		assert.Panics(t, func() {
			server.resolveModelID("   ")
		})
		assert.Panics(t, func() {
			server.resolveModelID("...")
		})
	})

	t.Run("9. resolveModelID Panics on Missing Route Env Var", func(t *testing.T) {
		os.Unsetenv("MODEL_ROUTE_UNKNOWNROUTE")
		assert.Panics(t, func() {
			server.resolveModelID("worker.ai.unknownroute")
		})
	})
}

// TestChallengerM4I2_ConcurrentStressAndRaceSafety tests race conditions and panic handling under high concurrency.
func TestChallengerM4I2_ConcurrentStressAndRaceSafety(t *testing.T) {
	os.Setenv("MODEL_ROUTE_INFERENCE", "model-inference-v1")
	os.Setenv("OPENAI_TEXT_MODEL", "gpt-4o-text")

	js := &mockJS{}
	analyzer := &trackingAnalyzer{}
	store := &mockStorageManager{fileBytes: []byte("concurrent test payload")}
	server := NewNatsServer(nil, js, analyzer, store, 10, "test", 10, 10, 8192)

	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()

			payload, _ := structpb.NewStruct(map[string]any{
				"storage": map[string]any{"document_uri": fmt.Sprintf("internal-store://tenant1/app1/job1/file_%d.txt", id)},
				"node_config": map[string]any{
					"inference_input_key": "input_text",
				},
				"global_state": map[string]any{
					"functional": map[string]any{
						"input_text": fmt.Sprintf("payload content %d", id),
					},
				},
			})

			req := &pb.ExecuteTaskRequest{
				Identity:    &pb.InfrastructureIdentity{TenantId: "tenant1", AppId: "app1", JobId: fmt.Sprintf("job_%d", id)},
				TaskSubject: "worker.ai.inference",
				Payload:     payload,
			}
			data, _ := proto.Marshal(req)

			msg := &mockMsg{
				data:     data,
				metadata: &jetstream.MsgMetadata{NumDelivered: 1},
			}

			server.processMessage(context.Background(), msg)
		}(i)
	}

	wg.Wait()

	analyzer.mu.Lock()
	defer analyzer.mu.Unlock()
	assert.True(t, analyzer.textCalled)
}
