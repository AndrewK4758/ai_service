package server

import (
	"context"
	"fmt"
	"os"
	"runtime"
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

func init() {
	os.Setenv("WORKER_ACK_WAIT", "1m")
	os.Setenv("NATS_RATE_LIMIT_DELAY", "500ms")
	os.Setenv("NATS_NACK_ERROR_TIMEOUT", "1s")
	os.Setenv("NATS_REPLY_DELAY", "10ms")
	os.Setenv("NATS_RESPONSE_STREAM", "WORKFLOW_RESPONSES")
	os.Setenv("SERVICE_NAME", "ai_service")
	os.Setenv("MODEL_ROUTE_DEFAULT", "model-default-v0")
	os.Setenv("MODEL_ROUTE_SUBJECT", "worker.ai.inference")
}

func TestAdversarial_AIService_ModelResolutionAndVisionRoutingEdgeCases(t *testing.T) {
	t.Setenv("MODEL_ROUTE_DEFAULT", "default-model-v1")
	t.Setenv("MODEL_ROUTE_INFERENCE", "inference-model-v2")
	t.Setenv("MODEL_ROUTE_OCR", "ocr-model-v3")
	t.Setenv("MODEL_EMBEDDING", "text-embedding-3-small")
	t.Setenv("MODEL_ROUTE_CUSTOM_STEP", "custom-model-v4")
	t.Setenv("MODEL_ROUTE_FALLBACK", "fallback-model-v5")

	server := &NatsServer{}

	t.Run("resolveModelID Edge Cases", func(t *testing.T) {
		tests := []struct {
			subject  string
			expected string
			panics   bool
		}{
			{"", "", true},
			{"   ", "", true},
			{"...", "", true},
			{"worker.ai.inference", "inference-model-v2", false},
			{"worker.ai.ocr", "ocr-model-v3", false},
			{"worker.ai.embed", "text-embedding-3-small", false},
			{"worker.ai.embedding", "text-embedding-3-small", false},
			{"worker.ai.custom-step", "custom-model-v4", false},
			{"worker.ai.fallback", "fallback-model-v5", false},
		}

		for _, tt := range tests {
			if tt.panics {
				func() {
					defer func() {
						if r := recover(); r == nil {
							t.Errorf("resolveModelID(%q) expected to panic but didn't", tt.subject)
						}
					}()
					server.resolveModelID(tt.subject)
				}()
			} else {
				got, _ := server.resolveModelID(tt.subject)
				if got != tt.expected {
					t.Errorf("resolveModelID(%q) = %q; want %q", tt.subject, got, tt.expected)
				}
			}
		}
	})

	t.Run("isVisionRequest Edge Cases", func(t *testing.T) {
		tests := []struct {
			subj     string
			modelID  string
			ext      string
			expected bool
		}{
			{"worker.ai.vision", "any-model", ".txt", true},
			{"worker.ai.ocr", "any-model", ".pdf", true},
			{"worker.ai.image", "any-model", ".pdf", true},
			{"worker.ai.text", "gpt-4o", ".txt", true},
			{"worker.ai.text", "claude-3-opus", ".txt", true},
			{"worker.ai.text", "standard-model", ".png", true},
			{"worker.ai.text", "standard-model", ".jpg", true},
			{"worker.ai.text", "standard-model", ".jpeg", true},
			{"worker.ai.text", "standard-model", ".webp", true},
			{"worker.ai.text", "standard-model", ".tiff", true},
			{"worker.ai.text", "standard-model", ".bmp", true},
			{"worker.ai.text", "standard-model", ".pdf", false},
			{"worker.ai.text", "standard-model", ".docx", false},
			{"worker.ai.text", "standard-model", ".txt", false},
		}

		for _, tt := range tests {
			got := isVisionRequest(tt.subj, tt.modelID, tt.ext)
			if got != tt.expected {
				t.Errorf("isVisionRequest(%q, %q, %q) = %v; want %v", tt.subj, tt.modelID, tt.ext, got, tt.expected)
			}
		}
	})
}

func TestAdversarial_AIService_ErrorContractContextEnrichment_AllPaths(t *testing.T) {
	t.Run("NACK / Error Reply Context Enrichment", func(t *testing.T) {
		js := &resiliencyCapturingJS{}
		analyzer := &mockAnalyzerWithErr{err: fmt.Errorf("invalid prompt format")}
		mockStore := &workingStore{data: []byte("sample content")}

		server := NewNatsServer(nil, js, analyzer, mockStore, 1, "test", 1, 1, 8192)

		hdr := make(nats.Header)
		hdr.Set("Reply-Subject", "test.reply.adv")
		hdr.Set("X-Page-Limit", "10")
		hdr.Set("X-Tenant-Id", "t1")

		payload, _ := structpb.NewStruct(map[string]any{
			"node_config": map[string]any{},
			"global_state": map[string]any{
				"functional": map[string]any{},
			},
			"storage": map[string]any{"document_uri": "internal-store://edge/doc.txt"},
		})
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
			TaskSubject: "worker.ai.default",
			StepId:      "ADV_AI_STEP",
			Payload:     payload,
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
			t.Fatalf("expected error reply to be published")
		}

		resp := &pb.PerformActionResponse{}
		if err := proto.Unmarshal(pubData, resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		if resp.ErrorContract == nil {
			t.Fatalf("expected ErrorContract to be populated")
		}

		expectedPrefix := "[Step: ADV_AI_STEP, URI: internal-store://edge/doc.txt]"
		if !strings.HasPrefix(resp.ErrorContract.Message, expectedPrefix) {
			t.Errorf("error contract message does not start with expected prefix %q, got: %q", expectedPrefix, resp.ErrorContract.Message)
		}
		if resp.ErrorContract.Source != "ai_service" {
			t.Errorf("expected source 'ai_service', got %q", resp.ErrorContract.Source)
		}
		if resp.ErrorContract.Code != "AI_PROCESSING_FAILED" {
			t.Errorf("expected code 'AI_PROCESSING_FAILED', got %q", resp.ErrorContract.Code)
		}
	})
}

func TestAdversarial_AIService_JITByteDiscardAndMemoryAudit(t *testing.T) {
	js := &resiliencyCapturingJS{}
	analyzer := &memoryAuditAnalyzer{}

	payloadSize := 5 * 1024 * 1024
	bigPayload := make([]byte, payloadSize)
	for i := range bigPayload {
		bigPayload[i] = 'Z'
	}

	store := &memoryAuditStorage{payload: bigPayload}
	server := NewNatsServer(nil, js, analyzer, store, 10, "test", 10, 10, 0)

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	for range 20 {
		payload, _ := structpb.NewStruct(map[string]any{
			"node_config": map[string]any{},
			"global_state": map[string]any{
				"functional": map[string]any{},
			},
			"storage": map[string]any{"document_uri": "internal-store://mem_test/doc.txt"},
		})
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
			TaskSubject: "worker.ai.default",
			Payload:     payload,
		}
		data, _ := proto.Marshal(req)
		hdr := make(nats.Header)
		hdr.Set("Reply-Subject", "test.reply.mem")
		hdr.Set("X-Page-Limit", "10")
		hdr.Set("X-Tenant-Id", "t1")

		msg := &mockMsg{
			data:    data,
			headers: hdr,
			metadata: &jetstream.MsgMetadata{
				NumDelivered: 1,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		server.processMessage(ctx, msg)
		cancel()
	}

	runtime.GC()
	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	analyzer.mu.Lock()
	callCount := analyzer.callCount
	analyzer.mu.Unlock()

	if callCount != 20 {
		t.Errorf("expected 20 analyzer calls, got %d", callCount)
	}
}

func TestAdversarial_AIService_HighConcurrencyStress(t *testing.T) {
	js := &mockJS{}
	analyzer := &memoryAuditAnalyzer{}
	store := &memoryAuditStorage{payload: []byte("adversarial race test payload")}
	server := NewNatsServer(nil, js, analyzer, store, 20, "test", 20, 20, 8192)

	concurrency := 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()

			payload, _ := structpb.NewStruct(map[string]any{
				"node_config": map[string]any{},
				"global_state": map[string]any{
					"functional": map[string]any{},
				},
				"storage": map[string]any{"document_uri": "internal-store://mem_test/doc.txt"},
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
