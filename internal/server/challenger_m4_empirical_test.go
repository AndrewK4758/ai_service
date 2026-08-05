package server


import (
	"context"
	"fmt"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func assertPanicContains(t *testing.T, f func(), expectedSubstr string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, but code did not panic", expectedSubstr)
		}
		var errStr string
		switch v := r.(type) {
		case error:
			errStr = v.Error()
		case string:
			errStr = v
		default:
			errStr = fmt.Sprintf("%v", v)
		}
		assert.Contains(t, errStr, expectedSubstr)
	}()
	f()
}

func assertPanicEquals(t *testing.T, f func(), expectedMsg string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic equal to %q, but code did not panic", expectedMsg)
		}
		var errStr string
		switch v := r.(type) {
		case error:
			errStr = v.Error()
		case string:
			errStr = v
		default:
			errStr = fmt.Sprintf("%v", v)
		}
		assert.Equal(t, expectedMsg, errStr)
	}()
	f()
}

func TestEmpirical_FailFastScenarios(t *testing.T) {
	srv := &NatsServer{}
	ctx := context.Background()

	t.Run("Scenario_1:_Malformed_proto_payload_in_processMessage_->_must_panic", func(t *testing.T) {
		msg := &mockMsg{
			data:     []byte("invalid-proto-garbage-payload"),
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assertPanicContains(t, func() {
			srv.processMessage(ctx, msg)
		}, "cannot parse invalid wire-format data")
	})

	t.Run("Scenario_2:_Missing_identity_in_processMessage_->_must_panic", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			TaskSubject: "worker.ai.inference",
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{
			data:     data,
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assertPanicContains(t, func() {
			srv.processMessage(ctx, msg)
		}, "action failed: invalid identity - InfrastructureIdentity is nil")
	})

	t.Run("Scenario_3:_Missing_task_subject_in_processMessage_->_must_panic", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "t1",
				AppId:    "a1",
				JobId:    "j1",
			},
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{
			data:     data,
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assertPanicEquals(t, func() {
			srv.processMessage(ctx, msg)
		}, "action failed: missing required TaskSubject")
	})

	t.Run("Scenario_4:_Missing_dynamic_input_in_processMessage_->_must_panic", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "t1",
				AppId:    "a1",
				JobId:    "j1",
			},
			TaskSubject: "worker.ai.inference",
		}
		data, _ := proto.Marshal(req)
		msg := &mockMsg{
			data:     data,
			metadata: &jetstream.MsgMetadata{NumDelivered: 1},
		}
		assertPanicContains(t, func() {
			srv.processMessage(ctx, msg)
		}, "action failed: missing payload")
	})

	t.Run("Scenario_5:_Malformed_scanned_flag_in_extractIsScannedFlag_->_must_panic", func(t *testing.T) {
		st, _ := structpb.NewStruct(map[string]any{
			"storage": map[string]any{"document_uri": "internal-store://edge/doc.txt"},
				"global_state": map[string]any{
				"functional": map[string]any{
					"is_scanned": "invalid_boolean_string",
				},
			},
		})
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "t1",
				AppId:    "a1",
				JobId:    "j1",
			},
			TaskSubject: "worker.ai.inference",
			Payload:     st,
		}
		assertPanicEquals(t, func() {
			extractIsScannedFlag(req)
		}, "action failed: invalid is_scanned flag value")
	})

	t.Run("Scenario_6:_Invalid_empty_subject_in_resolveModelID_->_must_panic", func(t *testing.T) {
		assertPanicEquals(t, func() {
			srv.resolveModelID("")
		}, "action failed: missing TaskSubject for resolveModelID")

		assertPanicEquals(t, func() {
			srv.resolveModelID("   ")
		}, "action failed: missing TaskSubject for resolveModelID")
	})

	t.Run("Scenario_7:_Missing_DocumentUri_in_getDocumentUri_->_must_panic", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "t1",
				AppId:    "a1",
				JobId:    "j1",
			},
		}
		assertPanicEquals(t, func() {
			getDocumentUri(req)
		}, "action failed: missing payload")
	})
}
