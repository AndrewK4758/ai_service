package ai

import (
	"strings"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestBuildPrompt(t *testing.T) {
	payload := &pb.ExecuteTaskPayload{
		NodeConfig: &pb.NodeConfiguration{
			SystemInstructions: "Perform audit check",
			ExpectedSchema:     `{"type":"object"}`,
			InferenceInputKey:  "my_input",
		},
		GlobalState: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"functional": structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"my_input": structpb.NewStringValue("Input Data: 123"),
					},
				}),
			},
		},
	}

	b, _ := protojson.Marshal(payload)
	st := &structpb.Struct{}
	st.UnmarshalJSON(b)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "t", AppId: "a", JobId: "j"},
		Payload:  st,
	}

	action := &pb.ActionDefinition{
		Name:   "AuditTask",
		Prompt: "Verify document amounts",
	}

	prompt := buildPrompt(req, action, "Document body text")

	// Verify SystemInstructions
	if !strings.Contains(prompt, "Perform audit check") {
		t.Errorf("expected SystemInstructions in prompt")
	}

	// Verify ExpectedSchema
	if !strings.Contains(prompt, `{"type":"object"}`) {
		t.Errorf("expected ExpectedSchema in prompt")
	}

	// Verify Inference Input
	if !strings.Contains(prompt, "Input Data: 123") {
		t.Errorf("expected inference_input in prompt")
	}

	// Verify Action Name and Prompt
	if !strings.Contains(prompt, "AuditTask") || !strings.Contains(prompt, "Verify document amounts") {
		t.Errorf("expected Action details in prompt")
	}

	// Verify Document Content
	if !strings.Contains(prompt, "Document body text") {
		t.Errorf("expected Document Content in prompt")
	}
}
