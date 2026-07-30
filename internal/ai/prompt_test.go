package ai

import (
	"strings"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/workflowparser"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestBuildPrompt_WithGlobalState_StructPB(t *testing.T) {
	stateMap := map[string]any{
		"extracted_doc": map[string]any{
			"title":  "Test Document",
			"amount": 123.45,
		},
		"other_key": "ignore_me",
	}
	funcStruct, err := structpb.NewStruct(stateMap)
	if err != nil {
		t.Fatalf("failed to create structpb: %v", err)
	}

	req := &pb.ExecuteWorkflowNodeRequest{
		NodeConfig: &pb.NodeConfiguration{
			InferenceInputKey:  "extracted_doc",
			SystemInstructions: "Perform audit check",
			ExpectedSchemaStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"type": structpb.NewStringValue("object"),
				},
			},
		},
		GlobalState: &pb.GlobalState{Functional: funcStruct},
	}
	parser := workflowparser.NewNodeRequestParser(req)

	action := &pb.ActionDefinition{
		Name:   "AuditTask",
		Prompt: "Verify document amounts",
	}

	prompt := buildPrompt(parser, action, "Document body text")

	// Verify SystemInstructions
	if !strings.Contains(prompt, "Perform audit check") {
		t.Errorf("expected SystemInstructions in prompt")
	}

	// Verify ExpectedSchema
	if !strings.Contains(strings.ReplaceAll(prompt, " ", ""), `{"type":"object"}`) {
		t.Errorf("expected ExpectedSchema in prompt")
	}

	// Verify Input Data from JSON
	if !strings.Contains(prompt, "Input Data (extracted_doc):") {
		t.Errorf("expected Input Data header in prompt")
	}
	if !strings.Contains(prompt, "Test Document") || !strings.Contains(prompt, "123.45") {
		t.Errorf("expected serialized structpb fields in prompt, got:\n%s", prompt)
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

func TestBuildPrompt_MissingInputKey(t *testing.T) {
	req := &pb.ExecuteWorkflowNodeRequest{
		NodeConfig: &pb.NodeConfiguration{
			InferenceInputKey: "missing_key",
		},
		GlobalState: &pb.GlobalState{},
	}
	parser := workflowparser.NewNodeRequestParser(req)

	prompt := buildPrompt(parser, nil, "")

	if strings.Contains(prompt, "Input Data (missing_key):") {
		t.Errorf("did not expect Input Data for missing key in prompt")
	}
}

func TestBuildPrompt_NilState(t *testing.T) {
	req := &pb.ExecuteWorkflowNodeRequest{
		NodeConfig: &pb.NodeConfiguration{
			InferenceInputKey: "any_key",
		},
		GlobalState: &pb.GlobalState{},
	}
	parser := workflowparser.NewNodeRequestParser(req)

	prompt := buildPrompt(parser, nil, "")
	if strings.Contains(prompt, "Input Data") {
		t.Errorf("expected no Input Data when GlobalState is nil")
	}
}

func TestBuildPrompt_StructPB_EdgeCases(t *testing.T) {
	tests := []struct {
		name                 string
		stateMap             map[string]any
		inferenceInputKey    string
		expectedSubstrings   []string
		unexpectedSubstrings []string
	}{
		{
			name: "DeeplyNestedJSON",
			stateMap: map[string]any{
				"deep_key": map[string]any{
					"level2": map[string]any{
						"level3": []any{
							map[string]any{"id": float64(101), "name": "nested_item_1"},
							map[string]any{"id": float64(102), "name": "nested_item_2"},
						},
						"active": true,
						"count":  float64(42.5),
					},
				},
				"ignored_root_key": "ignore_me",
			},
			inferenceInputKey: "deep_key",
			expectedSubstrings: []string{
				"Input Data (deep_key):",
				"level2",
				"level3",
				"nested_item_1",
				"nested_item_2",
				"42.5",
				"true",
			},
			unexpectedSubstrings: []string{
				"ignored_root_key",
				"ignore_me",
			},
		},
		{
			name: "SpecialCharactersInKeysAndValues",
			stateMap: map[string]any{
				"special_key_🔥": map[string]any{
					"quote_field":   `He said "Hello" & Goodbye \ \n \r \t`,
					"unicode_field": "日本語 / 中文 / Emoji 🚀⚡",
					"empty_str":     "",
				},
			},
			inferenceInputKey: "special_key_🔥",
			expectedSubstrings: []string{
				"Input Data (special_key_🔥):",
				`He said \"Hello\"`,
				"Goodbye",
				"日本語 / 中文 / Emoji 🚀⚡",
			},
		},
		{
			name: "PrimitiveValuesInInferenceInputKey",
			stateMap: map[string]any{
				"bool_key":  true,
				"num_key":   float64(987.65),
				"str_key":   "simple string payload",
				"array_key": []any{"item_a", "item_b", float64(3)},
				"null_val":  nil,
			},
			inferenceInputKey: "array_key",
			expectedSubstrings: []string{
				"Input Data (array_key):",
				`["item_a","item_b",3]`,
			},
			unexpectedSubstrings: []string{
				"bool_key",
				"num_key",
			},
		},
		{
			name: "NullValueInInputKey",
			stateMap: map[string]any{
				"null_key": nil,
			},
			inferenceInputKey: "null_key",
			expectedSubstrings: []string{
				"Input Data (null_key):",
				"null",
			},
		},
		{
			name: "EmptyInferenceInputKey",
			stateMap: map[string]any{
				"extracted_doc": "some_data",
			},
			inferenceInputKey: "",
			unexpectedSubstrings: []string{
				"Input Data",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var funcStruct *structpb.Struct
			if tt.stateMap != nil {
				var err error
				funcStruct, err = structpb.NewStruct(tt.stateMap)
				if err != nil {
					t.Fatalf("[%s] failed to create structpb: %v", tt.name, err)
				}
			}

			req := &pb.ExecuteWorkflowNodeRequest{
				NodeConfig: &pb.NodeConfiguration{
					InferenceInputKey: tt.inferenceInputKey,
				},
				GlobalState: &pb.GlobalState{Functional: funcStruct},
			}
			parser := workflowparser.NewNodeRequestParser(req)

			prompt := buildPrompt(parser, nil, "")

			for _, sub := range tt.expectedSubstrings {
				if !strings.Contains(prompt, sub) {
					t.Errorf("[%s] expected prompt to contain %q, but got:\n%s", tt.name, sub, prompt)
				}
			}

			for _, sub := range tt.unexpectedSubstrings {
				if strings.Contains(prompt, sub) {
					t.Errorf("[%s] unexpected substring %q found in prompt:\n%s", tt.name, sub, prompt)
				}
			}
		})
	}
}

