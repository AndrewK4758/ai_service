package ai

import (
	"encoding/json"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNormalizeVisionOutput_MultiLineTableCells(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"fee_schedule": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"description":   map[string]any{"type": "string"},
						"borrower_paid": map[string]any{"type": "string"},
						"seller_paid":   map[string]any{"type": "string"},
					},
				},
			},
		},
	}
	schemaStruct, err := structpb.NewStruct(schema)
	if err != nil {
		t.Fatalf("failed to create schema struct: %v", err)
	}

	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": schemaStruct.AsMap(),
		},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: payloadStruct,
	}

	inputJSON := `{
		"fee_schedule": [
			{
				"description": "Title - Settlement or Closing Fee\nFirst American Title",
				"borrower_paid": "$750.00",
				"seller_paid": ""
			},
			{
				"description": "Owner's Title Insurance   \n   (optional)",
				"borrower_paid": "null",
				"seller_paid": "$1,200.00",
				"hallucinated_column": "extra"
			}
		],
		"extra_root_field": "remove_me"
	}`

	normalized := NormalizeVisionOutput(inputJSON, req)

	var result map[string]any
	if err := json.Unmarshal([]byte(normalized), &result); err != nil {
		t.Fatalf("failed to unmarshal normalized output: %v", err)
	}

	// Root extra field should be removed
	if _, exists := result["extra_root_field"]; exists {
		t.Errorf("expected extra_root_field to be stripped")
	}

	fees, ok := result["fee_schedule"].([]any)
	if !ok || len(fees) != 2 {
		t.Fatalf("expected 2 fee items, got %v", result["fee_schedule"])
	}

	item1, ok := fees[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map for item1")
	}
	if item1["description"] != "Title - Settlement or Closing Fee First American Title" {
		t.Errorf("expected multi-line merged description, got %q", item1["description"])
	}
	if item1["seller_paid"] != nil {
		t.Errorf("expected empty string seller_paid to be normalized to nil, got %v", item1["seller_paid"])
	}

	item2, ok := fees[1].(map[string]any)
	if !ok {
		t.Fatalf("expected map for item2")
	}
	if item2["description"] != "Owner's Title Insurance (optional)" {
		t.Errorf("expected multi-line merged description with collapsed whitespace, got %q", item2["description"])
	}
	if item2["borrower_paid"] != nil {
		t.Errorf("expected 'null' string borrower_paid to be normalized to nil, got %v", item2["borrower_paid"])
	}
	if _, exists := item2["hallucinated_column"]; exists {
		t.Errorf("expected hallucinated_column to be stripped from item2")
	}
}

func TestNormalizeVisionOutput_CheckboxMatrices(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"is_borrower_paid": map[string]any{"type": "boolean"},
			"is_seller_paid":   map[string]any{"type": "boolean"},
			"is_financed":      map[string]any{"type": "boolean"},
			"is_waived":        map[string]any{"type": "boolean"},
		},
	}
	schemaStruct, _ := structpb.NewStruct(schema)
	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": schemaStruct.AsMap(),
		},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: payloadStruct,
	}

	inputJSON := `{
		"is_borrower_paid": "[X]",
		"is_seller_paid": "unchecked",
		"is_financed": "blank",
		"is_waived": false
	}`

	normalized := NormalizeVisionOutput(inputJSON, req)

	var result map[string]any
	if err := json.Unmarshal([]byte(normalized), &result); err != nil {
		t.Fatalf("failed to unmarshal normalized output: %v", err)
	}

	if result["is_borrower_paid"] != true {
		t.Errorf("expected is_borrower_paid to be true, got %v", result["is_borrower_paid"])
	}
	if result["is_seller_paid"] != false {
		t.Errorf("expected is_seller_paid to be false, got %v", result["is_seller_paid"])
	}
	if result["is_financed"] != nil {
		t.Errorf("expected is_financed to be nil, got %v", result["is_financed"])
	}
	if result["is_waived"] != false {
		t.Errorf("expected is_waived to be false, got %v", result["is_waived"])
	}
}

func TestNormalizeVisionOutput_StackedKeyValues(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"borrower_name": map[string]any{"type": "string"},
			"property_address": map[string]any{"type": "string"},
			"loan_amount": map[string]any{"type": "string"},
		},
	}
	schemaStruct, _ := structpb.NewStruct(schema)
	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": schemaStruct.AsMap(),
		},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: payloadStruct,
	}

	inputJSON := `{
		"borrower_name": "Borrower:\nJane Doe",
		"property_address": "123 Main Street\nSuite 400\nAtlanta, GA 30301",
		"loan_amount": "$450,000.00"
	}`

	normalized := NormalizeVisionOutput(inputJSON, req)

	var result map[string]any
	if err := json.Unmarshal([]byte(normalized), &result); err != nil {
		t.Fatalf("failed to unmarshal normalized output: %v", err)
	}

	if result["borrower_name"] != "Borrower: Jane Doe" {
		t.Errorf("expected merged borrower_name, got %q", result["borrower_name"])
	}
	if result["property_address"] != "123 Main Street Suite 400 Atlanta, GA 30301" {
		t.Errorf("expected merged property_address, got %q", result["property_address"])
	}
	if result["loan_amount"] != "$450,000.00" {
		t.Errorf("expected loan_amount $450,000.00, got %v", result["loan_amount"])
	}
}

func TestNormalizeJsonOutput_ConstrainedZeroCreativity(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"borrower_name": map[string]any{"type": "string"},
			"loan_amount":   map[string]any{"type": "number"},
			"interest_rate": map[string]any{"type": "number"},
			"is_refinance":  map[string]any{"type": "boolean"},
			"blank_field":   map[string]any{"type": "string"},
		},
	}
	schemaStruct, _ := structpb.NewStruct(schema)
	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": schemaStruct.AsMap(),
		},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: payloadStruct,
	}

	inputJSON := `{
		"borrower_name": "  John   Doe  ",
		"loan_amount": "500000.50",
		"interest_rate": "",
		"is_refinance": "true",
		"blank_field": "null",
		"hallucinated_property": "should_be_stripped"
	}`

	normalized := NormalizeJsonOutput(inputJSON, req)

	var result map[string]any
	if err := json.Unmarshal([]byte(normalized), &result); err != nil {
		t.Fatalf("failed to unmarshal normalized output: %v", err)
	}

	if result["borrower_name"] != "John Doe" {
		t.Errorf("expected borrower_name 'John Doe', got %v", result["borrower_name"])
	}
	if result["loan_amount"] != 500000.50 {
		t.Errorf("expected loan_amount 500000.50, got %v", result["loan_amount"])
	}
	if result["interest_rate"] != nil {
		t.Errorf("expected interest_rate nil, got %v", result["interest_rate"])
	}
	if result["is_refinance"] != true {
		t.Errorf("expected is_refinance true, got %v", result["is_refinance"])
	}
	if result["blank_field"] != nil {
		t.Errorf("expected blank_field nil, got %v", result["blank_field"])
	}
	if _, exists := result["hallucinated_property"]; exists {
		t.Errorf("expected hallucinated_property to be stripped")
	}
}

