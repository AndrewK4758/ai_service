package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
)

func TestBuildPrompt(t *testing.T) {
	workflowCtx := &pb.WorkflowContext{
		CurrentStep: &pb.StepDefinition{
			CustomInstructions: "Ignore everything else",
			ExpectedSchema:     `{"type": "object"}`,
		},
	}
	action := &pb.ActionDefinition{
		Name: "Extract Invoice",
	}
	content := "Invoice 123"

	prompt := buildPrompt(workflowCtx, action, content)

	if !strings.Contains(prompt, "Extract Invoice") {
		t.Error("Prompt missing Action Name")
	}
	if !strings.Contains(prompt, "Ignore everything else") {
		t.Error("Prompt missing CustomInstructions")
	}
	if !strings.Contains(prompt, `{"type": "object"}`) {
		t.Error("Prompt missing JsonSchema")
	}
	if !strings.Contains(prompt, "Invoice 123") {
		t.Error("Prompt missing content")
	}
}

func TestOllamaAnalyzer_AnalyzeText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/generate" {
			t.Errorf("Expected /api/generate, got %s", r.URL.Path)
		}

		var req ollamaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}

		if req.Model != "test-model" {
			t.Errorf("Expected model 'test-model', got %s", req.Model)
		}

		resp := ollamaResponse{Response: "```json\n{\"test\": 123}\n```"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	analyzer := NewOllamaAnalyzer(server.URL, "test-model", "vision-model")
	workflowCtx := &pb.WorkflowContext{}
	action := &pb.ActionDefinition{}

	result, err := analyzer.AnalyzeText(context.Background(), "hello", workflowCtx, action)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result != `{"test": 123}` {
		t.Errorf("Expected stripped JSON, got %s", result)
	}
}

func TestOllamaAnalyzer_AnalyzeImage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ollamaRequest
		json.NewDecoder(r.Body).Decode(&req)

		if len(req.Images) != 1 {
			t.Errorf("Expected 1 image, got %d", len(req.Images))
		}
		if req.Model != "vision-model" {
			t.Errorf("Expected 'vision-model', got %s", req.Model)
		}

		resp := ollamaResponse{Response: `{"image": "parsed"}`}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	analyzer := NewOllamaAnalyzer(server.URL, "test-model", "vision-model")
	workflowCtx := &pb.WorkflowContext{}
	action := &pb.ActionDefinition{}

	result, err := analyzer.AnalyzeImage(context.Background(), [][]byte{[]byte("fake-image")}, workflowCtx, action)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result != `{"image": "parsed"}` {
		t.Errorf("Expected JSON, got %s", result)
	}
}
