package ai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
)

func init() {
	os.Setenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
	os.Setenv("OPENAI_BASE_PROMPT", "You are a helpful AI assistant.")
	os.Setenv("DEFAULT_VISION_PROMPT", "Describe the image.")
}

func TestAnthropicAnalyzer_AnalyzeText(t *testing.T) {
	var capturedHeaderKey string
	var capturedHeaderVer string
	var capturedReqBody anthropicRequest

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaderKey = r.Header.Get("x-api-key")
		capturedHeaderVer = r.Header.Get("anthropic-version")

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}
		if err := json.Unmarshal(bodyBytes, &capturedReqBody); err != nil {
			t.Fatalf("failed to unmarshal request body: %v", err)
		}

		resp := anthropicResponse{
			ID:   "msg_123",
			Type: "message",
			Role: "assistant",
			Content: []anthropicResponseContent{
				{
					Type: "text",
					Text: "```json\n{\"status\": \"success\", \"summary\": \"analyzed\"}\n```",
				},
			},
			Model: capturedReqBody.Model,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	config := OpenAIConfig{
		AnthropicAPIKey:   "test-key-123",
		AnthropicBaseURL:  ts.URL,
		AnthropicVersion:  "2023-06-01",
		AnthropicTextModel: "claude-3-5-sonnet-20241022",
		Temperature:       0.0,
		MaxTokens:         2048,
		HTTPClient:        ts.Client(),
	}

	analyzer, err := NewAnthropicAnalyzer(config)
	if err != nil {
		t.Fatalf("NewAnthropicAnalyzer failed: %v", err)
	}

	ctx := context.Background()
	req := &pb.ExecuteTaskRequest{}
	parser := req
	action := &pb.ActionDefinition{Name: "test_action"}

	// Test 1: Fallback model
	res, err := analyzer.AnalyzeText(ctx, "Sample document text", parser, action, "")
	if err != nil {
		t.Fatalf("AnalyzeText failed: %v", err)
	}

	if capturedHeaderKey != "test-key-123" {
		t.Errorf("expected x-api-key header 'test-key-123', got '%s'", capturedHeaderKey)
	}
	if capturedHeaderVer != "2023-06-01" {
		t.Errorf("expected anthropic-version header '2023-06-01', got '%s'", capturedHeaderVer)
	}
	if capturedReqBody.Model != "claude-3-5-sonnet-20241022" {
		t.Errorf("expected default model 'claude-3-5-sonnet-20241022', got '%s'", capturedReqBody.Model)
	}
	expectedJSON := `{"status": "success", "summary": "analyzed"}`
	if res != expectedJSON {
		t.Errorf("expected cleaned JSON '%s', got '%s'", expectedJSON, res)
	}

	// Test 2: Model override
	_, err = analyzer.AnalyzeText(ctx, "Sample text 2", parser, action, "claude-3-haiku-20240307")
	if err != nil {
		t.Fatalf("AnalyzeText with override failed: %v", err)
	}
	if capturedReqBody.Model != "claude-3-haiku-20240307" {
		t.Errorf("expected overridden model 'claude-3-haiku-20240307', got '%s'", capturedReqBody.Model)
	}
}

func TestAnthropicAnalyzer_AnalyzeImage(t *testing.T) {
	var capturedReqBody anthropicRequest

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}
		if err := json.Unmarshal(bodyBytes, &capturedReqBody); err != nil {
			t.Fatalf("failed to unmarshal request body: %v", err)
		}

		resp := anthropicResponse{
			ID:   "msg_vision_123",
			Type: "message",
			Role: "assistant",
			Content: []anthropicResponseContent{
				{
					Type: "text",
					Text: "{\"extracted_text\": \"invoice data\"}",
				},
			},
			Model: capturedReqBody.Model,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	config := OpenAIConfig{
		AnthropicAPIKey:    "test-key-vision",
		AnthropicBaseURL:   ts.URL,
		AnthropicVersion:   "2023-06-01",
		AnthropicVisionModel: "claude-3-5-sonnet-20241022",
		HTTPClient:         ts.Client(),
	}

	analyzer, err := NewAnthropicAnalyzer(config)
	if err != nil {
		t.Fatalf("NewAnthropicAnalyzer failed: %v", err)
	}

	ctx := context.Background()
	req := &pb.ExecuteTaskRequest{}
	parser := req
	action := &pb.ActionDefinition{Name: "vision_task"}
	images := [][]byte{[]byte("fake-jpeg-bytes")}

	res, err := analyzer.AnalyzeImage(ctx, images, parser, action, "")
	if err != nil {
		t.Fatalf("AnalyzeImage failed: %v", err)
	}

	if len(capturedReqBody.Messages) == 0 || len(capturedReqBody.Messages[0].Content) < 2 {
		t.Fatalf("expected message with at least text and image content blocks")
	}

	imgBlock := capturedReqBody.Messages[0].Content[1]
	if imgBlock.Type != "image" || imgBlock.Source == nil || imgBlock.Source.MediaType != "image/jpeg" {
		t.Errorf("image content block improperly formatted: %+v", imgBlock)
	}

	expectedJSON := `{"extracted_text": "invoice data"}`
	if res != expectedJSON {
		t.Errorf("expected cleaned JSON '%s', got '%s'", expectedJSON, res)
	}
}

func TestAnthropicAnalyzer_ErrorHandling(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error": {"type": "rate_limit_error", "message": "Rate limit exceeded"}}`))
	}))
	defer ts.Close()

	config := OpenAIConfig{
		AnthropicAPIKey:  "key",
		AnthropicBaseURL: ts.URL,
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
	}
	analyzer, _ := NewAnthropicAnalyzer(config)

	ctx := context.Background()
	_, err := analyzer.AnalyzeText(ctx, "text", nil, nil, "")
	if err == nil {
		t.Fatalf("expected error for 429 status code, got nil")
	}
}

func TestAnthropicAnalyzer_ConfigFallbacks(t *testing.T) {
	// Test AuthToken fallback when AnthropicAPIKey is empty
	config := OpenAIConfig{
		AuthToken: "fallback-auth-token",
		TextModel: "claude-3-haiku-20240307",
	}

	analyzer, err := NewAnthropicAnalyzer(config)
	if err != nil {
		t.Fatalf("NewAnthropicAnalyzer failed: %v", err)
	}

	if analyzer.apiKey != "fallback-auth-token" {
		t.Errorf("expected apiKey 'fallback-auth-token', got '%s'", analyzer.apiKey)
	}
	if analyzer.baseURL != "https://api.anthropic.com/v1" {
		t.Errorf("expected default baseURL 'https://api.anthropic.com/v1', got '%s'", analyzer.baseURL)
	}
	if analyzer.version != "2023-06-01" {
		t.Errorf("expected default version '2023-06-01', got '%s'", analyzer.version)
	}
	if analyzer.textModel != "claude-3-haiku-20240307" {
		t.Errorf("expected textModel 'claude-3-haiku-20240307', got '%s'", analyzer.textModel)
	}
}

func TestAnthropicAnalyzer_EmptyContent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "msg_empty", "type": "message", "role": "assistant", "content": []}`))
	}))
	defer ts.Close()

	config := OpenAIConfig{
		AnthropicAPIKey:  "key",
		AnthropicBaseURL: ts.URL,
		HTTPClient:       ts.Client(),
	}
	analyzer, _ := NewAnthropicAnalyzer(config)

	_, err := analyzer.AnalyzeText(context.Background(), "text", nil, nil, "")
	if err == nil {
		t.Fatalf("expected error for empty content slice, got nil")
	}
}

func TestAnthropicAnalyzer_AnalyzeImage_MultipleImages(t *testing.T) {
	var capturedReqBody anthropicRequest

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		if err := json.Unmarshal(bodyBytes, &capturedReqBody); err != nil {
			t.Fatalf("failed to unmarshal body: %v", err)
		}

		resp := anthropicResponse{
			ID:   "msg_multi",
			Type: "message",
			Role: "assistant",
			Content: []anthropicResponseContent{
				{Type: "text", Text: "{\"page_count\": 2}"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	config := OpenAIConfig{
		AnthropicAPIKey:  "key",
		AnthropicBaseURL: ts.URL,
		HTTPClient:       ts.Client(),
	}
	analyzer, _ := NewAnthropicAnalyzer(config)

	images := [][]byte{[]byte("img1"), []byte("img2")}
	res, err := analyzer.AnalyzeImage(context.Background(), images, nil, nil, "")
	if err != nil {
		t.Fatalf("AnalyzeImage failed: %v", err)
	}

	if len(capturedReqBody.Messages) == 0 || len(capturedReqBody.Messages[0].Content) != 3 {
		t.Fatalf("expected text prompt + 2 image content blocks, got %d blocks", len(capturedReqBody.Messages[0].Content))
	}
	if res != `{"page_count": 2}` {
		t.Errorf("unexpected res: %s", res)
	}
}
