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

func TestOpenAIAnalyzer_AnalyzeText(t *testing.T) {
	tests := []struct {
		name           string
		mockStatusCode int
		mockRespBody   openAIResponse
		mockErrorMsg   string
		text           string
		wantResult     string
		wantErr        bool
	}{
		{
			name:           "Success",
			mockStatusCode: http.StatusOK,
			mockRespBody: openAIResponse{
				Choices: []struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				}{
					{
						Message: struct {
							Content string `json:"content"`
						}{
							Content: "```json\n{\"foo\":\"bar\"}\n```",
						},
					},
				},
			},
			text:       "test doc",
			wantResult: "{\"foo\":\"bar\"}",
			wantErr:    false,
		},
		{
			name:           "Error - 500 status",
			mockStatusCode: http.StatusInternalServerError,
			mockErrorMsg:   "internal error",
			text:           "test doc",
			wantResult:     "",
			wantErr:        true,
		},
		{
			name:           "Error - empty choices",
			mockStatusCode: http.StatusOK,
			mockRespBody:   openAIResponse{},
			text:           "test doc",
			wantResult:     "",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/chat/completions" {
					t.Errorf("Expected path /chat/completions, got %s", r.URL.Path)
				}
				if r.Method != "POST" {
					t.Errorf("Expected method POST, got %s", r.Method)
				}

				w.WriteHeader(tt.mockStatusCode)
				if tt.mockErrorMsg != "" {
					w.Write([]byte(tt.mockErrorMsg))
				} else {
					json.NewEncoder(w).Encode(tt.mockRespBody)
				}
			}))
			defer server.Close()

			config := DefaultLocalConfig()
			config.BaseURL = server.URL
			config.TextModel = "test-text-model"
			config.VisionModel = "test-vision-model"
			analyzer := NewOpenAIAnalyzerWithConfig(config)

			ctx := context.Background()
			action := &pb.ActionDefinition{Name: "test"}
			got, err := analyzer.AnalyzeText(ctx, tt.text, nil, action, "test-text-model")

			if (err != nil) != tt.wantErr {
				t.Errorf("OpenAIAnalyzer.AnalyzeText() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantResult {
				t.Errorf("OpenAIAnalyzer.AnalyzeText() = %v, want %v", got, tt.wantResult)
			}
		})
	}
}

func TestOpenAIAnalyzer_AnalyzeImage(t *testing.T) {
	// Simple sanity check for vision payload wrapping
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		resp := openAIResponse{}
		resp.Choices = append(resp.Choices, struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		}{
			Message: struct {
				Content string `json:"content"`
			}{
				Content: "{\"vision\":\"ok\"}",
			},
		})
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	config := DefaultLocalConfig()
	config.BaseURL = server.URL
	config.TextModel = "test-text-model"
	config.VisionModel = "test-vision-model"
	analyzer := NewOpenAIAnalyzerWithConfig(config)

	got, err := analyzer.AnalyzeImage(context.Background(), [][]byte{[]byte("fakeimage")}, nil, nil, "test-vision-model")
	if err != nil {
		t.Errorf("OpenAIAnalyzer.AnalyzeImage() unexpected error = %v", err)
	}
	if !strings.Contains(got, "vision") {
		t.Errorf("OpenAIAnalyzer.AnalyzeImage() expected 'vision', got %v", got)
	}
}

func TestOpenAIAnalyzer_AnalyzeImage_VisionBaseURL(t *testing.T) {
	textServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("text server should not receive vision requests")
	}))
	defer textServer.Close()

	visionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("Expected path /chat/completions, got %s", r.URL.Path)
		}
		resp := openAIResponse{}
		resp.Choices = append(resp.Choices, struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		}{
			Message: struct {
				Content string `json:"content"`
			}{
				Content: `{"vision":"routed_to_vision_host"}`,
			},
		})
		json.NewEncoder(w).Encode(resp)
	}))
	defer visionServer.Close()

	config := DefaultLocalConfig()
	config.BaseURL = textServer.URL
	config.VisionBaseURL = visionServer.URL
	config.VisionModel = "glm-ocr"
	analyzer := NewOpenAIAnalyzerWithConfig(config)

	got, err := analyzer.AnalyzeImage(context.Background(), [][]byte{[]byte("fakeimage")}, nil, nil, "glm-ocr")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "routed_to_vision_host") {
		t.Errorf("expected vision response routed to vision server, got: %s", got)
	}
}
