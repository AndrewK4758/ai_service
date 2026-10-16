package ai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/jpeg"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/image/tiff"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
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
			
			mockSchema, _ := structpb.NewStruct(map[string]any{"properties": map[string]any{"foo": map[string]any{}}})
			payloadStruct, _ := structpb.NewStruct(map[string]any{
				"node_config": map[string]any{
					"expected_schema_struct": mockSchema.AsMap(),
				},
				"global_state": map[string]any{},
				"dynamic_input": map[string]any{},
			})
			req := &pb.ExecuteTaskRequest{
				Payload: payloadStruct,
			}
			
			got, err := analyzer.AnalyzeText(ctx, tt.text, req, action, "test-text-model")

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

	mockSchema, _ := structpb.NewStruct(map[string]any{"properties": map[string]any{"vision": map[string]any{}}})
	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": mockSchema.AsMap(),
		},
		"global_state":  map[string]any{},
		"dynamic_input": map[string]any{},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: payloadStruct,
	}
	var validPng bytes.Buffer
	_ = png.Encode(&validPng, image.NewRGBA(image.Rect(0, 0, 1, 1)))
	got, err := analyzer.AnalyzeImage(context.Background(), [][]byte{validPng.Bytes()}, req, nil, "test-vision-model")
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

	mockSchema, _ := structpb.NewStruct(map[string]any{"properties": map[string]any{"vision": map[string]any{}}})
	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": mockSchema.AsMap(),
		},
		"global_state":  map[string]any{},
		"dynamic_input": map[string]any{},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: payloadStruct,
	}
	var validPng bytes.Buffer
	_ = png.Encode(&validPng, image.NewRGBA(image.Rect(0, 0, 1, 1)))
	got, err := analyzer.AnalyzeImage(context.Background(), [][]byte{validPng.Bytes()}, req, nil, "glm-ocr")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "routed_to_vision_host") {
		t.Errorf("expected vision response routed to vision server, got: %s", got)
	}
}

func TestOpenAIAnalyzer_AnalyzeImage_TIFFTranscoding(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	var tiffBuf bytes.Buffer
	if err := tiff.Encode(&tiffBuf, img, nil); err != nil {
		t.Fatalf("failed to encode test TIFF: %v", err)
	}
	tiffBytes := tiffBuf.Bytes()

	var receivedDataURI string
	visionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body openAIRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		if len(body.Messages) > 0 {
			if contentList, ok := body.Messages[0].Content.([]any); ok {
				for _, item := range contentList {
					if itemMap, ok := item.(map[string]any); ok {
						if imgUrl, ok := itemMap["image_url"].(map[string]any); ok {
							receivedDataURI, _ = imgUrl["url"].(string)
						}
					}
				}
			}
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
				Content: `{"extracted":"tiff_success"}`,
			},
		})
		json.NewEncoder(w).Encode(resp)
	}))
	defer visionServer.Close()

	config := DefaultLocalConfig()
	config.VisionBaseURL = visionServer.URL
	analyzer := NewOpenAIAnalyzerWithConfig(config)

	mockSchema, _ := structpb.NewStruct(map[string]any{"properties": map[string]any{"extracted": map[string]any{}}})
	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"expected_schema_struct": mockSchema.AsMap(),
		},
		"global_state":  map[string]any{},
		"dynamic_input": map[string]any{},
	})
	req := &pb.ExecuteTaskRequest{
		Payload: payloadStruct,
	}

	got, err := analyzer.AnalyzeImage(context.Background(), [][]byte{tiffBytes}, req, nil, "glm-ocr")
	if err != nil {
		t.Fatalf("AnalyzeImage with TIFF failed: %v", err)
	}
	if !strings.Contains(got, "tiff_success") {
		t.Errorf("expected tiff_success, got %s", got)
	}
	if !strings.HasPrefix(receivedDataURI, "data:image/png;base64,") {
		t.Errorf("expected data URI to start with 'data:image/png;base64,', got %s", receivedDataURI[:min(len(receivedDataURI), 30)])
	}
}

func TestNormalizeImageForVision_Formats(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	var tiffBuf bytes.Buffer
	tiff.Encode(&tiffBuf, img, nil)
	mime, b64, err := normalizeImageForVision(tiffBuf.Bytes())
	if err != nil || mime != "image/png" || b64 == "" {
		t.Errorf("TIFF normalization failed: mime=%s, err=%v", mime, err)
	}

	var pngBuf bytes.Buffer
	png.Encode(&pngBuf, img)
	mime, b64, err = normalizeImageForVision(pngBuf.Bytes())
	if err != nil || mime != "image/png" || b64 == "" {
		t.Errorf("PNG normalization failed: mime=%s, err=%v", mime, err)
	}

	var jpegBuf bytes.Buffer
	if err := jpeg.Encode(&jpegBuf, img, nil); err != nil {
		t.Fatalf("failed to encode test JPEG: %v", err)
	}
	mime, b64, err = normalizeImageForVision(jpegBuf.Bytes())
	if err != nil || mime != "image/png" || b64 == "" {
		t.Errorf("JPEG normalization failed: mime=%s, err=%v", mime, err)
	}
}

func TestNormalizeImageForVision_DownscalesOversizedImage(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 2000, 3000))
	var pngBuf bytes.Buffer
	if err := png.Encode(&pngBuf, img); err != nil {
		t.Fatalf("failed to encode large image: %v", err)
	}

	mime, b64, err := normalizeImageForVision(pngBuf.Bytes())
	if err != nil {
		t.Fatalf("normalizeImageForVision failed on oversized image: %v", err)
	}
	if mime != "image/png" {
		t.Errorf("expected mime image/png, got %s", mime)
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("failed to decode base64: %v", err)
	}
	decodedImg, _, err := image.Decode(bytes.NewReader(decodedBytes))
	if err != nil {
		t.Fatalf("failed to decode scaled image: %v", err)
	}

	bounds := decodedImg.Bounds()
	if bounds.Dy() != maxVisionDimension {
		t.Errorf("expected height %d, got %d", maxVisionDimension, bounds.Dy())
	}
	expectedWidth := int(float64(2000) * (float64(maxVisionDimension) / float64(3000)))
	if bounds.Dx() != expectedWidth {
		t.Errorf("expected width %d, got %d", expectedWidth, bounds.Dx())
	}
}
