package ai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	_ "image/jpeg"
	"image/png"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/image/draw"
	"golang.org/x/image/tiff"

	"github.com/AndrewK4758/shared_utils/envutil"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/workflowparser"
)

func getInferenceTimeout() time.Duration {
	mins := envutil.MustGetInt("OLLAMA_TIMEOUT_MINUTES")
	return time.Duration(mins) * time.Minute
}

type APIType string

const (
	APITypeOpenAI APIType = "OPEN_AI"
	APITypeAzure  APIType = "AZURE"
	APITypeLocal  APIType = "LOCAL"
)

type OpenAIConfig struct {
	AuthToken            string
	BaseURL              string
	VisionBaseURL        string
	EmbeddingBaseURL     string
	APIType              APIType
	APIVersion           string
	HTTPClient           *http.Client
	TextModel            string
	VisionModel          string
	EmbeddingModel       string
	Temperature          float32
	MaxTokens            int
	AnthropicAPIKey      string
	AnthropicBaseURL     string
	AnthropicVersion     string
	AnthropicTextModel   string
	AnthropicVisionModel string
}

func DefaultConfig(authToken string) OpenAIConfig {
	return OpenAIConfig{
		AuthToken:   authToken,
		BaseURL:     "https://api.openai.com/v1",
		APIType:     APITypeOpenAI,
		HTTPClient:  &http.Client{Timeout: getInferenceTimeout()},
		Temperature: 0.0,
		MaxTokens:   4096,
	}
}

func DefaultLocalConfig() OpenAIConfig {
	config := DefaultConfig("EMPTY")
	config.BaseURL = "http://localhost:11434/v1"
	config.APIType = APITypeLocal
	return config
}

// OpenAIAnalyzer implements the Analyzer interface for OpenAI-compatible endpoints (like vLLM)
type OpenAIAnalyzer struct {
	config OpenAIConfig
}

func NewOpenAIAnalyzerWithConfig(config OpenAIConfig) *OpenAIAnalyzer {
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: getInferenceTimeout()}
	}
	return &OpenAIAnalyzer{
		config: config,
	}
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"` // can be string or array of objects for vision
}

type openAIResponseFormat struct {
	Type       string            `json:"type"`
	JSONSchema *openAIJSONSchema `json:"json_schema,omitempty"`
}

type openAIJSONSchema struct {
	Name   string `json:"name"`
	Strict bool   `json:"strict"`
	Schema any    `json:"schema"`
}

type openAIRequest struct {
	Model          string                `json:"model"`
	Messages       []openAIMessage       `json:"messages"`
	Temperature    float32               `json:"temperature"`
	MaxTokens      int                   `json:"max_tokens,omitempty"`
	ResponseFormat *openAIResponseFormat `json:"response_format,omitempty"`
}

type TokenUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage TokenUsage `json:"usage"`
}

func (o *OpenAIAnalyzer) callOpenAI(ctx context.Context, reqBody openAIRequest, baseURLOverride ...string) (string, error) {
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal openai request: %w", err)
	}

	baseURL := o.config.BaseURL
	if len(baseURLOverride) > 0 && baseURLOverride[0] != "" {
		baseURL = baseURLOverride[0]
	}
	url := fmt.Sprintf("%s/chat/completions", strings.TrimSuffix(baseURL, "/"))

	// Create context using the HTTP client's timeout for safety, default to 5m if 0
	timeout := o.config.HTTPClient.Timeout
	if timeout == 0 {
		timeout = getInferenceTimeout()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if o.config.APIType == APITypeAzure {
		req.Header.Set("api-key", o.config.AuthToken)
	} else {
		req.Header.Set("Authorization", "Bearer "+o.config.AuthToken)
	}

	logger.Info("AI_Service", "[Ai Service] Sending request to %s with model %s (Payload Size: %d bytes)", url, reqBody.Model, len(jsonData))
	logger.Debug("AI_Service", "[Ai Service] Request Body: %s", string(jsonData))

	resp, err := o.config.HTTPClient.Do(req)
	if err != nil {
		logger.Error("AI_Service", "[Ai Service] Request Failed. Request Body: %s", string(jsonData))
		return "", fmt.Errorf("LLM inference timed out or failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read openai response body: %w", err)
	}

	logger.Info("AI_Service", "[Ai Service] Received response status %d (Response Size: %d bytes)", resp.StatusCode, len(bodyBytes))
	logger.Debug("AI_Service", "[Ai Service] Raw Response Body: %s", string(bodyBytes))

	if resp.StatusCode != http.StatusOK {
		logger.Error("AI_Service", "[Ai Service] Request Failed with status %d. Request Body: %s, Response Body: %s", resp.StatusCode, string(jsonData), string(bodyBytes))
		return "", fmt.Errorf("openai endpoint returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var oaiResp openAIResponse
	if err := json.Unmarshal(bodyBytes, &oaiResp); err != nil {
		return "", fmt.Errorf("failed to decode openai response: %w", err)
	}

	if len(oaiResp.Choices) == 0 {
		return "", fmt.Errorf("openai endpoint returned no choices")
	}

	response := jsonclean.CleanJSON(oaiResp.Choices[0].Message.Content)

	return response, nil
}

func (o *OpenAIAnalyzer) AnalyzeText(ctx context.Context, text string, req *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(req, action, text)
	logger.Debug("AI_Service", "[Ai Service] Built Prompt before sending:\n%s\n", prompt)

	m := modelID
	if m == "" {
		panic(fmt.Errorf("missing modelID in OpenAI AnalyzeText"))
	}

	reqBody := openAIRequest{
		Model: m,
		Messages: []openAIMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
		Temperature:    o.config.Temperature,
		MaxTokens:      o.config.MaxTokens,
		ResponseFormat: buildResponseFormat(req),
	}

	resp, err := o.callOpenAI(ctx, reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to call OpenAI for text analysis: %w", err)
	}
	return enforceDeterministicValidation(resp, req), nil
}

func isTIFF(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return (data[0] == 'I' && data[1] == 'I' && data[2] == 0x2A && data[3] == 0x00) ||
		(data[0] == 'M' && data[1] == 'M' && data[2] == 0x00 && data[3] == 0x2A)
}

func isPNG(data []byte) bool {
	if len(data) < 8 {
		return false
	}
	return data[0] == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G' &&
		data[4] == 0x0D && data[5] == 0x0A && data[6] == 0x1A && data[7] == 0x0A
}

func isJPEG(data []byte) bool {
	if len(data) < 3 {
		return false
	}
	return data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF
}

const maxVisionDimension = 1536

func scaleImageIfExceedsMax(img image.Image, maxDim int) image.Image {
	bounds := img.Bounds()
	w := bounds.Dx()
	h := bounds.Dy()
	if w <= maxDim && h <= maxDim {
		return img
	}

	var newW, newH int
	if w > h {
		newW = maxDim
		newH = int(float64(h) * (float64(maxDim) / float64(w)))
	} else {
		newH = maxDim
		newW = int(float64(w) * (float64(maxDim) / float64(h)))
	}
	if newW < 1 {
		newW = 1
	}
	if newH < 1 {
		newH = 1
	}

	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))
	draw.BiLinear.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)
	return dst
}

func normalizeImageForVision(imgBytes []byte) (string, string, error) {
	var img image.Image
	var err error

	if isTIFF(imgBytes) {
		img, err = tiff.Decode(bytes.NewReader(imgBytes))
		if err != nil {
			return "", "", fmt.Errorf("failed to decode TIFF image: %w", err)
		}
	} else if isPNG(imgBytes) || isJPEG(imgBytes) {
		img, _, err = image.Decode(bytes.NewReader(imgBytes))
		if err != nil {
			return "", "", fmt.Errorf("failed to decode image: %w", err)
		}
	} else {
		var format string
		img, format, err = image.Decode(bytes.NewReader(imgBytes))
		if err != nil {
			return "", "", fmt.Errorf("unsupported or corrupted image format (%s): %w", format, err)
		}
	}

	img = scaleImageIfExceedsMax(img, maxVisionDimension)

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return "", "", fmt.Errorf("failed to encode normalized image to PNG: %w", err)
	}
	return "image/png", base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func (o *OpenAIAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, req *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(req, action, "(See attached image(s))")

	var contentArray []map[string]any
	contentArray = append(contentArray, map[string]any{
		"type": "text",
		"text": prompt,
	})

	for _, img := range imageBytes {
		mimeType, base64Img, err := normalizeImageForVision(img)
		if err != nil {
			return "", fmt.Errorf("failed to normalize image for vision: %w", err)
		}
		contentArray = append(contentArray, map[string]any{
			"type": "image_url",
			"image_url": map[string]any{
				"url": fmt.Sprintf("data:%s;base64,%s", mimeType, base64Img),
			},
		})
	}

	m := modelID
	if m == "" {
		panic(fmt.Errorf("missing modelID in OpenAI AnalyzeImage"))
	}

	reqBody := openAIRequest{
		Model: m,
		Messages: []openAIMessage{
			{
				Role:    "user",
				Content: contentArray,
			},
		},
		Temperature:    o.config.Temperature,
		MaxTokens:      o.config.MaxTokens,
		ResponseFormat: buildResponseFormat(req),
	}

	resp, err := o.callOpenAI(ctx, reqBody, o.config.VisionBaseURL)
	if err != nil {
		return "", fmt.Errorf("failed to call OpenAI for image analysis: %w", err)
	}
	return enforceDeterministicValidation(resp, req), nil
}

type openAIEmbeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type openAIEmbeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (o *OpenAIAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	m := modelID
	if m == "" {
		panic(fmt.Errorf("missing modelID in OpenAI GenerateEmbedding"))
	}

	reqBody := openAIEmbeddingRequest{
		Model: m,
		Input: text,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal openai request: %w", err)
	}

	baseURL := o.config.BaseURL
	if o.config.EmbeddingBaseURL != "" {
		baseURL = o.config.EmbeddingBaseURL
	}
	url := fmt.Sprintf("%s/embeddings", strings.TrimSuffix(baseURL, "/"))
	logger.Info("AI_Service", "[Ai Service] Sending request to %s with model %s", url, m)

	timeout := o.config.HTTPClient.Timeout
	if timeout == 0 {
		timeout = getInferenceTimeout()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if o.config.APIType == APITypeAzure {
		req.Header.Set("api-key", o.config.AuthToken)
	} else {
		req.Header.Set("Authorization", "Bearer "+o.config.AuthToken)
	}

	resp, err := o.config.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("LLM embedding timed out or failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read openai response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openai endpoint returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var oaiResp openAIEmbeddingResponse
	if err := json.Unmarshal(bodyBytes, &oaiResp); err != nil {
		return nil, fmt.Errorf("failed to decode openai response: %w", err)
	}

	if len(oaiResp.Data) == 0 {
		return nil, fmt.Errorf("openai endpoint returned no embeddings")
	}

	return oaiResp.Data[0].Embedding, nil
}

func buildResponseFormat(req *pb.ExecuteTaskRequest) *openAIResponseFormat {
	schemaObj := extractSchemaMap(req)

	return &openAIResponseFormat{
		Type: "json_schema",
		JSONSchema: &openAIJSONSchema{
			Name:   "output_schema",
			Strict: true,
			Schema: schemaObj,
		},
	}
}

func extractSchemaMap(req *pb.ExecuteTaskRequest) map[string]any {
	if req == nil || req.Payload == nil {
		panic(fmt.Errorf("missing required ExpectedSchemaStruct in payload"))
	}

	parser := workflowparser.NewTaskRequestParser(req)
	payload := parser.MustGetExecuteTaskPayload()
	if payload.NodeConfig == nil || payload.NodeConfig.ExpectedSchemaStruct == nil {
		panic(fmt.Errorf("missing required ExpectedSchemaStruct in payload"))
	}

	return payload.NodeConfig.ExpectedSchemaStruct.AsMap()
}

func enforceDeterministicValidation(responseStr string, req *pb.ExecuteTaskRequest) string {
	return NormalizeVisionOutput(responseStr, req)
}
