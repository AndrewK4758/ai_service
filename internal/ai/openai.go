package ai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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
	Type string `json:"type"`
}

type openAIRequest struct {
	Model          string                `json:"model"`
	Messages       []openAIMessage       `json:"messages"`
	Temperature    float32               `json:"temperature"`
	MaxTokens      int                   `json:"max_tokens,omitempty"`
	ResponseFormat *openAIResponseFormat `json:"response_format,omitempty"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func (o *OpenAIAnalyzer) callOpenAI(ctx context.Context, reqBody openAIRequest) (string, error) {
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal openai request: %w", err)
	}

	url := fmt.Sprintf("%s/chat/completions", strings.TrimSuffix(o.config.BaseURL, "/"))

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

	logger.Info("AI_Service", "[Ai Service] Sending request to %s with model %s", url, reqBody.Model)

	resp, err := o.config.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("LLM inference timed out or failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read openai response body: %w", err)
	}

	logger.Info("AI_Service", "[Ai Service] Received response status %d", resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
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

func (o *OpenAIAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)

	m := modelID
	if m == "" {
		m = o.config.TextModel
	}

	reqBody := openAIRequest{
		Model: m,
		Messages: []openAIMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
		Temperature: o.config.Temperature,
		MaxTokens:   o.config.MaxTokens,
		ResponseFormat: &openAIResponseFormat{
			Type: "json_object",
		},
	}

	return o.callOpenAI(ctx, reqBody)
}

func (o *OpenAIAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(workflowCtx, action, "(See attached image(s))")

	var contentArray []map[string]any
	contentArray = append(contentArray, map[string]any{
		"type": "text",
		"text": prompt,
	})

	for _, img := range imageBytes {
		base64Img := base64.StdEncoding.EncodeToString(img)
		contentArray = append(contentArray, map[string]any{
			"type": "image_url",
			"image_url": map[string]any{
				"url": fmt.Sprintf("data:image/jpeg;base64,%s", base64Img),
			},
		})
	}

	m := modelID
	if m == "" {
		m = o.config.VisionModel
	}

	reqBody := openAIRequest{
		Model: m,
		Messages: []openAIMessage{
			{
				Role:    "user",
				Content: contentArray,
			},
		},
		Temperature: o.config.Temperature,
		MaxTokens:   o.config.MaxTokens,
		ResponseFormat: &openAIResponseFormat{
			Type: "json_object",
		},
	}

	return o.callOpenAI(ctx, reqBody)
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
		m = o.config.EmbeddingModel
	}
	if m == "" {
		m = envutil.GetEmbeddingModel()
	}

	reqBody := openAIEmbeddingRequest{
		Model: m,
		Input: text,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal openai request: %w", err)
	}

	url := fmt.Sprintf("%s/embeddings", strings.TrimSuffix(o.config.BaseURL, "/"))
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
