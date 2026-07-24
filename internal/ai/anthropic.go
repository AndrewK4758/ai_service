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

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
)

// AnthropicAnalyzer implements the Analyzer interface for Anthropic Messages REST API endpoints
type AnthropicAnalyzer struct {
	apiKey      string
	baseURL     string
	version     string
	textModel   string
	visionModel string
	temperature float32
	maxTokens   int
	httpClient  *http.Client
}

func NewAnthropicAnalyzer(config OpenAIConfig) (*AnthropicAnalyzer, error) {
	apiKey := config.AnthropicAPIKey
	if apiKey == "" {
		apiKey = config.AuthToken
	}

	baseURL := config.AnthropicBaseURL
	if baseURL == "" {
		baseURL = "https://api.anthropic.com/v1"
	}

	version := config.AnthropicVersion
	if version == "" {
		version = "2023-06-01"
	}

	textModel := config.AnthropicTextModel
	if textModel == "" {
		textModel = config.TextModel
	}
	if textModel == "" {
		textModel = "claude-3-5-sonnet-20241022"
	}

	visionModel := config.AnthropicVisionModel
	if visionModel == "" {
		visionModel = config.VisionModel
	}
	if visionModel == "" {
		visionModel = "claude-3-5-sonnet-20241022"
	}

	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: getInferenceTimeout()}
	}

	return &AnthropicAnalyzer{
		apiKey:      apiKey,
		baseURL:     baseURL,
		version:     version,
		textModel:   textModel,
		visionModel: visionModel,
		temperature: config.Temperature,
		maxTokens:   config.MaxTokens,
		httpClient:  client,
	}, nil
}

type anthropicContentBlock struct {
	Type   string                `json:"type"`
	Text   string                `json:"text,omitempty"`
	Source *anthropicImageSource `json:"source,omitempty"`
}

type anthropicImageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

type anthropicMessage struct {
	Role    string                  `json:"role"`
	Content []anthropicContentBlock `json:"content"`
}

type anthropicRequest struct {
	Model       string             `json:"model"`
	MaxTokens   int                `json:"max_tokens"`
	System      string             `json:"system,omitempty"`
	Messages    []anthropicMessage `json:"messages"`
	Temperature float32            `json:"temperature,omitempty"`
}

type anthropicResponseContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type anthropicResponse struct {
	ID         string                     `json:"id"`
	Type       string                     `json:"type"`
	Role       string                     `json:"role"`
	Content    []anthropicResponseContent `json:"content"`
	Model      string                     `json:"model"`
	StopReason string                     `json:"stop_reason"`
	Error      *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (a *AnthropicAnalyzer) callAnthropic(ctx context.Context, reqBody anthropicRequest) (string, error) {
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal anthropic request: %w", err)
	}

	url := fmt.Sprintf("%s/messages", strings.TrimSuffix(a.baseURL, "/"))

	timeout := a.httpClient.Timeout
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
	req.Header.Set("x-api-key", a.apiKey)
	req.Header.Set("anthropic-version", a.version)

	logger.Info("AI_Service", "[Ai Service] Sending Anthropic request to %s with model %s", url, reqBody.Model)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("LLM inference timed out or failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read anthropic response body: %w", err)
	}

	logger.Info("AI_Service", "[Ai Service] Received Anthropic response status %d", resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("anthropic endpoint returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var antResp anthropicResponse
	if err := json.Unmarshal(bodyBytes, &antResp); err != nil {
		return "", fmt.Errorf("failed to decode anthropic response: %w", err)
	}

	if antResp.Error != nil && antResp.Error.Message != "" {
		return "", fmt.Errorf("anthropic API error (%s): %s", antResp.Error.Type, antResp.Error.Message)
	}

	if len(antResp.Content) == 0 {
		return "", fmt.Errorf("anthropic endpoint returned no content")
	}

	var sb strings.Builder
	for _, block := range antResp.Content {
		if block.Type == "text" || block.Text != "" {
			sb.WriteString(block.Text)
		}
	}

	result := jsonclean.CleanJSON(sb.String())
	return result, nil
}

func (a *AnthropicAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)

	m := modelID
	if m == "" {
		m = a.textModel
	}

	maxTok := a.maxTokens
	if maxTok <= 0 {
		maxTok = 4096
	}

	reqBody := anthropicRequest{
		Model:       m,
		MaxTokens:   maxTok,
		Temperature: a.temperature,
		Messages: []anthropicMessage{
			{
				Role: "user",
				Content: []anthropicContentBlock{
					{
						Type: "text",
						Text: prompt,
					},
				},
			},
		},
	}

	return a.callAnthropic(ctx, reqBody)
}

func (a *AnthropicAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(workflowCtx, action, "(See attached image(s))")

	contentBlocks := []anthropicContentBlock{
		{
			Type: "text",
			Text: prompt,
		},
	}

	for _, img := range imageBytes {
		base64Img := base64.StdEncoding.EncodeToString(img)
		contentBlocks = append(contentBlocks, anthropicContentBlock{
			Type: "image",
			Source: &anthropicImageSource{
				Type:      "base64",
				MediaType: "image/jpeg",
				Data:      base64Img,
			},
		})
	}

	m := modelID
	if m == "" {
		m = a.visionModel
	}

	maxTok := a.maxTokens
	if maxTok <= 0 {
		maxTok = 4096
	}

	reqBody := anthropicRequest{
		Model:       m,
		MaxTokens:   maxTok,
		Temperature: a.temperature,
		Messages: []anthropicMessage{
			{
				Role:    "user",
				Content: contentBlocks,
			},
		},
	}

	return a.callAnthropic(ctx, reqBody)
}

func (a *AnthropicAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return nil, fmt.Errorf("GenerateEmbedding not implemented for Anthropic")
}
