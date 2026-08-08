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
	"github.com/AndrewK4758/shared_utils/envutil"
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
		apiKey = envutil.MustGetString("ANTHROPIC_API_KEY")
		if apiKey == "" {
			panic(fmt.Errorf("missing required config: AnthropicAPIKey"))
		}
	}

	baseURL := config.AnthropicBaseURL
	if baseURL == "" {
		baseURL = envutil.MustGetString("ANTHROPIC_BASE_URL")
		if baseURL == "" {
			panic(fmt.Errorf("missing required config: AnthropicBaseURL"))
		}
	}

	version := config.AnthropicVersion
	if version == "" {
		version = envutil.MustGetString("ANTHROPIC_VERSION")
		if version == "" {
			panic(fmt.Errorf("missing required config: AnthropicVersion"))
		}
	}

	textModel := config.AnthropicTextModel
	if textModel == "" {
		textModel = envutil.MustGetString("ANTHROPIC_TEXT_MODEL")
		if textModel == "" {
			panic(fmt.Errorf("missing required config: AnthropicTextModel"))
		}
	}

	visionModel := config.AnthropicVisionModel
	if visionModel == "" {
		visionModel = envutil.MustGetString("ANTHROPIC_VISION_MODEL")
		if visionModel == "" {
			panic(fmt.Errorf("missing required config: AnthropicVisionModel"))
		}
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
	logger.Info("AI_Service", "[Ai Service] Request Body: %s", string(jsonData))

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
	logger.Info("AI_Service", "[Ai Service] Raw Response Body: %s", string(bodyBytes))

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

func (a *AnthropicAnalyzer) AnalyzeText(ctx context.Context, text string, req *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(req, action, text)

	m := modelID
	if m == "" {
		panic(fmt.Errorf("missing modelID in Anthropic AnalyzeText"))
	}

	maxTok := a.maxTokens
	if maxTok <= 0 {
		panic(fmt.Errorf("missing maxTokens in Anthropic configuration"))
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

	resp, err := a.callAnthropic(ctx, reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to call Anthropic for text analysis: %w", err)
	}
	return resp, nil
}

func (a *AnthropicAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, req *pb.ExecuteTaskRequest, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(req, action, "(See attached image(s))")

	contentBlocks := []anthropicContentBlock{
		{
			Type: "text",
			Text: prompt,
		},
	}

	for _, img := range imageBytes {
		base64Img := base64.StdEncoding.EncodeToString(img)
		mediaType := http.DetectContentType(img)
		contentBlocks = append(contentBlocks, anthropicContentBlock{
			Type: "image",
			Source: &anthropicImageSource{
				Type:      "base64",
				MediaType: mediaType,
				Data:      base64Img,
			},
		})
	}

	m := modelID
	if m == "" {
		panic(fmt.Errorf("missing modelID in Anthropic AnalyzeImage"))
	}

	maxTok := a.maxTokens
	if maxTok <= 0 {
		panic(fmt.Errorf("missing maxTokens in Anthropic configuration"))
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

	resp, err := a.callAnthropic(ctx, reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to call Anthropic for image analysis: %w", err)
	}
	return resp, nil
}

func (a *AnthropicAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return nil, fmt.Errorf("GenerateEmbedding not implemented for Anthropic")
}
