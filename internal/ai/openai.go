package ai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
)

// OpenAIAnalyzer implements the Analyzer interface for OpenAI-compatible endpoints (like vLLM)
type OpenAIAnalyzer struct {
	BaseURL     string
	TextModel   string
	VisionModel string
}

func NewOpenAIAnalyzer(baseURL, textModel, visionModel string) *OpenAIAnalyzer {
	return &OpenAIAnalyzer{
		BaseURL:     baseURL,
		TextModel:   textModel,
		VisionModel: visionModel,
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

	url := fmt.Sprintf("%s/chat/completions", strings.TrimSuffix(o.BaseURL, "/"))
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// For vLLM local, auth token is optional, but we set it just in case
	req.Header.Set("Authorization", "Bearer EMPTY")

	client := &http.Client{
		Timeout: 5 * time.Minute,
	}

	log.Printf("[Ai Service] Sending request to %s with model %s", url, reqBody.Model)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("LLM inference timed out or failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read openai response body: %w", err)
	}

	log.Printf("[Ai Service] Received response status %d", resp.StatusCode)

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

	response := strings.TrimSpace(oaiResp.Choices[0].Message.Content)
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)

	return response, nil
}

func (o *OpenAIAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)

	reqBody := openAIRequest{
		Model: o.TextModel,
		Messages: []openAIMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
		Temperature: 0.0, // Strict adherence for 0% creativity
		MaxTokens:   4096,
		ResponseFormat: &openAIResponseFormat{
			Type: "json_object",
		},
	}

	return o.callOpenAI(ctx, reqBody)
}

func (o *OpenAIAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
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

	reqBody := openAIRequest{
		Model: o.VisionModel,
		Messages: []openAIMessage{
			{
				Role:    "user",
				Content: contentArray,
			},
		},
		Temperature: 0.0,
		MaxTokens:   4096,
		ResponseFormat: &openAIResponseFormat{
			Type: "json_object",
		},
	}

	return o.callOpenAI(ctx, reqBody)
}
