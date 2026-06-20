package ai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const BasePrompt = "You are an expert document metadata extractor. Your primary goal is to carefully analyze the provided document and output a JSON object containing a brief summary and any key entities found. Do not output anything except JSON."

// OllamaAnalyzer implements the Analyzer interface.
type OllamaAnalyzer struct {
	BaseURL     string
	TextModel   string
	VisionModel string
}

func NewOllamaAnalyzer(baseURL, textModel, visionModel string) *OllamaAnalyzer {
	return &OllamaAnalyzer{
		BaseURL:     baseURL,
		TextModel:   textModel,
		VisionModel: visionModel,
	}
}

type ollamaRequest struct {
	Model  string      `json:"model"`
	Prompt string      `json:"prompt"`
	Stream bool        `json:"stream"`
	Images []string    `json:"images,omitempty"`
	Format interface{} `json:"format,omitempty"` // Forces JSON output, can accept a raw JSON schema object
}

type ollamaResponse struct {
	Response string `json:"response"`
}

// buildPrompt constructs the final prompt by combining the BasePrompt with any optional parameters
func buildPrompt(opts AnalyzeOptions, documentContent string) string {
	prompt := BasePrompt + "\n"

	if opts.DocumentType != "" {
		prompt += fmt.Sprintf("\nThe document is of type: %s\n", opts.DocumentType)
	}

	if len(opts.TargetFields) > 0 {
		prompt += fmt.Sprintf("\nYou MUST extract the following specific fields: %v\n", opts.TargetFields)
	}

	if opts.CustomInstructions != "" {
		prompt += fmt.Sprintf("\nAdditional Instructions:\n%s\n", opts.CustomInstructions)
	}

	if opts.JsonSchema != "" {
		prompt += fmt.Sprintf("\nYou MUST format your output strictly matching this JSON Schema:\n%s\n", opts.JsonSchema)
	}

	prompt += "\nDocument Content:\n" + documentContent
	return prompt
}

func (o *OllamaAnalyzer) callOllama(ctx context.Context, reqBody ollamaRequest) (string, error) {
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal ollama request: %w", err)
	}

	url := fmt.Sprintf("%s/api/generate", o.BaseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call ollama: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ollama returned status %d: %s", resp.StatusCode, string(body))
	}

	var ollamaResp ollamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&ollamaResp); err != nil {
		return "", fmt.Errorf("failed to decode ollama response: %w", err)
	}

	return ollamaResp.Response, nil
}

func (o *OllamaAnalyzer) AnalyzeText(ctx context.Context, text string, opts AnalyzeOptions) (string, error) {
	prompt := buildPrompt(opts, text)

	reqBody := ollamaRequest{
		Model:  o.TextModel,
		Prompt: prompt,
		Stream: false,
	}

	// If a schema is provided, try to pass it directly to Ollama's Structured Outputs engine
	if opts.JsonSchema != "" {
		var schemaMap map[string]interface{}
		if err := json.Unmarshal([]byte(opts.JsonSchema), &schemaMap); err == nil {
			reqBody.Format = schemaMap
		} else {
			reqBody.Format = "json" // Fallback
		}
	} else {
		reqBody.Format = "json"
	}

	return o.callOllama(ctx, reqBody)
}

func (o *OllamaAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, opts AnalyzeOptions) (string, error) {
	var base64Images []string
	for _, img := range imageBytes {
		base64Images = append(base64Images, base64.StdEncoding.EncodeToString(img))
	}

	prompt := buildPrompt(opts, "(See attached image(s))")

	reqBody := ollamaRequest{
		Model:  o.VisionModel,
		Prompt: prompt,
		Stream: false,
		Images: base64Images,
	}

	if opts.JsonSchema != "" {
		var schemaMap map[string]interface{}
		if err := json.Unmarshal([]byte(opts.JsonSchema), &schemaMap); err == nil {
			reqBody.Format = schemaMap
		} else {
			reqBody.Format = "json"
		}
	} else {
		reqBody.Format = "json"
	}

	return o.callOllama(ctx, reqBody)
}
