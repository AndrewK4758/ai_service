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
	"os"
	"strconv"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
)

var defaultBasePrompt = "You are a strict AI assistant executing tasks for a business application. Your only goal is to handle the provided tasks exactly as described with 0 creativity. Do not hallucinate, explain, or add any unrequested information."

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
	Model   string                 `json:"model"`
	Prompt  string                 `json:"prompt"`
	Stream  bool                   `json:"stream"`
	Images  []string               `json:"images,omitempty"`
	Format  interface{}            `json:"format,omitempty"` // Forces JSON output, can accept a raw JSON schema object
	Options map[string]interface{} `json:"options,omitempty"`
}

type ollamaResponse struct {
	Response string `json:"response"`
}

func buildPrompt(workflowCtx *pb.WorkflowContext, action *pb.ActionDefinition, documentContent string) string {
	basePrompt := os.Getenv("OLLAMA_BASE_PROMPT")
	if basePrompt == "" {
		basePrompt = defaultBasePrompt
	}
	prompt := basePrompt + "\n"

	isOrchPrimitive := false
	if action != nil {
		p := action.OrchestratorPrimitive
		if p == "PLANNING_PRIMITIVE" || p == "REVIEW_PRIMITIVE" || p == "VALIDATION_PRIMITIVE" || p == "ASSEMBLY_PRIMITIVE" {
			isOrchPrimitive = true
		}
	}

	if workflowCtx != nil && !isOrchPrimitive {
		if workflowCtx.JobDefinition != nil && workflowCtx.JobDefinition.GlobalInstructions != "" {
			prompt += fmt.Sprintf("\nGlobal Job Instructions:\n%s\n", workflowCtx.JobDefinition.GlobalInstructions)
		}

		if workflowCtx.CurrentStep != nil {
			if workflowCtx.CurrentStep.CustomInstructions != "" {
				prompt += fmt.Sprintf("\nStep Specific Instructions:\n%s\n", workflowCtx.CurrentStep.CustomInstructions)
			}
			if workflowCtx.CurrentStep.ExpectedSchema != "" {
				prompt += fmt.Sprintf("\nYou MUST format your output strictly matching this JSON Schema:\n%s\n", workflowCtx.CurrentStep.ExpectedSchema)
			}
		}

		if workflowCtx.GlobalState != nil {
			stateBytes, err := workflowCtx.GlobalState.MarshalJSON()
			if err == nil {
				prompt += fmt.Sprintf("\nCurrent Global State:\n%s\n", string(stateBytes))
			}
		}
	}

	if action != nil && !isOrchPrimitive {
		prompt += fmt.Sprintf("\nYour exact Task to execute right now:\n%s\n", action.Name)
	}

	prompt += "\nDocument Content / Input:\n" + documentContent
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

	log.Printf("[Ollama] Sending request to %s with model %s", url, reqBody.Model)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call ollama: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read ollama response body: %w", err)
	}

	log.Printf("[Ollama] Received response status %d. Body: %s", resp.StatusCode, string(bodyBytes))

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var ollamaResp ollamaResponse
	if err := json.Unmarshal(bodyBytes, &ollamaResp); err != nil {
		return "", fmt.Errorf("failed to decode ollama response: %w", err)
	}

	response := strings.TrimSpace(ollamaResp.Response)
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)

	return response, nil
}

func (o *OllamaAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.WorkflowContext, action *pb.ActionDefinition) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)

	numCtx := 32768
	if envCtx := os.Getenv("OLLAMA_NUM_CTX"); envCtx != "" {
		if c, err := strconv.Atoi(envCtx); err == nil && c > 0 {
			numCtx = c
		}
	}

	reqBody := ollamaRequest{
		Model:  o.TextModel,
		Prompt: prompt,
		Stream: false,
		Options: map[string]interface{}{
			"num_ctx": numCtx,
		},
	}

	// We omit passing opts.JsonSchema to reqBody.Format because smaller models like
	// gemma4:e2b struggle with strict schema enforcement and will return a blank {} object.
	// The BasePrompt already instructs the model to return JSON.

	return o.callOllama(ctx, reqBody)
}

func (o *OllamaAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.WorkflowContext, action *pb.ActionDefinition) (string, error) {
	var base64Images []string
	for _, img := range imageBytes {
		base64Images = append(base64Images, base64.StdEncoding.EncodeToString(img))
	}

	prompt := buildPrompt(workflowCtx, action, "(See attached image(s))")

	numCtx := 32768
	if envCtx := os.Getenv("OLLAMA_NUM_CTX"); envCtx != "" {
		if c, err := strconv.Atoi(envCtx); err == nil && c > 0 {
			numCtx = c
		}
	}

	reqBody := ollamaRequest{
		Model:  o.VisionModel,
		Prompt: prompt,
		Stream: false,
		Images: base64Images,
		Options: map[string]interface{}{
			"num_ctx": numCtx,
		},
	}

	// We omit passing opts.JsonSchema to reqBody.Format because some models
	// struggle with strict schema enforcement and will return a blank {} object.
	// The BasePrompt already instructs the model to return JSON.

	return o.callOllama(ctx, reqBody)
}
