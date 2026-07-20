package ai

import (
	"context"
	"fmt"
	"log"
	"os"

	"ai_service/internal/util"
	"cloud.google.com/go/vertexai/genai"
	pb "github.com/AndrewK4758/shared_protos"
)

type VertexAnalyzer struct {
	textModel   string
	visionModel string
	temperature float32
	maxTokens   int32
	client      *genai.Client
}

func NewVertexAnalyzer(config OpenAIConfig) (*VertexAnalyzer, error) {
	projectID := os.Getenv("VERTEX_PROJECT_ID")
	location := os.Getenv("VERTEX_LOCATION")
	
	if projectID == "" || location == "" {
		return nil, fmt.Errorf("VERTEX_PROJECT_ID and VERTEX_LOCATION must be set")
	}

	ctx := context.Background()
	client, err := genai.NewClient(ctx, projectID, location)
	if err != nil {
		return nil, fmt.Errorf("failed to create vertex client: %w", err)
	}

	return &VertexAnalyzer{
		textModel:   config.TextModel,
		visionModel: config.VisionModel,
		temperature: config.Temperature,
		maxTokens:   int32(config.MaxTokens),
		client:      client,
	}, nil
}

func (v *VertexAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)
	
	model := v.client.GenerativeModel(v.textModel)
	model.SetTemperature(v.temperature)
	if v.maxTokens > 0 {
		model.SetMaxOutputTokens(int32(v.maxTokens))
	}
	model.ResponseMIMEType = "application/json"

	log.Printf("[Ai Service] Sending Vertex AI request to model %s", v.textModel)

	resp, err := model.GenerateContent(ctx, genai.Text(prompt))
	if err != nil {
		return "", fmt.Errorf("vertex inference failed: %w", err)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("vertex returned empty response")
	}

	part := resp.Candidates[0].Content.Parts[0]
	if txt, ok := part.(genai.Text); ok {
		return string(txt), nil
	}

	return "", fmt.Errorf("unexpected part type from vertex")
}

func (v *VertexAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
	prompt := "Please extract the text from this image as JSON."
	if action != nil && workflowCtx != nil {
		prompt = buildPrompt(workflowCtx, action, "")
	}

	model := v.client.GenerativeModel(v.visionModel)
	model.SetTemperature(v.temperature)
	if v.maxTokens > 0 {
		model.SetMaxOutputTokens(int32(v.maxTokens))
	}
	model.ResponseMIMEType = "application/json"

	var parts []genai.Part
	parts = append(parts, genai.Text(prompt))
	for _, b := range imageBytes {
		parts = append(parts, genai.ImageData("image/jpeg", b)) // Assume JPEG for now
	}

	log.Printf("[Ai Service] Sending Vertex AI Vision request to model %s", v.visionModel)

	resp, err := model.GenerateContent(ctx, parts...)
	if err != nil {
		return "", fmt.Errorf("vertex vision inference failed: %w", err)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("vertex returned empty response")
	}

	part := resp.Candidates[0].Content.Parts[0]
	if txt, ok := part.(genai.Text); ok {
		return util.CleanJSON(string(txt)), nil
	}

	return "", fmt.Errorf("unexpected part type from vertex")
}
