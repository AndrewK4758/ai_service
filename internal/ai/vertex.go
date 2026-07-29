package ai

import (
	"context"
	"fmt"

	"github.com/AndrewK4758/shared_utils/envutil"

	"cloud.google.com/go/vertexai/genai"
	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/workflowparser"
)

type VertexAnalyzer struct {
	textModel   string
	visionModel string
	temperature float32
	maxTokens   int32
	client      *genai.Client
}

func NewVertexAnalyzer(config OpenAIConfig) (*VertexAnalyzer, error) {
	projectID := envutil.MustGetString("VERTEX_PROJECT_ID")
	location := envutil.MustGetString("VERTEX_LOCATION")

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

func (v *VertexAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)

	m := modelID
	if m == "" {
		m = v.textModel
	}

	model := v.client.GenerativeModel(m)
	model.SetTemperature(v.temperature)
	if v.maxTokens > 0 {
		model.SetMaxOutputTokens(int32(v.maxTokens))
	}
	model.ResponseMIMEType = "application/json"

	logger.Info("AI_Service", "[Ai Service] Sending Vertex AI request to model %s", m)

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

func (v *VertexAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := envutil.MustGetString("DEFAULT_VISION_PROMPT")
	if action != nil && workflowCtx != nil {
		prompt = buildPrompt(workflowCtx, action, "")
	}

	m := modelID
	if m == "" {
		m = v.visionModel
	}

	model := v.client.GenerativeModel(m)
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

	logger.Info("AI_Service", "[Ai Service] Sending Vertex AI Vision request to model %s", m)

	resp, err := model.GenerateContent(ctx, parts...)
	if err != nil {
		return "", fmt.Errorf("vertex vision inference failed: %w", err)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("vertex returned empty response")
	}

	part := resp.Candidates[0].Content.Parts[0]
	if txt, ok := part.(genai.Text); ok {
		return jsonclean.CleanJSON(string(txt)), nil
	}

	return "", fmt.Errorf("unexpected part type from vertex")
}

func (v *VertexAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return nil, fmt.Errorf("GenerateEmbedding not implemented for Vertex")
}
