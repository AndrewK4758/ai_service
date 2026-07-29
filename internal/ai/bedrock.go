package ai

import (
	"context"
	"fmt"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/workflowparser"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

type BedrockRuntimeAPI interface {
	Converse(ctx context.Context, params *bedrockruntime.ConverseInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error)
}

type BedrockAnalyzer struct {
	textModel   string
	visionModel string
	temperature float32
	maxTokens   int32
	client      BedrockRuntimeAPI
}

func NewBedrockAnalyzer(aiConfig OpenAIConfig) (*BedrockAnalyzer, error) {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load bedrock config: %w", err)
	}

	client := bedrockruntime.NewFromConfig(cfg)
	return &BedrockAnalyzer{
		textModel:   aiConfig.TextModel,
		visionModel: aiConfig.VisionModel,
		temperature: aiConfig.Temperature,
		maxTokens:   int32(aiConfig.MaxTokens),
		client:      client,
	}, nil
}

func NewBedrockAnalyzerWithClient(aiConfig OpenAIConfig, client BedrockRuntimeAPI) *BedrockAnalyzer {
	return &BedrockAnalyzer{
		textModel:   aiConfig.TextModel,
		visionModel: aiConfig.VisionModel,
		temperature: aiConfig.Temperature,
		maxTokens:   int32(aiConfig.MaxTokens),
		client:      client,
	}
}

func (b *BedrockAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)

	inferenceConfig := &types.InferenceConfiguration{
		Temperature: &b.temperature,
	}
	if b.maxTokens > 0 {
		inferenceConfig.MaxTokens = &b.maxTokens
	}

	m := modelID
	if m == "" {
		m = b.textModel
	}

	// Bedrock Converse API
	req := &bedrockruntime.ConverseInput{
		ModelId: &m,
		Messages: []types.Message{
			{
				Role: types.ConversationRoleUser,
				Content: []types.ContentBlock{
					&types.ContentBlockMemberText{Value: prompt},
				},
			},
		},
		InferenceConfig: inferenceConfig,
	}

	logger.Info("AI_Service", "[Ai Service] Sending Bedrock request to model %s", m)
	resp, err := b.client.Converse(ctx, req)
	if err != nil {
		return "", fmt.Errorf("bedrock inference failed: %w", err)
	}

	msg, ok := resp.Output.(*types.ConverseOutputMemberMessage)
	if !ok {
		return "", fmt.Errorf("bedrock returned unexpected output type")
	}
	if len(msg.Value.Content) == 0 {
		return "", fmt.Errorf("bedrock returned empty content")
	}
	txt, ok := msg.Value.Content[0].(*types.ContentBlockMemberText)
	if !ok {
		return "", fmt.Errorf("bedrock returned non-text content")
	}

	return jsonclean.CleanJSON(txt.Value), nil
}

func (b *BedrockAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error) {
	prompt := envutil.MustGetString("DEFAULT_VISION_PROMPT")
	if action != nil && workflowCtx != nil {
		prompt = buildPrompt(workflowCtx, action, "")
	}

	contentBlocks := []types.ContentBlock{
		&types.ContentBlockMemberText{Value: prompt},
	}
	for _, bytes := range imageBytes {
		contentBlocks = append(contentBlocks, &types.ContentBlockMemberImage{
			Value: types.ImageBlock{
				Format: types.ImageFormatJpeg, // Assume jpeg
				Source: &types.ImageSourceMemberBytes{Value: bytes},
			},
		})
	}

	inferenceConfig := &types.InferenceConfiguration{
		Temperature: &b.temperature,
	}
	if b.maxTokens > 0 {
		inferenceConfig.MaxTokens = &b.maxTokens
	}

	m := modelID
	if m == "" {
		m = b.visionModel
	}

	req := &bedrockruntime.ConverseInput{
		ModelId: &m,
		Messages: []types.Message{
			{
				Role:    types.ConversationRoleUser,
				Content: contentBlocks,
			},
		},
		InferenceConfig: inferenceConfig,
	}

	logger.Info("AI_Service", "[Ai Service] Sending Bedrock Vision request to model %s", m)
	resp, err := b.client.Converse(ctx, req)
	if err != nil {
		return "", fmt.Errorf("bedrock vision inference failed: %w", err)
	}

	msg, ok := resp.Output.(*types.ConverseOutputMemberMessage)
	if !ok {
		return "", fmt.Errorf("bedrock returned unexpected output type")
	}
	if len(msg.Value.Content) == 0 {
		return "", fmt.Errorf("bedrock returned empty content")
	}
	txt, ok := msg.Value.Content[0].(*types.ContentBlockMemberText)
	if !ok {
		return "", fmt.Errorf("bedrock returned non-text content")
	}

	return jsonclean.CleanJSON(txt.Value), nil
}

func (b *BedrockAnalyzer) GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error) {
	return nil, fmt.Errorf("GenerateEmbedding not implemented for Bedrock")
}
