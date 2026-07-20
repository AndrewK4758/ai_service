package ai

import (
	"context"
	"fmt"
	"log"

	"ai_service/internal/util"
	pb "github.com/AndrewK4758/shared_protos"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

type BedrockAnalyzer struct {
	textModel   string
	visionModel string
	client      *bedrockruntime.Client
}

func NewBedrockAnalyzer(textModel, visionModel string) (*BedrockAnalyzer, error) {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load bedrock config: %w", err)
	}

	client := bedrockruntime.NewFromConfig(cfg)
	return &BedrockAnalyzer{
		textModel:   textModel,
		visionModel: visionModel,
		client:      client,
	}, nil
}

func (b *BedrockAnalyzer) AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
	prompt := buildPrompt(workflowCtx, action, text)

	// Bedrock Converse API
	req := &bedrockruntime.ConverseInput{
		ModelId: &b.textModel,
		Messages: []types.Message{
			{
				Role: types.ConversationRoleUser,
				Content: []types.ContentBlock{
					&types.ContentBlockMemberText{Value: prompt},
				},
			},
		},
		InferenceConfig: &types.InferenceConfiguration{
			Temperature: new(float32), // 0.0
		},
	}

	log.Printf("[Ai Service] Sending Bedrock request to model %s", b.textModel)
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

	return txt.Value, nil
}

func (b *BedrockAnalyzer) AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition) (string, error) {
	prompt := "Please extract the text from this image as JSON."
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

	req := &bedrockruntime.ConverseInput{
		ModelId: &b.visionModel,
		Messages: []types.Message{
			{
				Role:    types.ConversationRoleUser,
				Content: contentBlocks,
			},
		},
		InferenceConfig: &types.InferenceConfiguration{
			Temperature: new(float32),
		},
	}

	log.Printf("[Ai Service] Sending Bedrock Vision request to model %s", b.visionModel)
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

	return util.CleanJSON(txt.Value), nil
}
