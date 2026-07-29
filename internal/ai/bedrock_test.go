package ai

import (
	"context"
	"errors"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/workflowparser"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

type mockBedrockClient struct {
	lastInput *bedrockruntime.ConverseInput
	output    *bedrockruntime.ConverseOutput
	err       error
}

func (m *mockBedrockClient) Converse(ctx context.Context, params *bedrockruntime.ConverseInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error) {
	m.lastInput = params
	if m.err != nil {
		return nil, m.err
	}
	return m.output, nil
}

func TestBedrockAnalyzer_AnalyzeText(t *testing.T) {
	mockClient := &mockBedrockClient{
		output: &bedrockruntime.ConverseOutput{
			Output: &types.ConverseOutputMemberMessage{
				Value: types.Message{
					Role: types.ConversationRoleAssistant,
					Content: []types.ContentBlock{
						&types.ContentBlockMemberText{
							Value: "```json\n{\"summary\": \"bedrock text output\"}\n```",
						},
					},
				},
			},
		},
	}

	config := OpenAIConfig{
		TextModel:   "anthropic.claude-3-sonnet-20240229-v1:0",
		VisionModel: "anthropic.claude-3-sonnet-20240229-v1:0",
		Temperature: 0.2,
		MaxTokens:   1024,
	}

	analyzer := NewBedrockAnalyzerWithClient(config, mockClient)
	ctx := context.Background()
	req := &pb.ExecuteWorkflowNodeRequest{}
	parser := workflowparser.NewNodeRequestParser(req)
	action := &pb.ActionDefinition{Name: "bedrock_action"}

	// Test 1: Default text model
	res, err := analyzer.AnalyzeText(ctx, "sample input text", parser, action, "")
	if err != nil {
		t.Fatalf("AnalyzeText failed: %v", err)
	}

	if mockClient.lastInput == nil || mockClient.lastInput.ModelId == nil {
		t.Fatalf("expected ConverseInput ModelId to be set")
	}
	if *mockClient.lastInput.ModelId != "anthropic.claude-3-sonnet-20240229-v1:0" {
		t.Errorf("expected model 'anthropic.claude-3-sonnet-20240229-v1:0', got '%s'", *mockClient.lastInput.ModelId)
	}
	if res != "{\"summary\": \"bedrock text output\"}" {
		t.Errorf("unexpected res: %s", res)
	}

	// Test 2: Dynamic model override
	overrideModel := "us.meta.llama3-3-70b-instruct-v1:0"
	_, err = analyzer.AnalyzeText(ctx, "sample text", parser, action, overrideModel)
	if err != nil {
		t.Fatalf("AnalyzeText with model override failed: %v", err)
	}
	if *mockClient.lastInput.ModelId != overrideModel {
		t.Errorf("expected model override '%s', got '%s'", overrideModel, *mockClient.lastInput.ModelId)
	}
}

func TestBedrockAnalyzer_AnalyzeImage(t *testing.T) {
	mockClient := &mockBedrockClient{
		output: &bedrockruntime.ConverseOutput{
			Output: &types.ConverseOutputMemberMessage{
				Value: types.Message{
					Role: types.ConversationRoleAssistant,
					Content: []types.ContentBlock{
						&types.ContentBlockMemberText{
							Value: "{\"extracted\": \"invoice #999\"}",
						},
					},
				},
			},
		},
	}

	config := OpenAIConfig{
		VisionModel: "anthropic.claude-3-5-sonnet-20241022-v2:0",
		Temperature: 0.1,
		MaxTokens:   2048,
	}

	analyzer := NewBedrockAnalyzerWithClient(config, mockClient)
	ctx := context.Background()
	req := &pb.ExecuteWorkflowNodeRequest{}
	parser := workflowparser.NewNodeRequestParser(req)
	action := &pb.ActionDefinition{Name: "bedrock_vision"}
	images := [][]byte{[]byte("fake-image-bytes")}

	// Test vision analysis
	res, err := analyzer.AnalyzeImage(ctx, images, parser, action, "")
	if err != nil {
		t.Fatalf("AnalyzeImage failed: %v", err)
	}

	if *mockClient.lastInput.ModelId != "anthropic.claude-3-5-sonnet-20241022-v2:0" {
		t.Errorf("expected default vision model, got '%s'", *mockClient.lastInput.ModelId)
	}
	if res != "{\"extracted\": \"invoice #999\"}" {
		t.Errorf("unexpected vision result: %s", res)
	}

	// Test vision with dynamic model override
	overrideVisionModel := "amazon.titan-embed-text-v2:0"
	_, err = analyzer.AnalyzeImage(ctx, images, parser, action, overrideVisionModel)
	if err != nil {
		t.Fatalf("AnalyzeImage with override failed: %v", err)
	}
	if *mockClient.lastInput.ModelId != overrideVisionModel {
		t.Errorf("expected overridden vision model '%s', got '%s'", overrideVisionModel, *mockClient.lastInput.ModelId)
	}
}

func TestBedrockAnalyzer_ErrorHandling(t *testing.T) {
	// Case 1: Client Converse returns error
	mockClient := &mockBedrockClient{
		err: errors.New("bedrock service unavailable"),
	}
	config := OpenAIConfig{TextModel: "test-model"}
	analyzer := NewBedrockAnalyzerWithClient(config, mockClient)

	_, err := analyzer.AnalyzeText(context.Background(), "test", nil, nil, "")
	if err == nil {
		t.Fatalf("expected error when bedrock client fails, got nil")
	}

	// Case 2: Response output is not ConverseOutputMemberMessage
	mockClient.err = nil
	mockClient.output = &bedrockruntime.ConverseOutput{
		Output: nil,
	}
	_, err = analyzer.AnalyzeText(context.Background(), "test", nil, nil, "")
	if err == nil {
		t.Fatalf("expected error when output is nil/unexpected, got nil")
	}

	// Case 3: Empty content in response
	mockClient.output = &bedrockruntime.ConverseOutput{
		Output: &types.ConverseOutputMemberMessage{
			Value: types.Message{
				Content: []types.ContentBlock{},
			},
		},
	}
	_, err = analyzer.AnalyzeText(context.Background(), "test", nil, nil, "")
	if err == nil {
		t.Fatalf("expected error when content is empty, got nil")
	}
}
