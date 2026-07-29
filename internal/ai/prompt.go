package ai

import (
	"fmt"

	"github.com/AndrewK4758/shared_utils/envutil"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/workflowparser"
)

func buildPrompt(parser *workflowparser.NodeRequestParser, action *pb.ActionDefinition, documentContent string) string {
	basePrompt := envutil.MustGetString("OPENAI_BASE_PROMPT")

	prompt := ""
	if basePrompt != "" {
		prompt = basePrompt + "\n"
	}

	if parser != nil {
		sysInstr := parser.GetSystemInstructions()
		if sysInstr != "" {
			prompt += fmt.Sprintf("\nGlobal Job Instructions:\n%s\n", sysInstr)
		}

		schema := parser.GetExpectedSchema()
		if schema != "" {
			prompt += fmt.Sprintf("\nYou MUST format your output strictly matching this JSON Schema:\n%s\n", schema)
		}

		inputData, err := parser.GetInferenceInputData()
		if err != nil {
			logger.Info("AI_Service", "Could not extract inference input data: %v", err)
		} else if inputData != "" {
			inputKey := parser.GetInferenceInputKey()
			prompt += fmt.Sprintf("\nInput Data (%s):\n%s\n", inputKey, inputData)
		}
	}

	if action != nil {
		if action.Name != "" {
			prompt += fmt.Sprintf("\nYour exact Task to execute right now:\n%s\n", action.Name)
		}
		if action.Prompt != "" {
			prompt += fmt.Sprintf("\nTask Instructions:\n%s\n", action.Prompt)
		}
	}

	if documentContent != "" {
		prompt += "\nDocument Content / Input:\n" + documentContent
	}
	return prompt
}
