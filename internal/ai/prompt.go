package ai

import (
	"fmt"
	"os"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
)

func buildPrompt(req *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition, documentContent string) string {
	basePrompt := os.Getenv("OPENAI_BASE_PROMPT")
	if basePrompt == "" {
		basePrompt = os.Getenv("DEFAULT_BASE_PROMPT")
	}
	if basePrompt == "" {
		basePrompt = "You are a strict AI assistant executing tasks for a business application. Your only goal is to handle the provided tasks exactly as described with 0 creativity. Do not hallucinate, explain, or add any unrequested information."
	}
	prompt := basePrompt + "\n"

	if req != nil {
		if req.NodeConfig != nil && req.NodeConfig.SystemInstructions != "" {
			prompt += fmt.Sprintf("\nGlobal Job Instructions:\n%s\n", req.NodeConfig.SystemInstructions)
		}

		if req.NodeConfig != nil && req.NodeConfig.ExpectedSchema != "" {
			prompt += fmt.Sprintf("\nYou MUST format your output strictly matching this JSON Schema:\n%s\n", req.NodeConfig.ExpectedSchema)
		}

		if req.GlobalState != nil {
			stateJson, err := req.GlobalState.MarshalJSON()
			if err != nil {
				logger.Info("AI_Service", "failed to marshal global state: %v", err)
			} else {
				prompt += fmt.Sprintf("\nCurrent Global State:\n%s\n", string(stateJson))
			}
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

	prompt += "\nDocument Content / Input:\n" + documentContent
	return prompt
}
