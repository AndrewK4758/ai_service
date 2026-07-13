package ai

import (
	"fmt"
	"os"

	pb "github.com/AndrewK4758/shared_protos"
)

var defaultBasePrompt = "You are a strict AI assistant executing tasks for a business application. Your only goal is to handle the provided tasks exactly as described with 0 creativity. Do not hallucinate, explain, or add any unrequested information."

func buildPrompt(workflowCtx *pb.WorkflowContext, action *pb.ActionDefinition, documentContent string) string {
	basePrompt := os.Getenv("OPENAI_BASE_PROMPT")
	if basePrompt == "" {
		basePrompt = defaultBasePrompt
	}
	prompt := basePrompt + "\n"

	if workflowCtx != nil {
		if workflowCtx.JobDefinition != nil && workflowCtx.JobDefinition.GlobalInstructions != "" {
			prompt += fmt.Sprintf("\nGlobal Job Instructions:\n%s\n", workflowCtx.JobDefinition.GlobalInstructions)
		}

		if workflowCtx.GlobalState != nil {
			stateBytes, err := workflowCtx.GlobalState.MarshalJSON()
			if err == nil {
				prompt += fmt.Sprintf("\nCurrent Global State:\n%s\n", string(stateBytes))
			}
		}

		if workflowCtx.ActionState != nil {
			actionBytes, err := workflowCtx.ActionState.MarshalJSON()
			if err == nil {
				prompt += fmt.Sprintf("\nCurrent Action State (Modify this):\n%s\n", string(actionBytes))
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
		if action.ExpectedOutputSchema != "" {
			prompt += fmt.Sprintf("\nYou MUST format your output strictly matching this JSON Schema:\n%s\n", action.ExpectedOutputSchema)
		}
	}

	prompt += "\nDocument Content / Input:\n" + documentContent
	return prompt
}
