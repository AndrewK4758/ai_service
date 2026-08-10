package ai

import (
	"fmt"

	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/workflowparser"

	pb "github.com/AndrewK4758/shared_protos"
)

func buildPrompt(req *pb.ExecuteTaskRequest, action *pb.ActionDefinition, documentContent string) string {
	basePrompt := envutil.MustGetString("OPENAI_BASE_PROMPT")

	prompt := ""
	if basePrompt != "" {
		prompt = basePrompt + "\n"
	}

	if req != nil && req.Payload != nil {
		parser := workflowparser.NewTaskRequestParser(req)
		payload := parser.MustGetExecuteTaskPayload()

		if payload.NodeConfig != nil {
			if payload.NodeConfig.SystemInstructions != "" {
				prompt += fmt.Sprintf("\nGlobal Job Instructions:\n%s\n", payload.NodeConfig.SystemInstructions)
			}
			
			if payload.NodeConfig.ExpectedSchemaStruct != nil {
				b, _ := payload.NodeConfig.ExpectedSchemaStruct.MarshalJSON()
				if len(b) > 0 && string(b) != "{}" {
					prompt += fmt.Sprintf("\nYou MUST format your output strictly matching this JSON Schema:\n%s\n", string(b))
				}
			}

			if payload.NodeConfig.InferenceInputKey != "" {
				infInputStr := parser.MustExtractDynamicInputStrict(payload.NodeConfig.InferenceInputKey, "")
				if infInputStr != "" {
					if documentContent != "" {
						documentContent += "\n\n"
					}
					documentContent += "Additional Input:\n" + infInputStr
				}
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
		for _, rule := range action.Rules {
			if rule != "" {
				prompt += fmt.Sprintf("- RULE: %s\n", rule)
			}
		}
	}

	if documentContent != "" {
		prompt += "\nDocument Content / Input:\n" + documentContent
	}
	return prompt
}
