package ai

import (
	"fmt"

	"github.com/AndrewK4758/shared_utils/envutil"

	pb "github.com/AndrewK4758/shared_protos"
)

func buildPrompt(req *pb.ExecuteTaskRequest, action *pb.ActionDefinition, documentContent string) string {
	basePrompt := envutil.MustGetString("OPENAI_BASE_PROMPT")

	prompt := ""
	if basePrompt != "" {
		prompt = basePrompt + "\n"
	}

	if req != nil && req.Payload != nil {
		payloadMap := req.Payload.AsMap()
		
		// Extract System Instructions from Payload if provided by client
		if sysInstrVal, ok := payloadMap["system_instructions"]; ok {
			if sysInstrStr, isStr := sysInstrVal.(string); isStr && sysInstrStr != "" {
				prompt += fmt.Sprintf("\nGlobal Job Instructions:\n%s\n", sysInstrStr)
			}
		}

		// Extract Schema from Payload if provided by client
		if schemaVal, ok := payloadMap["expected_schema"]; ok {
			if schemaStr, isStr := schemaVal.(string); isStr && schemaStr != "" {
				prompt += fmt.Sprintf("\nYou MUST format your output strictly matching this JSON Schema:\n%s\n", schemaStr)
			}
		}

		// Extract Inference Input from Payload
		if infInputVal, ok := payloadMap["inference_input"]; ok {
			if infInputStr, isStr := infInputVal.(string); isStr && infInputStr != "" {
				if documentContent != "" {
					documentContent += "\n\n"
				}
				documentContent += "Additional Input:\n" + infInputStr
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
