package ai

import (
	"fmt"

	"github.com/AndrewK4758/shared_utils/envutil"
)

func NewAnalyzerFactory(config OpenAIConfig) (Analyzer, error) {
	provider := envutil.GetStringOrDefault("AI_PROVIDER", "")
	if provider == "" {
		provider = "OPENAI" // Default
	}

	switch provider {
	case "OPENAI", "LOCAL":
		return NewOpenAIAnalyzerWithConfig(config), nil
	case "VERTEX":
		return NewVertexAnalyzer(config)
	case "BEDROCK":
		return NewBedrockAnalyzer(config)
	case "ANTHROPIC", "CLAUDE":
		return NewAnthropicAnalyzer(config)
	default:
		return nil, fmt.Errorf("unsupported AI_PROVIDER: %s", provider)
	}
}
