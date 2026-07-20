package ai

import (
	"fmt"
	"os"
)

func NewAnalyzerFactory(config OpenAIConfig) (Analyzer, error) {
	provider := os.Getenv("AI_PROVIDER")
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
	default:
		return nil, fmt.Errorf("unsupported AI_PROVIDER: %s", provider)
	}
}
