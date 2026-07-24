package ai

import (
	"fmt"
	"os"
	"testing"
)

func TestNewAnalyzerFactory(t *testing.T) {
	tests := []struct {
		name       string
		provider   string
		expectType string
		expectErr  bool
	}{
		{
			name:       "Default to OPENAI",
			provider:   "",
			expectType: "*ai.OpenAIAnalyzer",
			expectErr:  false,
		},
		{
			name:       "OPENAI",
			provider:   "OPENAI",
			expectType: "*ai.OpenAIAnalyzer",
			expectErr:  false,
		},
		{
			name:       "LOCAL",
			provider:   "LOCAL",
			expectType: "*ai.OpenAIAnalyzer",
			expectErr:  false,
		},
		{
			name:       "VERTEX",
			provider:   "VERTEX",
			expectType: "",
			expectErr:  true, // GCP SDK fails immediately without ADC credentials
		},
		{
			name:       "BEDROCK",
			provider:   "BEDROCK",
			expectType: "*ai.BedrockAnalyzer",
			expectErr:  false,
		},
		{
			name:       "ANTHROPIC",
			provider:   "ANTHROPIC",
			expectType: "*ai.AnthropicAnalyzer",
			expectErr:  false,
		},
		{
			name:       "CLAUDE",
			provider:   "CLAUDE",
			expectType: "*ai.AnthropicAnalyzer",
			expectErr:  false,
		},
		{
			name:       "Unknown Provider",
			provider:   "UNKNOWN",
			expectType: "",
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("AI_PROVIDER", tt.provider)
			os.Setenv("VERTEX_PROJECT_ID", "test-project")
			os.Setenv("VERTEX_LOCATION", "us-central1")
			defer os.Unsetenv("AI_PROVIDER")
			defer os.Unsetenv("VERTEX_PROJECT_ID")
			defer os.Unsetenv("VERTEX_LOCATION")

			config := DefaultConfig("fake-token")
			analyzer, err := NewAnalyzerFactory(config)

			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v", tt.expectErr, err)
			}

			if !tt.expectErr {
				actualType := fmt.Sprintf("%T", analyzer)
				if actualType != tt.expectType {
					t.Errorf("expected type %s, got %s", tt.expectType, actualType)
				}
			}
		})
	}
}
