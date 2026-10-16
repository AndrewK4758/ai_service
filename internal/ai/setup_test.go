package ai

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("ANTHROPIC_TEXT_MODEL", "test-text")
		os.Setenv("ANTHROPIC_VISION_MODEL", "test-vision")
	os.Setenv("ANTHROPIC_API_KEY", "test")
		os.Setenv("ANTHROPIC_VERSION", "2023-06-01")
		os.Setenv("ANTHROPIC_BASE_URL", "http://test")
	os.Setenv("OLLAMA_TIMEOUT_MINUTES", "5")
	os.Setenv("AI_PROVIDER", "OPENAI")

	os.Exit(m.Run())
}
