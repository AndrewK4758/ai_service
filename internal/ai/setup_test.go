package ai

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Set required environment variables for tests
	os.Setenv("OLLAMA_TIMEOUT_MINUTES", "5")
	os.Setenv("AI_PROVIDER", "OPENAI")

	os.Exit(m.Run())
}
