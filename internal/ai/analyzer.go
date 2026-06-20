package ai

import "context"

// AnalyzeOptions holds the optional dynamic prompt parameters sent from the frontend
type AnalyzeOptions struct {
	DocumentType       string
	TargetFields       []string
	JsonSchema         string
	CustomInstructions string
}

// Analyzer defines our robust dual-pipeline strategy
type Analyzer interface {
	AnalyzeText(ctx context.Context, text string, opts AnalyzeOptions) (string, error)
	AnalyzeImage(ctx context.Context, imageBytes []byte, opts AnalyzeOptions) (string, error)
}
