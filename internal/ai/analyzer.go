package ai

import (
	"context"

	"github.com/AndrewK4758/shared_utils/workflowparser"
	pb "github.com/AndrewK4758/shared_protos"
)

// Analyzer defines our robust dual-pipeline strategy
type Analyzer interface {
	AnalyzeText(ctx context.Context, text string, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error)
	AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *workflowparser.NodeRequestParser, action *pb.ActionDefinition, modelID string) (string, error)
	GenerateEmbedding(ctx context.Context, text string, modelID string) ([]float32, error)
}
