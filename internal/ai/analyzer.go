package ai

import (
	"context"

	pb "github.com/AndrewK4758/shared_protos"
)

// Analyzer defines our robust dual-pipeline strategy
type Analyzer interface {
	AnalyzeText(ctx context.Context, text string, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition, modelID string) (string, error)
	AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.ExecuteWorkflowNodeRequest, action *pb.ActionDefinition, modelID string) (string, error)
}
