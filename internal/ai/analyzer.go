package ai

import (
	"context"
	pb "github.com/AndrewK4758/shared_protos"
)

// Analyzer defines our robust dual-pipeline strategy
type Analyzer interface {
	AnalyzeText(ctx context.Context, text string, workflowCtx *pb.WorkflowContext, action *pb.ActionDefinition) (string, error)
	AnalyzeImage(ctx context.Context, imageBytes [][]byte, workflowCtx *pb.WorkflowContext, action *pb.ActionDefinition) (string, error)
}
