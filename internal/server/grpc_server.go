package server

import (
	"context"
	"log"

	"ai_service/internal/ai"
	pb "doc_processor/shared_protos"
	"github.com/google/uuid"
)

type DocumentProcessorServer struct {
	pb.UnimplementedDocumentProcessorServer
	analyzer ai.Analyzer
	router   *AppRouter
}

func NewDocumentProcessorServer(analyzer ai.Analyzer, router *AppRouter) *DocumentProcessorServer {
	return &DocumentProcessorServer{
		analyzer: analyzer,
		router:   router,
	}
}

func (s *DocumentProcessorServer) ProcessDocument(ctx context.Context, req *pb.ProcessDocumentRequest) (*pb.ProcessDocumentResponse, error) {
	// Generate correlation ID if not provided
	corrID := req.GetCorrelationId()
	if corrID == "" {
		corrID = uuid.New().String()
	}

	appID := req.GetAppId()
	if appID == "" {
		appID = "default"
	}

	log.Printf("[CorrelationID: %s] Received document: %s for AppID: %s", corrID, req.GetFileName(), appID)

	// Create job and dispatch it asynchronously
	job := Job{
		Ctx:           context.Background(), // use background so it survives gRPC cancellation
		Req:           req,
		CorrelationID: corrID,
	}
	s.router.Dispatch(job)

	// Return immediately
	return &pb.ProcessDocumentResponse{
		Success:       true,
		CorrelationId: corrID,
		Status:        "queued",
	}, nil
}
