package server

import (
	"context"
	"fmt"
	"log"

	"ai_service/internal/ai"
	"ai_service/internal/parser"
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

func (s *DocumentProcessorServer) ProcessDocument(_ context.Context, req *pb.ProcessDocumentRequest) (*pb.ProcessDocumentResponse, error) {
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

func (s *DocumentProcessorServer) PerformAction(ctx context.Context, req *pb.PerformActionRequest) (*pb.PerformActionResponse, error) {
	log.Printf("Received synchronous PerformAction request")

	opts := ai.AnalyzeOptions{
		CustomInstructions: req.GetPrompt(),
		JsonSchema:         req.GetJsonSchema(),
	}

	var text string
	var err error

	if req.GetInputText() != "" {
		text = req.GetInputText()
	} else if len(req.GetInputBytes()) > 0 {
		text, err = parser.ExtractPdfText(req.GetInputBytes())
		if err != nil {
			wrappedErr := fmt.Errorf("failed to extract text from binary input: %w", err)
			log.Printf("ERROR: [PerformAction] %v", wrappedErr)
			return &pb.PerformActionResponse{
				Success:      false,
				ErrorMessage: wrappedErr.Error(),
			}, nil
		}
	}

	result, err := s.analyzer.AnalyzeText(ctx, text, opts)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to perform action on text: %w", err)
		log.Printf("ERROR: [PerformAction] %v", wrappedErr)
		return &pb.PerformActionResponse{
			Success:      false,
			ErrorMessage: wrappedErr.Error(),
		}, nil
	}

	return &pb.PerformActionResponse{
		Success:          true,
		ActionResultJson: result,
	}, nil
}
