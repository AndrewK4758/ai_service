package server

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"ai_service/internal/ai"
	"ai_service/internal/parser"
	pb "ai_service/proto"
)

type DocumentProcessorServer struct {
	pb.UnimplementedDocumentProcessorServer
	analyzer ai.Analyzer
}

func NewDocumentProcessorServer(analyzer ai.Analyzer) *DocumentProcessorServer {
	return &DocumentProcessorServer{
		analyzer: analyzer,
	}
}

func (s *DocumentProcessorServer) ProcessDocument(ctx context.Context, req *pb.ProcessDocumentRequest) (*pb.ProcessDocumentResponse, error) {
	fileName := req.GetFileName()
	ext := strings.ToLower(filepath.Ext(fileName))

	log.Printf("Received document: %s, Extension: %s", fileName, ext)

	// Build the dynamic prompt options from the gRPC request
	opts := ai.AnalyzeOptions{
		DocumentType:       req.GetDocumentType(),
		TargetFields:       req.GetTargetFields(),
		JsonSchema:         req.GetJsonSchema(),
		CustomInstructions: req.GetCustomInstructions(),
	}

	var metadataJson string
	var err error

	// The Smart Router
	switch ext {
	case ".docx":
		extractedText, parseErr := parser.ExtractDocxText(req.GetFileContent())
		if parseErr != nil {
			return s.formatErrorResponse("Failed to parse DOCX", parseErr), nil
		}
		metadataJson, err = s.analyzer.AnalyzeText(ctx, extractedText, opts)

	case ".pdf":
		extractedText, parseErr := parser.ExtractPdfText(req.GetFileContent())
		if parseErr != nil {
			return s.formatErrorResponse("Failed to parse PDF", parseErr), nil
		}
		metadataJson, err = s.analyzer.AnalyzeText(ctx, extractedText, opts)

	case ".png", ".jpg", ".jpeg":
		metadataJson, err = s.analyzer.AnalyzeImage(ctx, req.GetFileContent(), opts)

	default:
		return s.formatErrorResponse(fmt.Sprintf("Unsupported file extension: %s", ext), nil), nil
	}

	if err != nil {
		log.Printf("AI analysis failed: %v", err)
		return s.formatErrorResponse("AI processing failed", err), nil
	}

	return &pb.ProcessDocumentResponse{
		Success:      true,
		MetadataJson: metadataJson,
	}, nil
}

func (s *DocumentProcessorServer) formatErrorResponse(msg string, err error) *pb.ProcessDocumentResponse {
	return &pb.ProcessDocumentResponse{
		Success:      false,
		ErrorMessage: fmt.Sprintf("%s: %v", msg, err),
	}
}
