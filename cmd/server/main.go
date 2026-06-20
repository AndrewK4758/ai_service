package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"ai_service/internal/ai"
	"ai_service/internal/server"
	pb "ai_service/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	port := 50051
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	ollamaHost := os.Getenv("OLLAMA_HOST")
	if ollamaHost == "" {
		ollamaHost = "http://host.docker.internal:11434"
	}

	// Configure the dual AI models
	ollamaTextModel := os.Getenv("OLLAMA_TEXT_MODEL")
	if ollamaTextModel == "" {
		ollamaTextModel = "llama3" // Standard model for DOCX/PDF
	}

	ollamaVisionModel := os.Getenv("OLLAMA_VISION_MODEL")
	if ollamaVisionModel == "" {
		ollamaVisionModel = "glm-ocr" // Vision model for images
	}

	// Initialize the AI Analyzer with both models
	ollamaAnalyzer := ai.NewOllamaAnalyzer(ollamaHost, ollamaTextModel, ollamaVisionModel)

	// Inject dependency into server
	grpcServerImpl := server.NewDocumentProcessorServer(ollamaAnalyzer)

	// Start gRPC server
	s := grpc.NewServer()
	pb.RegisterDocumentProcessorServer(s, grpcServerImpl)
	reflection.Register(s)

	log.Printf("Go AI Processing Service listening at %v", lis.Addr())
	log.Printf("Ollama Host: %s | Text Model: %s | Vision Model: %s", ollamaHost, ollamaTextModel, ollamaVisionModel)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
