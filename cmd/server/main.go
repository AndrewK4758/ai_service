package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"ai_service/internal/ai"
	"ai_service/internal/server"
	pb "doc_processor/shared_protos"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Attempt to load .env file (ignore if it doesn't exist)
	_ = godotenv.Load()
	portStr := os.Getenv("SERVER_PORT")
	if portStr == "" {
		portStr = "50051"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("invalid SERVER_PORT: %v", err)
	}

	queueSizeStr := os.Getenv("APP_QUEUE_SIZE")
	if queueSizeStr == "" {
		queueSizeStr = "100"
	}
	queueSize, err := strconv.Atoi(queueSizeStr)
	if err != nil {
		log.Fatalf("invalid APP_QUEUE_SIZE: %v", err)
	}

	workerPoolSizeStr := os.Getenv("WORKER_POOL_SIZE")
	if workerPoolSizeStr == "" {
		workerPoolSizeStr = "5"
	}
	workerPoolSize, err := strconv.Atoi(workerPoolSizeStr)
	if err != nil {
		log.Fatalf("invalid WORKER_POOL_SIZE: %v", err)
	}

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

	// Connect to NATS
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("Warning: failed to connect to NATS (%v), JetStream publishing will be disabled", err)
	} else {
		defer nc.Close()
	}

	var js nats.JetStreamContext
	if nc != nil {
		js, err = nc.JetStream()
		if err != nil {
			log.Printf("Warning: failed to initialize JetStream: %v", err)
		} else {
			// Ensure the stream exists for our subjects
			streamName := "AI_RESPONSES"
			_, err = js.StreamInfo(streamName)
			if err != nil {
				// Stream doesn't exist, create it
				_, err = js.AddStream(&nats.StreamConfig{
					Name:     streamName,
					Subjects: []string{"ai.processed.*"},
				})
				if err != nil {
					log.Printf("Warning: failed to create JetStream stream: %v", err)
				} else {
					log.Printf("Successfully created JetStream stream: %s", streamName)
				}
			}
		}
	}

	// Initialize the AI Analyzer with both models
	ollamaAnalyzer := ai.NewOllamaAnalyzer(ollamaHost, ollamaTextModel, ollamaVisionModel)

	// Create the AppRouter for isolated async queues
	router := server.NewAppRouter(ollamaAnalyzer, js, queueSize, workerPoolSize)

	// Inject dependency into server
	grpcServerImpl := server.NewDocumentProcessorServer(ollamaAnalyzer, router)

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
