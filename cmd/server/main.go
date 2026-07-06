package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"ai_service/internal/ai"
	"ai_service/internal/server"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

func main() {
	// Attempt to load .env file (ignore if it doesn't exist)
	_ = godotenv.Load()

	queueSizeStr := os.Getenv("APP_QUEUE_SIZE")
	if queueSizeStr == "" {
		queueSizeStr = "100"
	}
	_, err := strconv.Atoi(queueSizeStr)
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
			log.Fatalf("Warning: failed to initialize JetStream: %v", err)
		}
	} else {
		log.Fatalf("NATS connection is required for ai_service")
	}

	// Initialize the AI Analyzer with both models
	ollamaAnalyzer := ai.NewOllamaAnalyzer(ollamaHost, ollamaTextModel, ollamaVisionModel)

	natsServer := server.NewNatsServer(nc, js, ollamaAnalyzer, workerPoolSize)

	log.Printf("Ollama Host: %s | Text Model: %s | Vision Model: %s", ollamaHost, ollamaTextModel, ollamaVisionModel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Println("Shutting down AI service...")
		cancel()
	}()

	if err := natsServer.Start(ctx); err != nil {
		log.Fatalf("NATS server failed: %v", err)
	}
}
