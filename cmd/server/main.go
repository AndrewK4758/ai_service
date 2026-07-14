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
	"github.com/nats-io/nats.go/jetstream"
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

	aiHost := os.Getenv("OPENAI_BASE_URL")
	if aiHost == "" {
		aiHost = "http://localhost:11434/v1" // default for local Ollama via OpenAI spec
	}

	// Configure the dual AI models
	aiTextModel := os.Getenv("OPENAI_TEXT_MODEL")
	if aiTextModel == "" {
		aiTextModel = "llama3" // Standard model for DOCX/PDF
	}

	aiVisionModel := os.Getenv("OPENAI_VISION_MODEL")
	if aiVisionModel == "" {
		aiVisionModel = "glm-ocr" // Vision model for images
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

	var js jetstream.JetStream
	if nc != nil {
		js, err = jetstream.New(nc)
		if err != nil {
			log.Fatalf("Warning: failed to initialize JetStream: %v", err)
		}
	} else {
		log.Fatalf("NATS connection is required for ai_service")
	}

	queueFilter := os.Getenv("QUEUE_MODEL_FILTER")
	if queueFilter == "" {
		queueFilter = ">" // consume all by default
	}

	// Initialize the AI Analyzer (Strictly OpenAI Spec)
	analyzer := ai.NewOpenAIAnalyzer(aiHost, aiTextModel, aiVisionModel)

	natsServer := server.NewNatsServer(nc, js, analyzer, workerPoolSize, queueFilter)

	log.Printf("AI Engine: OpenAI Spec | Host: %s | Text Model: %s | Vision Model: %s", aiHost, aiTextModel, aiVisionModel)

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
