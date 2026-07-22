package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/AndrewK4758/shared_utils/logger"

	"ai_service/internal/ai"
	"ai_service/internal/server"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	logger.InitLogger()
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
		log.Fatalf("NATS connection is required for ai_service: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("failed to initialize JetStream: %v", err)
	}

	queueFilter := os.Getenv("QUEUE_MODEL_FILTER")
	if queueFilter == "" {
		queueFilter = ">" // consume all by default
	}

	maxAckPendingStr := os.Getenv("NATS_MAX_ACK_PENDING")
	if maxAckPendingStr == "" {
		maxAckPendingStr = strconv.Itoa(workerPoolSize * 10)
	}
	maxAckPending, err := strconv.Atoi(maxAckPendingStr)
	if err != nil {
		log.Fatalf("invalid NATS_MAX_ACK_PENDING: %v", err)
	}

	pullMaxMessagesStr := os.Getenv("NATS_PULL_MAX_MESSAGES")
	if pullMaxMessagesStr == "" {
		pullMaxMessagesStr = strconv.Itoa(workerPoolSize)
	}
	pullMaxMessages, err := strconv.Atoi(pullMaxMessagesStr)
	if err != nil {
		log.Fatalf("invalid NATS_PULL_MAX_MESSAGES: %v", err)
	}

	temperatureStr := os.Getenv("OPENAI_TEMPERATURE")
	if temperatureStr == "" {
		temperatureStr = "0.0"
	}
	temperature, err := strconv.ParseFloat(temperatureStr, 32)
	if err != nil {
		log.Fatalf("invalid OPENAI_TEMPERATURE: %v", err)
	}

	maxTokensStr := os.Getenv("OPENAI_MAX_TOKENS")
	if maxTokensStr == "" {
		maxTokensStr = "4096"
	}
	maxTokens, err := strconv.Atoi(maxTokensStr)
	if err != nil {
		log.Fatalf("invalid OPENAI_MAX_TOKENS: %v", err)
	}

	timeoutStr := os.Getenv("OPENAI_TIMEOUT_MINUTES")
	if timeoutStr == "" {
		timeoutStr = "5"
	}
	timeout, err := strconv.Atoi(timeoutStr)
	if err != nil {
		log.Fatalf("invalid OPENAI_TIMEOUT_MINUTES: %v", err)
	}

	// Initialize the AI Analyzer config
	config := ai.DefaultLocalConfig()
	config.BaseURL = aiHost
	config.TextModel = aiTextModel
	config.VisionModel = aiVisionModel
	config.Temperature = float32(temperature)
	config.MaxTokens = maxTokens
	config.HTTPClient = &http.Client{
		Timeout: time.Duration(timeout) * time.Minute,
	}

	apiType := os.Getenv("OPENAI_API_TYPE")
	if apiType != "" {
		config.APIType = ai.APIType(apiType)
	}
	authToken := os.Getenv("OPENAI_AUTH_TOKEN")
	if authToken != "" {
		config.AuthToken = authToken
	}

	analyzer, err := ai.NewAnalyzerFactory(config)
	if err != nil {
		log.Fatalf("failed to initialize AI analyzer: %v", err)
	}

	natsServer := server.NewNatsServer(nc, js, analyzer, workerPoolSize, queueFilter, maxAckPending, pullMaxMessages)

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
