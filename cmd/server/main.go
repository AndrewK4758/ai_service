package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/AndrewK4758/shared_utils/envutil"

	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"

	"ai_service/internal/ai"
	"ai_service/internal/server"

	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	logger.InitLogger()
	// Attempt to load .env file (ignore if it doesn't exist)
	envutil.LoadEnv(".env", "ai_service/.env", "../.env", "../../.env")

	queueSizeStr := envutil.GetStringOrDefault("APP_QUEUE_SIZE", "")
	if queueSizeStr == "" {
		queueSizeStr = "100"
	}
	_, err := strconv.Atoi(queueSizeStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid APP_QUEUE_SIZE: %v", err)
	}

	workerPoolSizeStr := envutil.GetStringOrDefault("WORKER_POOL_SIZE", "")
	if workerPoolSizeStr == "" {
		workerPoolSizeStr = "5"
	}
	workerPoolSize, err := strconv.Atoi(workerPoolSizeStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid WORKER_POOL_SIZE: %v", err)
	}

	aiHost := envutil.GetStringOrDefault("OPENAI_BASE_URL", "")
	if aiHost == "" {
		aiHost = "http://localhost:11434/v1" // default for local Ollama via OpenAI spec
	}

	// Configure the dual AI models
	aiTextModel := envutil.GetStringOrDefault("OPENAI_TEXT_MODEL", "gemma4:e4b-extended")
	aiVisionModel := envutil.GetStringOrDefault("OPENAI_VISION_MODEL", "glm-ocr")
	aiEmbeddingModel := envutil.GetEmbeddingModel("all-minilm:latest")
	chunkingRoute := envutil.GetStringOrDefault("MODEL_ROUTE_CHUNKING", "gemma4-extended")
	inferenceRoute := envutil.GetStringOrDefault("MODEL_ROUTE_INFERENCE", "gemma4:e4b-extended")
	extractionRoute := envutil.GetStringOrDefault("MODEL_ROUTE_EXTRACTION", "gemma4:34b-extended")

	// Connect to NATS
	natsURL := envutil.GetStringOrDefault("NATS_URL", "")
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	nc, err := natsutil.ConnectWithRetries(natsURL, "AI_Service")
	if err != nil {
		logger.Fatal("AI_Service", "NATS connection is required for ai_service: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Fatal("AI_Service", "failed to initialize JetStream: %v", err)
	}

	queueFilter := envutil.GetStringOrDefault("QUEUE_MODEL_FILTER", "")
	if queueFilter == "" {
		queueFilter = ">" // consume all by default
	}

	maxAckPendingStr := envutil.GetStringOrDefault("NATS_MAX_ACK_PENDING", "")
	if maxAckPendingStr == "" {
		maxAckPendingStr = strconv.Itoa(workerPoolSize)
	}
	maxAckPending, err := strconv.Atoi(maxAckPendingStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid NATS_MAX_ACK_PENDING: %v", err)
	}

	pullMaxMessagesStr := envutil.GetStringOrDefault("NATS_PULL_MAX_MESSAGES", "")
	if pullMaxMessagesStr == "" {
		pullMaxMessagesStr = strconv.Itoa(workerPoolSize)
	}
	pullMaxMessages, err := strconv.Atoi(pullMaxMessagesStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid NATS_PULL_MAX_MESSAGES: %v", err)
	}

	temperatureStr := envutil.GetStringOrDefault("OPENAI_TEMPERATURE", "")
	if temperatureStr == "" {
		temperatureStr = "0.0"
	}
	temperature, err := strconv.ParseFloat(temperatureStr, 32)
	if err != nil {
		logger.Fatal("AI_Service", "invalid OPENAI_TEMPERATURE: %v", err)
	}

	maxTokensStr := envutil.GetStringOrDefault("OPENAI_MAX_TOKENS", "")
	if maxTokensStr == "" {
		maxTokensStr = "4096"
	}
	maxTokens, err := strconv.Atoi(maxTokensStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid OPENAI_MAX_TOKENS: %v", err)
	}

	timeoutStr := envutil.GetStringOrDefault("OPENAI_TIMEOUT_MINUTES", "")
	if timeoutStr == "" {
		timeoutStr = "5"
	}
	timeout, err := strconv.Atoi(timeoutStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid OPENAI_TIMEOUT_MINUTES: %v", err)
	}

	// Initialize the AI Analyzer config
	config := ai.DefaultLocalConfig()
	config.BaseURL = aiHost
	config.TextModel = aiTextModel
	config.VisionModel = aiVisionModel
	config.EmbeddingModel = aiEmbeddingModel
	config.Temperature = float32(temperature)
	config.MaxTokens = maxTokens
	config.HTTPClient = &http.Client{
		Timeout: time.Duration(timeout) * time.Minute,
	}

	config.AnthropicAPIKey = envutil.GetStringOrDefault("ANTHROPIC_API_KEY", "")
	config.AnthropicBaseURL = envutil.GetStringOrDefault("ANTHROPIC_BASE_URL", "")
	config.AnthropicVersion = envutil.GetStringOrDefault("ANTHROPIC_VERSION", "")
	config.AnthropicTextModel = envutil.GetStringOrDefault("ANTHROPIC_TEXT_MODEL", "")
	config.AnthropicVisionModel = envutil.GetStringOrDefault("ANTHROPIC_VISION_MODEL", "")

	apiType := envutil.GetStringOrDefault("OPENAI_API_TYPE", "")
	if apiType != "" {
		config.APIType = ai.APIType(apiType)
	}
	authToken := envutil.GetStringOrDefault("OPENAI_AUTH_TOKEN", "")
	if authToken != "" {
		config.AuthToken = authToken
	}

	analyzer, err := ai.NewAnalyzerFactory(config)
	if err != nil {
		logger.Fatal("AI_Service", "failed to initialize AI analyzer: %v", err)
	}

	natsServer := server.NewNatsServer(nc, js, analyzer, workerPoolSize, queueFilter, maxAckPending, pullMaxMessages)

	logger.Info("AI_Service", "AI Engine: OpenAI Spec | Host: %s | Text: %s | Vision: %s | Embed: %s | Chunking: %s | Inference: %s | Extraction: %s", aiHost, aiTextModel, aiVisionModel, aiEmbeddingModel, chunkingRoute, inferenceRoute, extractionRoute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		logger.Info("AI_Service", "%v", "Shutting down AI service...")
		cancel()
	}()

	if err := natsServer.Start(ctx); err != nil {
		logger.Fatal("AI_Service", "NATS server failed: %v", err)
	}
}
