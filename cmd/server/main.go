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
	"ai_service/internal/storage"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Attempt to load .env file (ignore if it doesn't exist)
	envutil.LoadEnv(".env", "ai_service/.env", "../.env", "../../.env")
	logger.InitLogger()

	queueSizeStr := envutil.MustGetString("APP_QUEUE_SIZE")
	_, err := strconv.Atoi(queueSizeStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid APP_QUEUE_SIZE: %v", err)
	}

	workerPoolSizeStr := envutil.MustGetString("WORKER_POOL_SIZE")
	workerPoolSize, err := strconv.Atoi(workerPoolSizeStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid WORKER_POOL_SIZE: %v", err)
	}

	aiHost := envutil.MustGetString("OPENAI_BASE_URL")

	// Configure the dual AI models
	aiTextModel := envutil.MustGetString("OPENAI_TEXT_MODEL")
	aiVisionModel := envutil.MustGetString("OPENAI_VISION_MODEL")
	aiEmbeddingModel := envutil.MustGetEmbeddingModel()
	chunkingRoute := envutil.MustGetString("MODEL_ROUTE_CHUNKING")
	inferenceRoute := envutil.MustGetString("MODEL_ROUTE_INFERENCE")
	extractionRoute := envutil.MustGetString("MODEL_ROUTE_EXTRACTION")

	// Connect to NATS
	natsURL := envutil.MustGetString("NATS_URL")
	nc, err := natsutil.ConnectWithRetries(natsURL, "AI_Service")
	if err != nil {
		logger.Fatal("AI_Service", "NATS connection is required for ai_service: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Fatal("AI_Service", "failed to initialize JetStream: %v", err)
	}

	queueFilter := envutil.MustGetString("QUEUE_MODEL_FILTER")

	maxAckPendingStr := envutil.MustGetString("NATS_MAX_ACK_PENDING")
	maxAckPending, err := strconv.Atoi(maxAckPendingStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid NATS_MAX_ACK_PENDING: %v", err)
	}

	pullMaxMessagesStr := envutil.MustGetString("NATS_PULL_MAX_MESSAGES")
	pullMaxMessages, err := strconv.Atoi(pullMaxMessagesStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid NATS_PULL_MAX_MESSAGES: %v", err)
	}

	temperatureStr := envutil.MustGetString("OPENAI_TEMPERATURE")
	temperature, err := strconv.ParseFloat(temperatureStr, 32)
	if err != nil {
		logger.Fatal("AI_Service", "invalid OPENAI_TEMPERATURE: %v", err)
	}

	maxTokensStr := envutil.MustGetString("OPENAI_MAX_TOKENS")
	maxTokens, err := strconv.Atoi(maxTokensStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid OPENAI_MAX_TOKENS: %v", err)
	}

	timeoutStr := envutil.MustGetString("OPENAI_TIMEOUT_MINUTES")
	timeout, err := strconv.Atoi(timeoutStr)
	if err != nil {
		logger.Fatal("AI_Service", "invalid OPENAI_TIMEOUT_MINUTES: %v", err)
	}

	// Initialize the AI Analyzer config
	config := ai.DefaultLocalConfig()
	config.BaseURL = aiHost
	config.TextModel = aiTextModel
	config.VisionModel = aiVisionModel
	visionBaseURL := envutil.MustGetString("OPENAI_VISION_BASE_URL")
	config.VisionBaseURL = visionBaseURL
	config.EmbeddingModel = aiEmbeddingModel
	config.Temperature = float32(temperature)
	config.MaxTokens = maxTokens
	config.HTTPClient = &http.Client{
		Timeout: time.Duration(timeout) * time.Minute,
	}

	config.AnthropicAPIKey = envutil.MustGetString("ANTHROPIC_API_KEY")
	config.AnthropicBaseURL = envutil.MustGetString("ANTHROPIC_BASE_URL")
	config.AnthropicVersion = envutil.MustGetString("ANTHROPIC_VERSION")
	config.AnthropicTextModel = envutil.MustGetString("ANTHROPIC_TEXT_MODEL")
	config.AnthropicVisionModel = envutil.MustGetString("ANTHROPIC_VISION_MODEL")

	apiType := envutil.MustGetString("OPENAI_API_TYPE")
	config.APIType = ai.APIType(apiType)
	authToken := envutil.MustGetString("OPENAI_AUTH_TOKEN")
	config.AuthToken = authToken

	analyzer, err := ai.NewAnalyzerFactory(config)
	if err != nil {
		logger.Fatal("AI_Service", "failed to initialize AI analyzer: %v", err)
	}

	gatewayAddr := envutil.MustGetString("STORAGE_GATEWAY_ADDR")
	gatewayConn, err := grpc.NewClient(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("AI_Service", "failed to dial storage_gateway: %v", err)
	}
	defer gatewayConn.Close()
	store := storage.NewGatewayStorage(pb.NewStorageGatewayServiceClient(gatewayConn))
	maxDocTokens := envutil.MustGetInt("LARGE_DOCUMENT_MAX_TOKENS")
	natsServer := server.NewNatsServer(nc, js, analyzer, store, workerPoolSize, queueFilter, maxAckPending, pullMaxMessages, maxDocTokens)

	visionHost := visionBaseURL
	logger.Info("AI_Service", "AI Engine: OpenAI Spec | Host: %s | Text: %s | Vision: %s (Host: %s) | Embed: %s | Chunking: %s | Inference: %s | Extraction: %s", aiHost, aiTextModel, aiVisionModel, visionHost, aiEmbeddingModel, chunkingRoute, inferenceRoute, extractionRoute)

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
