package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AndrewK4758/shared_utils/envutil"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"ai_service/internal/ai"
	docparser "ai_service/internal/parser"
	"ai_service/internal/storage"

	"github.com/AndrewK4758/shared_utils/identityparser"
	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"
	"github.com/AndrewK4758/shared_utils/workflowparser"

	pb "github.com/AndrewK4758/shared_protos"
)

type NatsServer struct {
	nc                *nats.Conn
	js                jetstream.JetStream
	analyzer          ai.Analyzer
	store             storage.Manager
	workerPoolSize    int
	queueFilter       string
	maxAckPending     int
	pullMaxMessages   int
	maxDocumentTokens int
	keepAliveManager  *KeepAliveManager
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream, analyzer ai.Analyzer, store storage.Manager, workerPoolSize int, queueFilter string, maxAckPending int, pullMaxMessages int, maxDocumentTokens int) *NatsServer {
	ackWaitStr := envutil.MustGetString("WORKER_ACK_WAIT")
	ackWait, err := time.ParseDuration(ackWaitStr)
	if err != nil {
		panic(fmt.Errorf("invalid WORKER_ACK_WAIT: %w", err))
	}
	return &NatsServer{
		nc:                nc,
		js:                js,
		analyzer:          analyzer,
		store:             store,
		workerPoolSize:    workerPoolSize,
		queueFilter:       queueFilter,
		maxAckPending:     maxAckPending,
		pullMaxMessages:   pullMaxMessages,
		maxDocumentTokens: maxDocumentTokens,
		keepAliveManager:  NewKeepAliveManager(ackWait / 2),
	}
}

func (s *NatsServer) Start(ctx context.Context) error {
	streamName := envutil.MustGetString("NATS_WORKER_STREAM")
	if streamName == "" {
		panic(fmt.Errorf("NATS_WORKER_STREAM is missing"))
	}

	durableName := envutil.GetStringOrDefault("NATS_CONSUMER_NAME", "ai_service_workers_all")
	var filterSubjects []string
	if s.queueFilter == ">" {
		filterSubjects = []string{"worker.ai.>", "ai.action.>"}
	} else {
		filterSubjects = strings.Split(s.queueFilter, ",")
		for i := range filterSubjects {
			filterSubjects[i] = strings.TrimSpace(filterSubjects[i])
		}
	}

	ackWaitStr := envutil.MustGetString("WORKER_ACK_WAIT")
	ackWait, err := time.ParseDuration(ackWaitStr)
	if err != nil {
		panic(fmt.Errorf("invalid WORKER_ACK_WAIT: %w", err))
	}

	maxAck := envutil.MustGetInt("MAX_ACK_PENDING")
	maxDeliver := envutil.MustGetInt("NATS_MAX_DELIVER")
	retryCount := envutil.MustGetInt("NATS_CONSUMER_RETRY_COUNT")
	retrySleepSec := envutil.MustGetInt("NATS_CONSUMER_RETRY_SLEEP_SEC")

	var cons jetstream.Consumer
	for range retryCount {
		cons, err = s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Durable:        durableName,
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: filterSubjects,
			MaxAckPending:  maxAck,
			AckWait:        ackWait,
			MaxDeliver:     maxDeliver,
		})
		if err == nil {
			break
		}
		logger.Info("AI_Service", "Waiting for stream %s to be created... (%v)", streamName, err)
		time.Sleep(time.Duration(retrySleepSec) * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to create JetStream primary consumer after retries: %w", err)
	}

	logger.Info("AI_Service", "Starting AI consumer '%s' with WorkerPoolSize=%d, MaxAckPending=%d...", durableName, s.workerPoolSize, maxAck)
	sem := make(chan struct{}, s.workerPoolSize)

	// Start primary consumer
	var cc jetstream.ConsumeContext
	cc, err = cons.Consume(func(msg jetstream.Msg) {
		go s.handleMessage(ctx, msg, sem)
	}, jetstream.PullMaxMessages(s.pullMaxMessages))
	if err != nil {
		return fmt.Errorf("failed to start primary consuming: %w", err)
	}

	<-ctx.Done()
	if cc != nil {
		cc.Stop()
	}
	return nil
}

func (s *NatsServer) handleMessage(ctx context.Context, m jetstream.Msg, sem chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			t, a, j := natsutil.ExtractIdentityFromAiSubject(m.Subject())
			logger.Info("AI_Service", "Recovered from panic in worker goroutine for Tenant=%s App=%s Job=%s: %v", t, a, j, r)
			
			natsErrorTimeoutStr := envutil.GetStringOrDefault("NATS_NACK_ERROR_TIMEOUT", "15")
			natsErrorTimeout, errParse := strconv.Atoi(natsErrorTimeoutStr)
			if errParse != nil || natsErrorTimeout <= 0 {
				natsErrorTimeout = 15
			}
			_ = m.NakWithDelay(time.Duration(natsErrorTimeout) * time.Second)
		}
	}()

	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
		s.processMessage(ctx, m)
	case <-ctx.Done():
		_ = m.Nak()
	}
}

func getReplySubject(msg jetstream.Msg) string {
	if msg == nil {
		return ""
	}
	headers := msg.Headers()
	if headers == nil {
		return ""
	}
	return headers.Get("Reply-Subject")
}

func (s *NatsServer) processMessage(ctx context.Context, msg jetstream.Msg) {
	startTime := time.Now()
	meta, err := msg.Metadata()
	var attempt uint64 = 1
	if meta != nil {
		attempt = meta.NumDelivered
	}

	durableName := "ai_service_workers_all"

	t, a, j := natsutil.ExtractIdentityFromAiSubject(msg.Subject())
	identity := &pb.InfrastructureIdentity{TenantId: t, AppId: a, JobId: j}

	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "AI_Service",
		Event:     "DEQUEUED",
		Stream:    "WORKERS",
		Subject:   msg.Subject(),
		Consumer:  durableName,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		SizeBytes: len(msg.Data()),
		Attempt:   attempt,
	})

	if err == nil && meta.NumDelivered > 5 {
		s.handleExceededRetries(ctx, msg, durableName, identity, attempt, startTime)
		return
	}

	if ctx.Err() != nil {
		s.handleContextCanceled(msg, durableName, identity, attempt, startTime)
		return
	}

	msgId := fmt.Sprintf("%p", msg)
	s.keepAliveManager.Add(msgId, msg)
	defer s.keepAliveManager.Remove(msgId)


	logger.Info("AI_Service", "Received action request on subject: %s", msg.Subject())
	replySubj := getReplySubject(msg)

	req := &pb.ExecuteTaskRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		panic(fmt.Errorf("failed to unmarshal ExecuteTaskRequest: %w", err))
	}

	logger.Info("AI_Service", "[DEQUEUED PAYLOAD] Processing JobID: %s, Step: %s", req.GetIdentity().GetJobId(), req.GetTaskSubject())

	if reqJson, err := protojson.Marshal(req); err == nil {
		logger.Debug("AI_Service", "[DEQUEUED PAYLOAD] Full ExecuteTaskRequest: %s", string(reqJson))
	}

	if req.Payload != nil {
		logger.Debug("Input payload", "%s: %+v", "payload", req.Payload.AsMap())
	}

	// Fail-Fast Assertion 1: Infrastructure Identity Validation
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		panic(fmt.Sprintf("action failed: invalid identity - %v", err))
	}

	identity = req.GetIdentity()

	// Fail-Fast Assertion 2: Required TaskSubject Validation
	if req.GetTaskSubject() == "" {
		panic("action failed: missing required TaskSubject")
	}

	targetSubj := req.GetTaskSubject()

	isScanned, hasFlag, err := extractIsScannedFlag(req)
	if err != nil {
		panic(fmt.Errorf("failed to extract is_scanned flag: %w", err))
	}
	var modelID string
	if hasFlag {
		if isScanned {
			modelID = envutil.MustGetString("OPENAI_VISION_MODEL")
		} else {
			modelID = envutil.MustGetString("OPENAI_TEXT_MODEL")
		}
	} else {
		modelID, err = s.resolveModelID(targetSubj)
		if err != nil {
			panic(fmt.Errorf("failed to resolve model ID: %w", err))
		}
	}

	result, errCode, err := s.executeAction(ctx, req, targetSubj, modelID)
	if err != nil {
		s.handleExecutionError(ctx, msg, replySubj, req, errCode, err, durableName, identity, attempt, startTime)
		return
	}

	s.handleExecutionSuccess(ctx, msg, replySubj, req, result, durableName, identity, attempt, startTime)
}

func (s *NatsServer) handleExceededRetries(ctx context.Context, msg jetstream.Msg, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	logger.Info("AI_Service", "Message exceeded max delivery attempts, terminating")
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "TERMINATED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   identity.GetTenantId(),
		AppID:      identity.GetAppId(),
		JobID:      identity.GetJobId(),
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      fmt.Errorf("exceeded max delivery attempts (5)"),
	})
	replySubj := getReplySubject(msg)
	errReply := s.sendErrorReply(ctx, replySubj, "[Step: WORKER_DEQUEUE, URI: unknown] Action failed after multiple retries due to rate limits.", "EXCEEDED_MAX_RETRIES")
	if errReply != nil {
		logger.Info("AI_Service", "Failed to send error reply: %v", errReply)
	}
	_ = msg.Term()
}

func (s *NatsServer) handleContextCanceled(msg jetstream.Msg, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	logger.Info("AI_Service", "Context canceled, NAKing message")
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "NACKED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   identity.GetTenantId(),
		AppID:      identity.GetAppId(),
		JobID:      identity.GetJobId(),
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
	})
	_ = msg.Nak()
}

func (s *NatsServer) handleUnmarshalError(ctx context.Context, msg jetstream.Msg, replySubj string, err error, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	logger.Info("AI_Service", "Error unmarshaling request: %v", err)
	errMsg := fmt.Sprintf("[Step: WORKER_DEQUEUE, URI: unknown] invalid payload: %v", err)
	errReply := s.sendErrorReply(ctx, replySubj, errMsg, "INVALID_PAYLOAD")
	if errReply != nil {
		logger.Info("AI_Service", "Failed to send error reply: %v", errReply)
	}
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "TERMINATED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   identity.GetTenantId(),
		AppID:      identity.GetAppId(),
		JobID:      identity.GetJobId(),
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.Term()
}

func (s *NatsServer) executeAction(ctx context.Context, req *pb.ExecuteTaskRequest, targetSubj string, modelID string) (string, string, error) {
	docUri, err := getDocumentUri(req)
	if err != nil {
		return "", "INVALID_PAYLOAD", fmt.Errorf("[Step: EXECUTE_ACTION, URI: unknown] failed to get document URI: %w", err)
	}
	if docUri != "" {
		return s.processDocumentJIT(ctx, docUri, req, modelID, targetSubj)
	}
	res, err := s.analyzer.AnalyzeText(ctx, "", req, nil, modelID)
	if err != nil {
		step := getStepName(req)
		return "", "AI_PROCESSING_FAILED", fmt.Errorf("[Step: %s, URI: N/A] failed to perform action on payload: %w", step, err)
	}
	return res, "", nil
}

func (s *NatsServer) processDocumentJIT(ctx context.Context, docUri string, req *pb.ExecuteTaskRequest, modelID string, targetSubj string) (string, string, error) {
	step := getStepName(req)
	if s.store == nil {
		return "", "STORAGE_FETCH_FAILED", fmt.Errorf("[Step: %s, URI: %s] storage manager is not configured", step, docUri)
	}

	fileReader, err := s.store.FetchFile(ctx, docUri, req.Identity)
	if err != nil {
		return "", "STORAGE_FETCH_FAILED", fmt.Errorf("[Step: %s, URI: %s] failed to fetch document from storage: %w", step, docUri, err)
	}

	fileBytes, err := io.ReadAll(fileReader)
	if err != nil {
		return "", "STORAGE_FETCH_FAILED", fmt.Errorf("[Step: %s, URI: %s] failed to read document stream: %w", step, docUri, err)
	}

	return s.dispatchBytesToAnalyzer(ctx, fileBytes, docUri, req, modelID, targetSubj)
}

func EstimateDocumentTokens(text string, images [][]byte) int {
	tokens := 0
	if len(text) > 0 {
		tokens += (len(text) + 3) / 4
	}
	if len(images) > 0 {
		tokens += len(images) * 765
	}
	return tokens
}

func (s *NatsServer) checkDocumentTokenLimit(text string, images [][]byte, step string, docUri string) (string, error) {
	if s.maxDocumentTokens <= 0 {
		return "", nil
	}
	tokenCount := EstimateDocumentTokens(text, images)
	if tokenCount <= s.maxDocumentTokens {
		return "", nil
	}
	return "LARGE_DOCUMENT_EXCEEDED", fmt.Errorf("[Step: %s, URI: %s] document token count (%d) exceeds max token limit (%d)", step, docUri, tokenCount, s.maxDocumentTokens)
}

func (s *NatsServer) analyzeTextWithTokenCheck(ctx context.Context, text string, step string, docUri string, req *pb.ExecuteTaskRequest, modelID string) (string, string, error) {
	if errCode, tokenErr := s.checkDocumentTokenLimit(text, nil, step, docUri); tokenErr != nil {
		return "", errCode, tokenErr
	}
	res, analyzeErr := s.analyzer.AnalyzeText(ctx, text, req, nil, modelID)
	if analyzeErr != nil {
		return "", "AI_PROCESSING_FAILED", fmt.Errorf("[Step: %s, URI: %s] failed to perform action on payload: %w", step, docUri, analyzeErr)
	}
	return res, "", nil
}

func (s *NatsServer) analyzeImageWithTokenCheck(ctx context.Context, images [][]byte, step string, docUri string, req *pb.ExecuteTaskRequest, modelID string) (string, string, error) {
	if errCode, tokenErr := s.checkDocumentTokenLimit("", images, step, docUri); tokenErr != nil {
		return "", errCode, tokenErr
	}
	res, analyzeErr := s.analyzer.AnalyzeImage(ctx, images, req, nil, modelID)
	if analyzeErr != nil {
		return "", "AI_PROCESSING_FAILED", fmt.Errorf("[Step: %s, URI: %s] failed to perform action on payload: %w", step, docUri, analyzeErr)
	}
	return res, "", nil
}

func (s *NatsServer) dispatchBytesToAnalyzer(ctx context.Context, fileBytes []byte, docUri string, req *pb.ExecuteTaskRequest, modelID string, targetSubj string) (string, string, error) {
	step := getStepName(req)
	ext := strings.ToLower(filepath.Ext(docUri))

	isVision := isVisionRequest(targetSubj, modelID, ext)

	if isVision {
		return s.processVisionBytes(ctx, fileBytes, ext, step, docUri, req, modelID)
	}

	if ext == ".pdf" {
		return s.processPdfTextBytes(ctx, fileBytes, step, docUri, req, modelID)
	}

	if ext == ".docx" {
		return s.processDocxTextBytes(ctx, fileBytes, step, docUri, req, modelID)
	}

	text := string(fileBytes)
	fileBytes = nil
	return s.analyzeTextWithTokenCheck(ctx, text, step, docUri, req, modelID)
}

func (s *NatsServer) processVisionBytes(ctx context.Context, fileBytes []byte, ext string, step string, docUri string, req *pb.ExecuteTaskRequest, modelID string) (string, string, error) {
	if ext == ".pdf" {
		return s.processPdfVisionBytes(ctx, fileBytes, step, docUri, req, modelID)
	}
	return s.processRawImageBytes(ctx, fileBytes, step, docUri, req, modelID)
}

func (s *NatsServer) processPdfVisionBytes(ctx context.Context, fileBytes []byte, step string, docUri string, req *pb.ExecuteTaskRequest, modelID string) (string, string, error) {
	images, err := docparser.ConvertPdfToImages(fileBytes)
	fileBytes = nil
	if err != nil {
		return "", "AI_PROCESSING_FAILED", fmt.Errorf("[Step: %s, URI: %s] pdftoppm failed: %w", step, docUri, err)
	}
	return s.analyzeImageWithTokenCheck(ctx, images, step, docUri, req, modelID)
}

func (s *NatsServer) processRawImageBytes(ctx context.Context, fileBytes []byte, step string, docUri string, req *pb.ExecuteTaskRequest, modelID string) (string, string, error) {
	images := [][]byte{fileBytes}
	fileBytes = nil
	return s.analyzeImageWithTokenCheck(ctx, images, step, docUri, req, modelID)
}

func (s *NatsServer) processPdfTextBytes(ctx context.Context, fileBytes []byte, step string, docUri string, req *pb.ExecuteTaskRequest, modelID string) (string, string, error) {
	text, err := docparser.ExtractPdfText(fileBytes)
	fileBytes = nil
	if err != nil {
		return "", "AI_PROCESSING_FAILED", fmt.Errorf("[Step: %s, URI: %s] pdftotext failed: %w", step, docUri, err)
	}
	return s.analyzeTextWithTokenCheck(ctx, text, step, docUri, req, modelID)
}

func (s *NatsServer) processDocxTextBytes(ctx context.Context, fileBytes []byte, step string, docUri string, req *pb.ExecuteTaskRequest, modelID string) (string, string, error) {
	text, err := docparser.ExtractDocxText(fileBytes)
	fileBytes = nil
	if err != nil {
		return "", "AI_PROCESSING_FAILED", fmt.Errorf("[Step: %s, URI: %s] docx extraction failed: %w", step, docUri, err)
	}
	return s.analyzeTextWithTokenCheck(ctx, text, step, docUri, req, modelID)
}

func getStepName(req *pb.ExecuteTaskRequest) string {
	if req == nil {
		return "UNKNOWN"
	}
	if step := req.StepId; step != "" {
		return step
	}
	return "AI_PROCESSING"
}

func isVisionRequest(targetSubj string, modelID string, ext string) bool {
	subjLower := strings.ToLower(targetSubj)
	visionSubjects := envutil.GetStringOrDefault("VISION_SUBJECTS", "vision,ocr,image")
	for _, v := range strings.Split(visionSubjects, ",") {
		if strings.Contains(subjLower, strings.TrimSpace(v)) {
			return true
		}
	}

	modelLower := strings.ToLower(modelID)
	visionModels := envutil.GetStringOrDefault("VISION_MODELS", "vision,gpt-4o,claude-3")
	for _, m := range strings.Split(visionModels, ",") {
		if strings.Contains(modelLower, strings.TrimSpace(m)) {
			return true
		}
	}

	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" || ext == ".webp" || ext == ".tiff" || ext == ".bmp" {
		return true
	}
	return false
}

func (s *NatsServer) handleExecutionError(ctx context.Context, msg jetstream.Msg, replySubj string, req *pb.ExecuteTaskRequest, errCode string, err error, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	if ctx.Err() != nil {
		s.handleContextCanceled(msg, durableName, identity, attempt, startTime)
		return
	}
	logger.Info("AI_Service", "AI processing failed: %v", err)
	errStr := strings.ToLower(err.Error())

	if errCode != "LARGE_DOCUMENT_EXCEEDED" {
		isRateLimit := strings.Contains(errStr, "429") || strings.Contains(errStr, "too many requests") || strings.Contains(errStr, "throttlingexception") || strings.Contains(errStr, "quota")
		if isRateLimit {
			s.handleRateLimitError(msg, err, durableName, identity, attempt, startTime)
			return
		}

		isTempError := strings.Contains(errStr, "500") || strings.Contains(errStr, "502") || strings.Contains(errStr, "503") || strings.Contains(errStr, "504") || strings.Contains(errStr, "timeout") || strings.Contains(errStr, "eof")
		if isTempError {
			s.handleTempError(msg, err, durableName, identity, attempt, startTime)
			return
		}
	}

	if errCode == "" {
		errCode = "AI_PROCESSING_FAILED"
	}
	errReply := s.sendErrorReply(ctx, replySubj, err.Error(), errCode)
	if errReply != nil {
		logger.Info("AI_Service", "Failed to send error reply: %v", errReply)
	}
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "TERMINATED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   identity.GetTenantId(),
		AppID:      identity.GetAppId(),
		JobID:      identity.GetJobId(),
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.Term()
}

func (s *NatsServer) handleRateLimitError(msg jetstream.Msg, err error, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	natsRateLimitDelayStr := envutil.MustGetString("NATS_RATE_LIMIT_DELAY")
	natsRateLimitDelay, errParse := strconv.Atoi(natsRateLimitDelayStr)
	if errParse != nil || natsRateLimitDelay <= 0 {
		natsRateLimitDelay = 5
	}
	backoffDelay := time.Duration(1<<attempt) * time.Duration(natsRateLimitDelay) * time.Second
	logger.Info("AI_Service", "Rate limited by provider, applying NakWithDelay of %v", backoffDelay)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "NACKED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   identity.GetTenantId(),
		AppID:      identity.GetAppId(),
		JobID:      identity.GetJobId(),
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.NakWithDelay(backoffDelay)
}

func (s *NatsServer) handleTempError(msg jetstream.Msg, err error, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	natsErrorTimeoutStr := envutil.MustGetString("NATS_NACK_ERROR_TIMEOUT")
	natsErrorTimeout, errParse := strconv.Atoi(natsErrorTimeoutStr)
	if errParse != nil || natsErrorTimeout <= 0 {
		natsErrorTimeout = 15
	}
	logger.Info("AI_Service", "Temporary server error, applying NakWithDelay of %ds", natsErrorTimeout)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "NACKED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   identity.GetTenantId(),
		AppID:      identity.GetAppId(),
		JobID:      identity.GetJobId(),
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.NakWithDelay(time.Duration(natsErrorTimeout) * time.Second)
}

func (s *NatsServer) handleExecutionSuccess(ctx context.Context, msg jetstream.Msg, replySubj string, req *pb.ExecuteTaskRequest, result string, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	result = jsonclean.CleanJSON(result)
	logger.Debug("AI_Service", "AI Output for Job %s:\n%s", identity.GetJobId(), result)

	docUri, _ := getDocumentUri(req)
	uriStr := docUri
	if uriStr == "" {
		uriStr = "N/A"
	}
	step := getStepName(req)

	var outputMap map[string]any
	if err := json.Unmarshal([]byte(result), &outputMap); err != nil {
		if strings.HasPrefix(req.TaskSubject, "worker.ai.ocr") || strings.HasPrefix(req.TaskSubject, "worker.ai.vision") || req.TaskSubject == "worker.ai.inference" {
			outputMap = map[string]any{
				"result": result,
			}
		} else {
			errMsg := fmt.Sprintf("[Step: %s, URI: %s] AI returned invalid JSON: %v", step, uriStr, err)
			errReply := s.sendErrorReply(ctx, replySubj, errMsg, "INVALID_AI_OUTPUT")
			s.logReplyError(errReply)
			_ = msg.Term()
			return
		}
	}

	actionResultStruct, err := structpb.NewStruct(outputMap)
	if err != nil {
		errMsg := fmt.Sprintf("[Step: %s, URI: %s] Failed to convert AI output to struct: %v", step, uriStr, err)
		errReply := s.sendErrorReply(ctx, replySubj, errMsg, "STRUCT_CONVERSION_FAILED")
		s.logReplyError(errReply)
		_ = msg.Term()
		return
	}

	resp := &pb.PerformActionResponse{
		Success:      true,
		ActionResult: actionResultStruct,
	}

	if respJson, err := protojson.Marshal(resp); err == nil {
		logger.Debug("AI_Service", "[EXECUTION SUCCESS PAYLOAD] Full PerformActionResponse: %s", string(respJson))
	}

	if resp.ActionResult != nil {
		logger.Debug("Output payload", "%s: %+v", "payload", resp.ActionResult.AsMap())
	}

	respBytes, err := proto.Marshal(resp)
	if err != nil {
		logger.Info("AI_Service", "Failed to marshal response: %v", err)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "AI_Service",
			Event:      "TERMINATED",
			Stream:     "WORKERS",
			Subject:    msg.Subject(),
			Consumer:   durableName,
			TenantID:   identity.GetTenantId(),
			AppID:      identity.GetAppId(),
			JobID:      identity.GetJobId(),
			Attempt:    attempt,
			DurationMs: time.Since(startTime).Milliseconds(),
			Error:      err,
		})
		_ = msg.Term()
		return
	}

	if replySubj != "" {
		s.publishSuccessReply(ctx, msg, replySubj, respBytes, durableName, identity, attempt, startTime)
		return
	}

	s.ackMessage(msg, durableName, identity, attempt, startTime)
}

func (s *NatsServer) publishSuccessReply(ctx context.Context, msg jetstream.Msg, replySubj string, respBytes []byte, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	natsReplyDelayStr := envutil.MustGetString("NATS_REPLY_DELAY")
	natsReplyDelay, errParse := strconv.Atoi(natsReplyDelayStr)
	if errParse != nil || natsReplyDelay <= 0 {
		natsReplyDelay = 15
	}
	replyStream := envutil.MustGetString("NATS_RESPONSE_STREAM")
	_, pubErr := s.js.Publish(ctx, replySubj, respBytes)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "AI_Service",
		Event:     "ENQUEUED",
		Stream:    replyStream,
		Subject:   replySubj,
		TenantID:  identity.GetTenantId(),
		AppID:     identity.GetAppId(),
		JobID:     identity.GetJobId(),
		SizeBytes: len(respBytes),
		Error:     pubErr,
	})
	if pubErr != nil {
		logger.Info("AI_Service", "Failed to publish reply to NATS %s: %v", replySubj, pubErr)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "AI_Service",
			Event:      "NACKED",
			Stream:     "WORKERS",
			Subject:    msg.Subject(),
			Consumer:   durableName,
			TenantID:   identity.GetTenantId(),
			AppID:      identity.GetAppId(),
			JobID:      identity.GetJobId(),
			Attempt:    attempt,
			DurationMs: time.Since(startTime).Milliseconds(),
			Error:      pubErr,
		})
		_ = msg.NakWithDelay(time.Duration(natsReplyDelay) * time.Second)
		return
	}
	s.ackMessage(msg, durableName, identity, attempt, startTime)
}

func (s *NatsServer) ackMessage(msg jetstream.Msg, durableName string, identity *pb.InfrastructureIdentity, attempt uint64, startTime time.Time) {
	durationMs := time.Since(startTime).Milliseconds()
	if err := msg.Ack(); err != nil {
		logger.Info("AI_Service", "Failed to ack msg: %v", err)
		return
	}
	logger.Info("AI_Service", "Successfully completed action and replied")
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "ACKNOWLEDGED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   identity.GetTenantId(),
		AppID:      identity.GetAppId(),
		JobID:      identity.GetJobId(),
		Attempt:    attempt,
		DurationMs: durationMs,
	})
}

func (s *NatsServer) sendErrorReply(ctx context.Context, replySubject, errMsg string, opts ...string) error {
	if replySubject == "" {
		return nil
	}
	source := envutil.MustGetString("SERVICE_NAME")
	if source == "" {
		source = "ai_service"
	}
	code := "INTERNAL_ERROR"
	if len(opts) > 0 && opts[0] != "" {
		code = opts[0]
	}
	resp := &pb.PerformActionResponse{
		Success: false,
		ErrorContract: &pb.ErrorContract{
			Message: errMsg,
			Source:  source,
			Code:    code,
		},
	}
	respBytes, err := proto.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal error reply: %w", err)
	}
	if _, err := s.js.Publish(ctx, replySubject, respBytes); err != nil {
		return fmt.Errorf("failed to publish error reply to NATS: %w", err)
	}
	return nil
}

func (s *NatsServer) logReplyError(err error) {
	if err == nil {
		return
	}
	logger.Info("AI_Service", "Failed to send error reply: %v", err)
}

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func (s *NatsServer) resolveModelID(subject string) (string, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		panic("action failed: missing TaskSubject for resolveModelID")
	}

	parts := strings.Split(subject, ".")
	var trailingToken string
	for i := len(parts) - 1; i >= 0; i-- {
		token := strings.TrimSpace(parts[i])
		if token != "" {
			trailingToken = token
			break
		}
	}

	if trailingToken == "" {
		panic("action failed: missing TaskSubject trailing token for resolveModelID")
	}

	sanitized := nonAlphanumericRegex.ReplaceAllString(trailingToken, "_")
	upperToken := strings.ToUpper(sanitized)

	if upperToken == "EMBEDDING" || upperToken == "EMBED" {
		return envutil.MustGetEmbeddingModel(), nil
	}

	envKey := fmt.Sprintf("MODEL_ROUTE_%s", upperToken)
	return envutil.MustGetString(envKey), nil
}

func extractIsScannedFlag(req *pb.ExecuteTaskRequest) (bool, bool, error) {
	if req == nil || req.Payload == nil {
		panic("action failed: missing payload")
	}
	parser := workflowparser.NewTaskRequestParser(req)
	funcState, err := parser.GetFunctionalState()
	if err != nil {
		panic(fmt.Errorf("action failed: %w", err))
	}

	keys := []string{"is_scanned", "IsScanned"}
	for _, k := range keys {
		v, ok := funcState[k]
		if !ok {
			continue
		}
		if b, ok := v.(bool); ok {
			return b, true, nil
		}
		if s, ok := v.(string); ok {
			s = strings.TrimSpace(strings.ToLower(s))
			if s == "yes" || s == "true" || s == "1" {
				return true, true, nil
			} else if s == "no" || s == "false" || s == "0" {
				return false, true, nil
			}
		}
		panic("action failed: invalid is_scanned flag value")
	}
	return false, false, nil
}

func getDocumentUri(req *pb.ExecuteTaskRequest) (string, error) {
	if req == nil || req.Payload == nil {
		panic("action failed: missing payload")
	}

	parser := workflowparser.NewTaskRequestParser(req)
	executeTaskPayload := parser.MustGetExecuteTaskPayload()
	if executeTaskPayload != nil && executeTaskPayload.Storage != nil {
		return executeTaskPayload.Storage.DocumentUri, nil
	}

	return "", nil
}
