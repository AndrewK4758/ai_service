package server

import (
	"context"
	"encoding/json"
	"fmt"
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

	"github.com/AndrewK4758/shared_utils/workflowparser"
	"github.com/AndrewK4758/shared_utils/identityparser"
	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"

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
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream, analyzer ai.Analyzer, store storage.Manager, workerPoolSize int, queueFilter string, maxAckPending int, pullMaxMessages int, maxDocumentTokens int) *NatsServer {
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
	}
}

func (s *NatsServer) Start(ctx context.Context) error {
	streamName := "WORKERS"

	durableName := fmt.Sprintf("ai_service_workers_%s", s.queueFilter)
	filterSubject := fmt.Sprintf("worker.ai.%s", s.queueFilter)
	if s.queueFilter == ">" {
		durableName = "ai_service_workers_all"
		filterSubject = "worker.ai.>"
	}

	ackWaitStr := envutil.MustGetString("WORKER_ACK_WAIT")
	ackWait, err := time.ParseDuration(ackWaitStr)
	if err != nil {
		logger.Warn("AI_Service", "Invalid WORKER_ACK_WAIT, defaulting to 1m: %v", err)
		ackWait = 1 * time.Minute
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
			FilterSubjects: []string{filterSubject, "ai.action.>"},
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
		return fmt.Errorf("failed to create JetStream consumer after retries: %w", err)
	}

	logger.Info("AI_Service", "Starting AI consumer with WorkerPoolSize=%d, MaxAckPending=%d...", s.workerPoolSize, maxAck)
	sem := make(chan struct{}, s.workerPoolSize)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		go func(m jetstream.Msg) {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				s.processMessage(ctx, m)
			case <-ctx.Done():
				_ = m.Nak()
			}
		}(msg)
	}, jetstream.PullMaxMessages(s.pullMaxMessages))
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
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

	durableName := fmt.Sprintf("ai_service_workers_%s", s.queueFilter)
	if s.queueFilter == ">" {
		durableName = "ai_service_workers_all"
	}

	t, a, j := natsutil.ExtractIdentityFromAiSubject(msg.Subject())

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
		s.handleExceededRetries(ctx, msg, durableName, t, a, j, attempt, startTime)
		return
	}

	if ctx.Err() != nil {
		s.handleContextCanceled(msg, durableName, t, a, j, attempt, startTime)
		return
	}

	// --- START of InProgress keep-alive loop ---
	ackWaitStr := envutil.MustGetString("WORKER_ACK_WAIT")
	ackWait, err := time.ParseDuration(ackWaitStr)
	if err != nil {
		ackWait = 1 * time.Minute
	}
	keepAliveInterval := ackWait / 2

	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	done := make(chan struct{})
	defer close(done)

	go s.runKeepAlive(ctx, msg, ticker, done, durableName, t, a, j, attempt)
	// --- END of InProgress keep-alive loop ---

	logger.Info("AI_Service", "Received action request on subject: %s", msg.Subject())
	replySubj := getReplySubject(msg)

	req := &pb.ExecuteTaskRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		panic(fmt.Errorf("action failed: unmarshal error: %v", err))
	}

	if reqJson, err := protojson.Marshal(req); err == nil {
		logger.Info("AI_Service", "[DEQUEUED PAYLOAD] Full ExecuteTaskRequest: %s", string(reqJson))
	}

	if req.Payload != nil {
		logger.Info("Input payload", "%s: %+v", "payload", req.Payload.AsMap())
	}

	// Fail-Fast Assertion 1: Infrastructure Identity Validation
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		panic(fmt.Errorf("action failed: invalid identity - %v", err))
	}

	t = req.GetIdentity().GetTenantId()
	a = req.GetIdentity().GetAppId()
	j = req.GetIdentity().GetJobId()

	// Fail-Fast Assertion 2: Required TaskSubject Validation
	if req.GetTaskSubject() == "" {
		panic(fmt.Errorf("action failed: missing required TaskSubject"))
	}

	targetSubj := req.GetTaskSubject()

	isScanned, hasFlag := extractIsScannedFlag(req)
	var modelID string
	if hasFlag {
		if isScanned {
			modelID = envutil.MustGetString("OPENAI_VISION_MODEL")
		} else {
			modelID = envutil.MustGetString("OPENAI_TEXT_MODEL")
		}
	} else {
		modelID = s.resolveModelID(targetSubj)
	}

	result, errCode, err := s.executeAction(ctx, req, targetSubj, modelID)
	if err != nil {
		s.handleExecutionError(ctx, msg, replySubj, req, errCode, err, durableName, t, a, j, attempt, startTime)
		return
	}

	s.handleExecutionSuccess(ctx, msg, replySubj, req, result, durableName, t, a, j, attempt, startTime)
}

func (s *NatsServer) handleExceededRetries(ctx context.Context, msg jetstream.Msg, durableName, t, a, j string, attempt uint64, startTime time.Time) {
	logger.Info("AI_Service", "Message exceeded max delivery attempts, terminating")
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "TERMINATED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   t,
		AppID:      a,
		JobID:      j,
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

func (s *NatsServer) handleContextCanceled(msg jetstream.Msg, durableName, t, a, j string, attempt uint64, startTime time.Time) {
	logger.Info("AI_Service", "Context canceled, NAKing message")
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "AI_Service",
		Event:      "NACKED",
		Stream:     "WORKERS",
		Subject:    msg.Subject(),
		Consumer:   durableName,
		TenantID:   t,
		AppID:      a,
		JobID:      j,
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
	})
	_ = msg.Nak()
}

func (s *NatsServer) runKeepAlive(ctx context.Context, msg jetstream.Msg, ticker *time.Ticker, done chan struct{}, durableName, t, a, j string, attempt uint64) {
	for {
		select {
		case <-ticker.C:
			s.sendKeepAlive(msg, durableName, t, a, j, attempt)
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *NatsServer) sendKeepAlive(msg jetstream.Msg, durableName, t, a, j string, attempt uint64) {
	if err := msg.InProgress(); err != nil {
		return
	}
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "AI_Service",
		Event:     "IN_PROGRESS",
		Stream:    "WORKERS",
		Subject:   msg.Subject(),
		Consumer:  durableName,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		Attempt:   attempt,
	})
}

func (s *NatsServer) handleUnmarshalError(ctx context.Context, msg jetstream.Msg, replySubj string, err error, durableName, t, a, j string, attempt uint64, startTime time.Time) {
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
		TenantID:   t,
		AppID:      a,
		JobID:      j,
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.Term()
}

func (s *NatsServer) executeAction(ctx context.Context, req *pb.ExecuteTaskRequest, targetSubj string, modelID string) (string, string, error) {
	docUri := getDocumentUri(req)
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

	fileBytes, err := s.store.FetchFile(ctx, docUri, req.Identity)
	if err != nil {
		return "", "STORAGE_FETCH_FAILED", fmt.Errorf("[Step: %s, URI: %s] failed to fetch document from storage: %w", step, docUri, err)
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

	isScanned, hasFlag := extractIsScannedFlag(req)
	if hasFlag && ext == ".pdf" {
		if isScanned {
			return s.processPdfVisionBytes(ctx, fileBytes, step, docUri, req, modelID)
		}
		return s.processPdfTextBytes(ctx, fileBytes, step, docUri, req, modelID)
	}

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
	if strings.Contains(subjLower, "vision") || strings.Contains(subjLower, "image") || strings.Contains(subjLower, "ocr") {
		return true
	}
	modelLower := strings.ToLower(modelID)
	if strings.Contains(modelLower, "vision") || strings.Contains(modelLower, "gpt-4o") || strings.Contains(modelLower, "claude-3") {
		return true
	}
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" || ext == ".webp" || ext == ".tiff" || ext == ".bmp" {
		return true
	}
	return false
}

func (s *NatsServer) handleExecutionError(ctx context.Context, msg jetstream.Msg, replySubj string, req *pb.ExecuteTaskRequest, errCode string, err error, durableName, t, a, j string, attempt uint64, startTime time.Time) {
	if ctx.Err() != nil {
		s.handleContextCanceled(msg, durableName, t, a, j, attempt, startTime)
		return
	}
	logger.Info("AI_Service", "AI processing failed: %v", err)
	errStr := strings.ToLower(err.Error())

	if errCode != "LARGE_DOCUMENT_EXCEEDED" {
		isRateLimit := strings.Contains(errStr, "429") || strings.Contains(errStr, "too many requests") || strings.Contains(errStr, "throttlingexception") || strings.Contains(errStr, "quota")
		if isRateLimit {
			s.handleRateLimitError(msg, err, durableName, t, a, j, attempt, startTime)
			return
		}

		isTempError := strings.Contains(errStr, "500") || strings.Contains(errStr, "502") || strings.Contains(errStr, "503") || strings.Contains(errStr, "504") || strings.Contains(errStr, "timeout") || strings.Contains(errStr, "eof")
		if isTempError {
			s.handleTempError(msg, err, durableName, t, a, j, attempt, startTime)
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
		TenantID:   t,
		AppID:      a,
		JobID:      j,
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.Term()
}

func (s *NatsServer) handleRateLimitError(msg jetstream.Msg, err error, durableName, t, a, j string, attempt uint64, startTime time.Time) {
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
		TenantID:   t,
		AppID:      a,
		JobID:      j,
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.NakWithDelay(backoffDelay)
}

func (s *NatsServer) handleTempError(msg jetstream.Msg, err error, durableName, t, a, j string, attempt uint64, startTime time.Time) {
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
		TenantID:   t,
		AppID:      a,
		JobID:      j,
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
		Error:      err,
	})
	_ = msg.NakWithDelay(time.Duration(natsErrorTimeout) * time.Second)
}

func (s *NatsServer) handleExecutionSuccess(ctx context.Context, msg jetstream.Msg, replySubj string, req *pb.ExecuteTaskRequest, result string, durableName, t, a, j string, attempt uint64, startTime time.Time) {
	result = jsonclean.CleanJSON(result)
	logger.Info("AI_Service", "AI Output for Job %s:\n%s", j, result)

	docUri := getDocumentUri(req)
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
		logger.Info("AI_Service", "[EXECUTION SUCCESS PAYLOAD] Full PerformActionResponse: %s", string(respJson))
	}

	if resp.ActionResult != nil {
		logger.Info("Output payload", "%s: %+v", "payload", resp.ActionResult.AsMap())
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
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: time.Since(startTime).Milliseconds(),
			Error:      err,
		})
		_ = msg.Term()
		return
	}

	if replySubj != "" {
		s.publishSuccessReply(ctx, msg, replySubj, respBytes, durableName, t, a, j, attempt, startTime)
		return
	}

	s.ackMessage(msg, durableName, t, a, j, attempt, startTime)
}

func (s *NatsServer) publishSuccessReply(ctx context.Context, msg jetstream.Msg, replySubj string, respBytes []byte, durableName, t, a, j string, attempt uint64, startTime time.Time) {
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
		TenantID:  t,
		AppID:     a,
		JobID:     j,
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
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: time.Since(startTime).Milliseconds(),
			Error:      pubErr,
		})
		_ = msg.NakWithDelay(time.Duration(natsReplyDelay) * time.Second)
		return
	}
	s.ackMessage(msg, durableName, t, a, j, attempt, startTime)
}

func (s *NatsServer) ackMessage(msg jetstream.Msg, durableName, t, a, j string, attempt uint64, startTime time.Time) {
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
		TenantID:   t,
		AppID:      a,
		JobID:      j,
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

func (s *NatsServer) resolveModelID(subject string) string {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		panic(fmt.Errorf("action failed: missing TaskSubject for resolveModelID"))
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
		panic(fmt.Errorf("action failed: missing TaskSubject trailing token for resolveModelID"))
	}

	sanitized := nonAlphanumericRegex.ReplaceAllString(trailingToken, "_")
	upperToken := strings.ToUpper(sanitized)

	if upperToken == "EMBEDDING" || upperToken == "EMBED" {
		return envutil.MustGetEmbeddingModel()
	}

	envKey := fmt.Sprintf("MODEL_ROUTE_%s", upperToken)
	return envutil.MustGetString(envKey)
}

func extractIsScannedFlag(req *pb.ExecuteTaskRequest) (bool, bool) {
	if req == nil || req.Payload == nil {
		panic(fmt.Errorf("action failed: missing payload"))
	}
	parser := workflowparser.NewTaskRequestParser(req)
	funcState, err := parser.GetFunctionalState()
	if err != nil {
		panic(fmt.Errorf("action failed: %v", err))
	}
	// 1. Check node_config first
	if req.Payload != nil {
		payloadMap := req.Payload.AsMap()
		if nodeConfigRaw, ok := payloadMap["node_config"]; ok {
			if nodeConfig, ok := nodeConfigRaw.(map[string]any); ok {
				keys := []string{"is_scanned", "IsScanned"}
				for _, k := range keys {
					if v, ok := nodeConfig[k]; ok {
						if b, ok := v.(bool); ok {
							return b, true
						}
						if s, ok := v.(string); ok {
							s = strings.TrimSpace(strings.ToLower(s))
							if s == "yes" || s == "true" || s == "1" {
								return true, true
							} else if s == "no" || s == "false" || s == "0" {
								return false, true
							}
						}
					}
				}
			}
		}
	}

	// 2. Check global_state.functional
	keys := []string{"is_scanned", "IsScanned"}
	for _, k := range keys {
		v, ok := funcState[k]
		if !ok {
			continue
		}
		if b, ok := v.(bool); ok {
			return b, true
		}
		if s, ok := v.(string); ok {
			s = strings.TrimSpace(strings.ToLower(s))
			if s == "yes" || s == "true" || s == "1" {
				return true, true
			}
			if s == "no" || s == "false" || s == "0" {
				return false, true
			}
			if parsed, err := strconv.ParseBool(s); err == nil {
				return parsed, true
			}
			panic(fmt.Errorf("action failed: invalid is_scanned flag value"))
		}
		panic(fmt.Errorf("action failed: invalid is_scanned flag type"))
	}
	return false, false
}

func getDocumentUri(req *pb.ExecuteTaskRequest) string {
	if req == nil || req.Payload == nil {
		panic(fmt.Errorf("action failed: missing payload"))
	}
	parser := workflowparser.NewTaskRequestParser(req)
	funcState, err := parser.GetFunctionalState()
	if err != nil {
		panic(fmt.Errorf("action failed: %v", err))
	}
	
	keys := []string{"document_uri", "DocumentUri"}
	for _, k := range keys {
		if v, ok := funcState[k].(string); ok {
			return v
		}
	}
	return ""
}
