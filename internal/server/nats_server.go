package server

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AndrewK4758/shared_utils/envutil"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"ai_service/internal/ai"

	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"

	pb "github.com/AndrewK4758/shared_protos"
)

type NatsServer struct {
	nc              *nats.Conn
	js              jetstream.JetStream
	analyzer        ai.Analyzer
	workerPoolSize  int
	queueFilter     string
	maxAckPending   int
	pullMaxMessages int
}

func getMaxAckPending(defaultMax int) int {
	if val := envutil.GetStringOrDefault("MAX_ACK_PENDING", ""); val != "" {
		if max, err := strconv.Atoi(val); err == nil && max > 0 {
			return max
		}
	}
	if val := envutil.GetStringOrDefault("NATS_MAX_ACK_PENDING", ""); val != "" {
		if max, err := strconv.Atoi(val); err == nil && max > 0 {
			return max
		}
	}
	return defaultMax
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream, analyzer ai.Analyzer, workerPoolSize int, queueFilter string, maxAckPending int, pullMaxMessages int) *NatsServer {
	return &NatsServer{
		nc:              nc,
		js:              js,
		analyzer:        analyzer,
		workerPoolSize:  workerPoolSize,
		queueFilter:     queueFilter,
		maxAckPending:   maxAckPending,
		pullMaxMessages: pullMaxMessages,
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

	ackWaitStr := envutil.GetStringOrDefault("WORKER_ACK_WAIT", "1m")
	ackWait, err := time.ParseDuration(ackWaitStr)
	if err != nil {
		logger.Warn("AI_Service", "Invalid WORKER_ACK_WAIT, defaulting to 1m: %v", err)
		ackWait = 1 * time.Minute
	}

	maxAck := getMaxAckPending(s.maxAckPending)
	if maxAck <= 0 {
		maxAck = s.workerPoolSize
	}

	maxDeliver := 5
	if envVal := envutil.GetStringOrDefault("NATS_MAX_DELIVER", ""); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			maxDeliver = parsed
		} else if err != nil {
			return fmt.Errorf("failed to parse NATS_MAX_DELIVER: %w", err)
		}
	}

	var cons jetstream.Consumer
	for i := 0; i < 90; i++ {
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
		time.Sleep(2 * time.Second)
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
		replySubj := ""
		if headers := msg.Headers(); headers != nil {
			replySubj = headers.Get("Reply-Subject")
		}
		if sendErr := s.sendErrorReply(ctx, replySubj, "Action failed after multiple retries due to rate limits."); sendErr != nil {
			logger.Info("AI_Service", "Failed to send error reply: %v", sendErr)
		}
		_ = msg.Term()
		return
	}

	if ctx.Err() != nil {
		logger.Info("AI_Service", "Context canceled, NAKing message: %v", ctx.Err())
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
			Error:      ctx.Err(),
		})
		_ = msg.Nak()
		return
	}

	// --- START of InProgress keep-alive loop ---
	ackWaitStr := envutil.GetStringOrDefault("WORKER_ACK_WAIT", "1m")
	ackWait, err := time.ParseDuration(ackWaitStr)
	if err != nil {
		ackWait = 1 * time.Minute
	}
	keepAliveInterval := ackWait / 2

	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case <-ticker.C:
				if err := msg.InProgress(); err == nil {
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
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	// --- END of InProgress keep-alive loop ---

	logger.Info("AI_Service", "Received action request on subject: %s", msg.Subject())

	var replySubj string
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}

	req := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		logger.Info("AI_Service", "Error unmarshaling request: %v", err)
		if sendErr := s.sendErrorReply(ctx, replySubj, fmt.Sprintf("invalid payload: %v", err)); sendErr != nil {
			logger.Info("AI_Service", "Failed to send error reply: %v", sendErr)
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
		return
	}

	if req.Identity != nil {
		t, a, j = natsutil.ExtractIdentity(req.Identity)
	}

	var result string
	var textToAnalyze string
	if req.GetGlobalState() != nil {
		stateBytes, err := json.Marshal(req.GetGlobalState().AsMap())
		if err != nil {
			logger.Info("AI_Service", "Failed to marshal global state: %v", err)
		} else {
			textToAnalyze = string(stateBytes)
		}
	}

	modelID := s.resolveModelID(msg.Subject())
	result, err = s.analyzer.AnalyzeText(ctx, textToAnalyze, req, nil, modelID)

	if err != nil {
		if ctx.Err() != nil {
			logger.Info("AI_Service", "Context canceled during AI analysis, NAKing message: %v", ctx.Err())
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
				Error:      ctx.Err(),
			})
			_ = msg.Nak()
			return
		}
		logger.Info("AI_Service", "AI analysis failed: %v", err)
		errStr := strings.ToLower(err.Error())

		isRateLimit := strings.Contains(errStr, "429") || strings.Contains(errStr, "too many requests") || strings.Contains(errStr, "throttlingexception") || strings.Contains(errStr, "quota")
		isTempError := strings.Contains(errStr, "500") || strings.Contains(errStr, "502") || strings.Contains(errStr, "503") || strings.Contains(errStr, "504") || strings.Contains(errStr, "timeout") || strings.Contains(errStr, "eof")

		if isRateLimit {
			natsRateLimitDelayStr := envutil.GetStringOrDefault("NATS_RATE_LIMIT_DELAY", "")
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
			return
		}

		if isTempError {
			natsErrorTimeoutStr := envutil.GetStringOrDefault("NATS_NACK_ERROR_TIMEOUT", "")
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
			return
		}

		if sendErr := s.sendErrorReply(ctx, replySubj, fmt.Sprintf("failed to perform action on payload: %v", err)); sendErr != nil {
			logger.Info("AI_Service", "Failed to send error reply: %v", sendErr)
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
		return
	}

	result = jsonclean.CleanJSON(result)

	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResultJson: result,
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
		natsReplyDelayStr := envutil.GetStringOrDefault("NATS_REPLY_DELAY", "")
		natsReplyDelay, errParse := strconv.Atoi(natsReplyDelayStr)
		if errParse != nil || natsReplyDelay <= 0 {
			natsReplyDelay = 15
		}
		replyStream := "AI_RESPONSES"
		if strings.HasPrefix(replySubj, "orchestrator.response") {
			replyStream = "ORCHESTRATOR_RESPONSES"
		}
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
			msg.NakWithDelay(time.Duration(natsReplyDelay) * time.Second)
			return
		}
	}

	durationMs := time.Since(startTime).Milliseconds()
	if err := msg.Ack(); err != nil {
		logger.Info("AI_Service", "Failed to ack msg: %v", err)
	} else {
		logger.Info("AI_Service", "Successfully completed action and replied to %s", replySubj)
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
}

func (s *NatsServer) sendErrorReply(ctx context.Context, replySubject, errMsg string) error {
	if replySubject == "" {
		return nil
	}
	resp := &pb.PerformActionResponse{
		Success: false,
		ErrorContract: &pb.ErrorContract{
			Message: errMsg,
			Source:  "ai_service",
			Code:    "INTERNAL_ERROR",
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

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func (s *NatsServer) resolveModelID(subject string) string {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return envutil.GetStringOrDefault("MODEL_ROUTE_DEFAULT", "")
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

	if trailingToken != "" {
		sanitized := nonAlphanumericRegex.ReplaceAllString(trailingToken, "_")
		upperToken := strings.ToUpper(sanitized)
		envKey := fmt.Sprintf("MODEL_ROUTE_%s", upperToken)
		if val := envutil.GetStringOrDefault(envKey, ""); val != "" {
			return val
		}
		if upperToken == "EMBEDDING" || upperToken == "EMBED" {
			if val := envutil.GetEmbeddingModel(""); val != "" {
				return val
			}
		}
		envKeyDirect := fmt.Sprintf("MODEL_%s", upperToken)
		if val := envutil.GetStringOrDefault(envKeyDirect, ""); val != "" {
			return val
		}
	}

	if val := envutil.GetStringOrDefault("MODEL_ROUTE_DEFAULT", ""); val != "" {
		return val
	}

	return ""
}
