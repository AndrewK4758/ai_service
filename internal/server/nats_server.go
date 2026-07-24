package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"ai_service/internal/ai"

	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"

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
	if val := os.Getenv("MAX_ACK_PENDING"); val != "" {
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

	var cons jetstream.Consumer
	var err error
	for i := 0; i < 30; i++ {
		cons, err = s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Durable:        durableName,
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: []string{filterSubject, "ai.action.>"},
			MaxAckPending:  getMaxAckPending(s.maxAckPending),
			AckWait:        1 * time.Minute,
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

	logger.Info("AI_Service", "Starting AI consumer with MaxAckPending=%d...", s.workerPoolSize)
	sem := make(chan struct{}, s.workerPoolSize)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		select {
		case sem <- struct{}{}:
			go func(m jetstream.Msg) {
				defer func() { <-sem }()
				s.processMessage(ctx, m)
			}(msg)
		case <-ctx.Done():
			msg.Nak()
			return
		}
	}, jetstream.PullMaxMessages(s.pullMaxMessages))
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}

func (s *NatsServer) processMessage(ctx context.Context, msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err == nil && meta.NumDelivered > 5 {
		logger.Info("AI_Service", "Message exceeded max delivery attempts, terminating")
		replySubj := ""
		if headers := msg.Headers(); headers != nil {
			replySubj = headers.Get("Reply-Subject")
		}
		if err := s.sendErrorReply(ctx, replySubj, "Action failed after multiple retries due to rate limits."); err != nil {
			logger.Info("AI_Service", "Failed to send error reply: %v", err)
			msg.NakWithDelay(15 * time.Second)
			return
		}
		msg.Term()
		return
	}
	// --- START of InProgress keep-alive loop ---
	// Send InProgress() every 30 seconds (half of the 1m AckWait)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					logger.Info("AI_Service", "Failed to set msg in progress: %v", err)
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
		if err := s.sendErrorReply(ctx, replySubj, fmt.Sprintf("invalid payload: %v", err)); err != nil {
			logger.Info("AI_Service", "Failed to send error reply: %v", err)
			msg.NakWithDelay(15 * time.Second)
			return
		}
		msg.Term()
		return
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
	result, err = s.analyzer.AnalyzeText(ctx, textToAnalyze, req, nil)

	if err != nil {
		logger.Info("AI_Service", "AI analysis failed: %v", err)
		errStr := strings.ToLower(err.Error())

		var attempts uint64 = 1
		if meta != nil {
			attempts = meta.NumDelivered
		}

		isRateLimit := strings.Contains(errStr, "429") || strings.Contains(errStr, "too many requests") || strings.Contains(errStr, "throttlingexception") || strings.Contains(errStr, "quota")
		isTempError := strings.Contains(errStr, "500") || strings.Contains(errStr, "502") || strings.Contains(errStr, "503") || strings.Contains(errStr, "504") || strings.Contains(errStr, "timeout") || strings.Contains(errStr, "eof")

		if isRateLimit {
			natsRateLimitDelayStr := os.Getenv("NATS_RATE_LIMIT_DELAY")
			natsRateLimitDelay, err := strconv.Atoi(natsRateLimitDelayStr)
			if err != nil || natsRateLimitDelay <= 0 {
				natsRateLimitDelay = 5
			}
			backoffDelay := time.Duration(1<<attempts) * time.Duration(natsRateLimitDelay) * time.Second
			logger.Info("AI_Service", "Rate limited by provider, applying NakWithDelay of %v", backoffDelay)
			msg.NakWithDelay(backoffDelay)
			return
		}

		if isTempError {
			natsErrorTimeoutStr := os.Getenv("NATS_NACK_ERROR_TIMEOUT")
			natsErrorTimeout, err := strconv.Atoi(natsErrorTimeoutStr)
			if err != nil || natsErrorTimeout <= 0 {
				natsErrorTimeout = 15
			}
			logger.Info("AI_Service", "Temporary server error, applying NakWithDelay of %ds", natsErrorTimeout)
			msg.NakWithDelay(time.Duration(natsErrorTimeout) * time.Second)
			return
		}

		if err := s.sendErrorReply(ctx, replySubj, fmt.Sprintf("failed to perform action on payload: %v", err)); err != nil {
			logger.Info("AI_Service", "Failed to send error reply: %v", err)
			msg.NakWithDelay(15 * time.Second)
			return
		}
		msg.Term()
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
		msg.Term()
		return
	}

	if replySubj != "" {
		natsReplyDelayStr := os.Getenv("NATS_REPLY_DELAY")
		natsReplyDelay, err := strconv.Atoi(natsReplyDelayStr)
		if err != nil || natsReplyDelay <= 0 {
			natsReplyDelay = 15
		}
		if _, err := s.js.Publish(ctx, replySubj, respBytes); err != nil {
			logger.Info("AI_Service", "Failed to publish reply to NATS %s: %v", replySubj, err)
			msg.NakWithDelay(time.Duration(natsReplyDelay) * time.Second) // Retry if NATS publish fails
			return
		}
	}

	if err := msg.Ack(); err != nil {
		logger.Info("AI_Service", "Failed to ack msg: %v", err)
	} else {
		logger.Info("AI_Service", "Successfully completed action and replied to %s", replySubj)
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
