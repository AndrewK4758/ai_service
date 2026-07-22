package server

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"ai_service/internal/ai"

	"github.com/AndrewK4758/shared_utils/jsonclean"

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
	streamName := "AI_ACTIONS"
	_, err := s.js.Stream(ctx, streamName)
	if err != nil {
		_, err = s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{"ai.action.>"},
		})
		if err != nil {
			return fmt.Errorf("failed to create JetStream stream: %w", err)
		}
		log.Printf("Successfully created JetStream stream: %s", streamName)
	}

	durableName := fmt.Sprintf("ai_service_workers_%s", s.queueFilter)
	filterSubject := fmt.Sprintf("ai.action.%s", s.queueFilter)
	if s.queueFilter == ">" {
		durableName = "ai_service_workers_all"
		filterSubject = "ai.action.>"
	}

	cons, err := s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       durableName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: filterSubject,
		MaxAckPending: s.maxAckPending,
		AckWait:       1 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("failed to create JetStream consumer: %w", err)
	}

	log.Printf("Starting AI consumer with MaxAckPending=%d...", s.workerPoolSize)
	sem := make(chan struct{}, s.workerPoolSize)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		sem <- struct{}{}
		go func(m jetstream.Msg) {
			defer func() { <-sem }()
			s.processMessage(ctx, m)
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
	meta, err := msg.Metadata()
	if err == nil && meta.NumDelivered > 5 {
		log.Printf("Message exceeded max delivery attempts, terminating")
		msg.Term()

		replySubj := ""
		if headers := msg.Headers(); headers != nil {
			replySubj = headers.Get("Reply-Subject")
		}
		s.sendErrorReply(ctx, replySubj, "Action failed after multiple retries due to rate limits.")
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
					log.Printf("Failed to set msg in progress: %v", err)
				}
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	// --- END of InProgress keep-alive loop ---

	log.Printf("Received action request on subject: %s", msg.Subject())

	var replySubj string
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}

	req := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		log.Printf("Error unmarshaling request: %v", err)
		msg.Term()
		s.sendErrorReply(ctx, replySubj, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	var result string
	// Generic AI Worker acts strictly on the provided ExecuteWorkflowNodeRequest
	// The client's global_state contains the context needed for analysis.
	result, err = s.analyzer.AnalyzeText(ctx, "", req, nil)

	if err != nil {
		log.Printf("AI analysis failed: %v", err)
		errStr := strings.ToLower(err.Error())

		var attempts uint64 = 1
		if meta != nil {
			attempts = meta.NumDelivered
		}

		isRateLimit := strings.Contains(errStr, "429") || strings.Contains(errStr, "too many requests") || strings.Contains(errStr, "throttlingexception") || strings.Contains(errStr, "quota")
		isTempError := strings.Contains(errStr, "500") || strings.Contains(errStr, "502") || strings.Contains(errStr, "503") || strings.Contains(errStr, "504") || strings.Contains(errStr, "timeout") || strings.Contains(errStr, "eof")

		if isRateLimit {
			backoffDelay := time.Duration(1<<attempts) * 5 * time.Second
			log.Printf("Rate limited by provider, applying NakWithDelay of %v", backoffDelay)
			msg.NakWithDelay(backoffDelay)
			return
		}

		if isTempError {
			log.Printf("Temporary server error, applying NakWithDelay of 15s")
			msg.NakWithDelay(15 * time.Second)
			return
		}

		msg.Term()
		s.sendErrorReply(ctx, replySubj, fmt.Sprintf("failed to perform action on payload: %v", err))
		return
	}

	result = jsonclean.CleanJSON(result)

	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResultJson: result,
	}

	respBytes, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		msg.Term()
		return
	}

	if replySubj != "" {
		if _, err := s.js.Publish(ctx, replySubj, respBytes); err != nil {
			log.Printf("Failed to publish reply to NATS %s: %v", replySubj, err)
			msg.NakWithDelay(5 * time.Second) // Retry if NATS publish fails
			return
		}
	}

	if err := msg.Ack(); err != nil {
		log.Printf("Failed to ack msg: %v", err)
	} else {
		log.Printf("Successfully completed action and replied to %s", replySubj)
	}
}

func (s *NatsServer) sendErrorReply(ctx context.Context, replySubject, errMsg string) {
	if replySubject == "" {
		return
	}
	resp := &pb.PerformActionResponse{
		Success:      false,
		ErrorMessage: errMsg,
	}
	respBytes, _ := proto.Marshal(resp)
	_, _ = s.js.Publish(ctx, replySubject, respBytes)
}
