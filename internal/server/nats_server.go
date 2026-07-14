package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"ai_service/internal/ai"
	"ai_service/internal/parser"
	pb "github.com/AndrewK4758/shared_protos"
)

type NatsServer struct {
	nc             *nats.Conn
	js             jetstream.JetStream
	analyzer       ai.Analyzer
	workerPoolSize int
	queueFilter    string
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream, analyzer ai.Analyzer, workerPoolSize int, queueFilter string) *NatsServer {
	return &NatsServer{
		nc:             nc,
		js:             js,
		analyzer:       analyzer,
		workerPoolSize: workerPoolSize,
		queueFilter:    queueFilter,
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
	filterSubject := fmt.Sprintf("ai.action.%s.>", s.queueFilter)
	if s.queueFilter == ">" {
		durableName = "ai_service_workers_all"
		filterSubject = "ai.action.>"
	}

	cons, err := s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       durableName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: filterSubject,
		MaxAckPending: s.workerPoolSize,
		AckWait:       10 * time.Minute,
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
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}

func (s *NatsServer) processMessage(ctx context.Context, msg jetstream.Msg) {
	// Acknowledge the message so it doesn't redeliver
	defer func() {
		if err := msg.Ack(); err != nil {
			log.Printf("Failed to ack msg: %v", err)
		}
	}()

	log.Printf("Received action request on subject: %s", msg.Subject())

	var replySubj string
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}

	req := &pb.PerformActionRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		log.Printf("Error unmarshaling request: %v", err)
		s.sendErrorReply(ctx, replySubj, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	var text string
	var err error

	if req.GetInputText() != "" && req.GetInputText() != " " {
		text = req.GetInputText()
	} else if len(req.GetInputBytes()) > 0 {
		text, err = parser.ExtractPdfText(req.GetInputBytes())
		if err != nil {
			log.Printf("Error extracting text from binary: %v", err)
			s.sendErrorReply(ctx, replySubj, fmt.Sprintf("failed to extract text from binary input: %v", err))
			return
		}
	} else if req.GetInputText() == " " {
		text = " "
	}

	result, err := s.analyzer.AnalyzeText(ctx, text, req.GetContext(), req.GetCurrentAction())
	if err != nil {
		log.Printf("AI analysis failed: %v", err)
		s.sendErrorReply(ctx, replySubj, fmt.Sprintf("failed to perform action on text: %v", err))
		return
	}

	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResultJson: result,
	}

	respBytes, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}

	if replySubj != "" {
		if _, err := s.js.Publish(ctx, replySubj, respBytes); err != nil {
			log.Printf("Failed to publish reply to NATS %s: %v", replySubj, err)
		}
	}
	log.Printf("Successfully completed action and replied to %s", replySubj)
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
