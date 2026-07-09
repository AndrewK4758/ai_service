package server

import (
	"context"
	"fmt"
	"log"

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
}

func NewNatsServer(nc *nats.Conn, js jetstream.JetStream, analyzer ai.Analyzer, workerPoolSize int) *NatsServer {
	return &NatsServer{
		nc:             nc,
		js:             js,
		analyzer:       analyzer,
		workerPoolSize: workerPoolSize,
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

	cons, err := s.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "ai_service_workers",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "ai.action.>",
		MaxAckPending: s.workerPoolSize,
	})
	if err != nil {
		return fmt.Errorf("failed to create JetStream consumer: %w", err)
	}

	log.Printf("Starting AI consumer with MaxAckPending=%d...", s.workerPoolSize)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		s.processMessage(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}

func (s *NatsServer) processMessage(ctx context.Context, msg jetstream.Msg) {
	// Acknowledge the message so it doesn't redeliver unless we explicitly fail to ack
	msg.Ack()

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
