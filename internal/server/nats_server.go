package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"ai_service/internal/ai"
	"ai_service/internal/parser"
	pb "doc_processor/shared_protos"
)

type NatsServer struct {
	nc             *nats.Conn
	js             nats.JetStreamContext
	analyzer       ai.Analyzer
	workerPoolSize int
}

func NewNatsServer(nc *nats.Conn, js nats.JetStreamContext, analyzer ai.Analyzer, workerPoolSize int) *NatsServer {
	return &NatsServer{
		nc:             nc,
		js:             js,
		analyzer:       analyzer,
		workerPoolSize: workerPoolSize,
	}
}

func (s *NatsServer) Start(ctx context.Context) error {
	// Create the JetStream stream for AI actions if it doesn't exist
	streamName := "AI_ACTIONS"
	_, err := s.js.StreamInfo(streamName)
	if err != nil {
		_, err = s.js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{"ai.action.>"},
		})
		if err != nil {
			return fmt.Errorf("failed to create JetStream stream: %w", err)
		}
		log.Printf("Successfully created JetStream stream: %s", streamName)
	}

	// Create a channel for incoming messages to distribute across the worker pool
	msgChan := make(chan *nats.Msg, s.workerPoolSize*2)

	// Subscribe to the stream using a Queue Group to allow horizontal scaling of ai_service instances
	sub, err := s.js.QueueSubscribeSync("ai.action.>", "ai_workers", nats.BindStream(streamName))
	if err != nil {
		return fmt.Errorf("failed to subscribe to ai.action.>: %w", err)
	}
	defer sub.Unsubscribe()

	log.Printf("Starting %d AI workers...", s.workerPoolSize)
	var wg sync.WaitGroup
	for i := 0; i < s.workerPoolSize; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.workerLoop(ctx, workerID, msgChan)
		}(i)
	}

	// Message pulling loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(msgChan)
				return
			default:
				msg, err := sub.NextMsg(1 * time.Second)
				if err != nil {
					if err == nats.ErrTimeout {
						continue
					}
					log.Printf("Error pulling message from NATS: %v", err)
					continue
				}
				msgChan <- msg
			}
		}
	}()

	wg.Wait()
	return nil
}

func (s *NatsServer) workerLoop(ctx context.Context, workerID int, msgChan <-chan *nats.Msg) {
	log.Printf("Worker %d ready", workerID)
	for msg := range msgChan {
		s.processMessage(ctx, workerID, msg)
	}
}

func (s *NatsServer) processMessage(ctx context.Context, workerID int, msg *nats.Msg) {
	// Acknowledge the message so it doesn't redeliver unless we explicitly fail to ack
	msg.Ack()

	log.Printf("[Worker %d] Received action request on subject: %s", workerID, msg.Subject)

	req := &pb.PerformActionRequest{}
	if err := proto.Unmarshal(msg.Data, req); err != nil {
		log.Printf("[Worker %d] Error unmarshaling request: %v", workerID, err)
		s.sendErrorReply(msg.Reply, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	opts := ai.AnalyzeOptions{
		CustomInstructions: req.GetPrompt(),
		JsonSchema:         req.GetJsonSchema(),
	}

	var text string
	var err error

	if req.GetInputText() != "" && req.GetInputText() != " " {
		text = req.GetInputText()
	} else if len(req.GetInputBytes()) > 0 {
		text, err = parser.ExtractPdfText(req.GetInputBytes())
		if err != nil {
			log.Printf("[Worker %d] Error extracting text from binary: %v", workerID, err)
			s.sendErrorReply(msg.Reply, fmt.Sprintf("failed to extract text from binary input: %v", err))
			return
		}
	} else if req.GetInputText() == " " {
		// Orchestrator uses a single space to bypass extraction logic and just process a pure prompt
		text = " "
	}

	result, err := s.analyzer.AnalyzeText(ctx, text, opts)
	if err != nil {
		log.Printf("[Worker %d] AI analysis failed: %v", workerID, err)
		s.sendErrorReply(msg.Reply, fmt.Sprintf("failed to perform action on text: %v", err))
		return
	}

	// Prepare success response
	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResultJson: result,
	}

	respBytes, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("[Worker %d] Failed to marshal response: %v", workerID, err)
		return
	}

	// Publish response to the dynamically requested Reply subject (Core NATS)
	if msg.Reply != "" {
		if err := s.nc.Publish(msg.Reply, respBytes); err != nil {
			log.Printf("[Worker %d] Failed to publish reply to %s: %v", workerID, msg.Reply, err)
		}
	}
	log.Printf("[Worker %d] Successfully completed action and replied to %s", workerID, msg.Reply)
}

func (s *NatsServer) sendErrorReply(replySubject, errMsg string) {
	if replySubject == "" {
		return
	}
	resp := &pb.PerformActionResponse{
		Success:      false,
		ErrorMessage: errMsg,
	}
	respBytes, _ := proto.Marshal(resp)
	_ = s.nc.Publish(replySubject, respBytes)
}
