package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/logger"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		logger.Fatal("AI_Service", "%v", "Usage: go run main.go <path-to-document-or-directory>")
	}
	inputPath := os.Args[1]

	// Determine if input is a single file or a directory
	info, err := os.Stat(inputPath)
	if err != nil {
		logger.Fatal("AI_Service", "Failed to stat input path: %v", err)
	}

	var filesToProcess []string
	if info.IsDir() {
		entries, err := os.ReadDir(inputPath)
		if err != nil {
			logger.Fatal("AI_Service", "Failed to read directory: %v", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				filesToProcess = append(filesToProcess, filepath.Join(inputPath, entry.Name()))
			}
		}
	} else {
		filesToProcess = append(filesToProcess, inputPath)
	}

	if len(filesToProcess) == 0 {
		logger.Fatal("AI_Service", "%v", "No files found to process")
	}

	fmt.Printf("Found %d file(s) to process.\n", len(filesToProcess))

	// Connect to our gRPC Server
	conn, err := grpc.NewClient(envutil.MustGetString("SERVER_PORT"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("AI_Service", "did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("Failed to close connection: ", err)
		}
	}(conn)
	client := pb.NewDocumentProcessorClient(conn)

	// Connect to NATS JetStream to listen for the async responses
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		logger.Fatal("AI_Service", "failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Fatal("AI_Service", "failed to init JetStream: %v", err)
	}

	appID := "test_client_app"
	subject := fmt.Sprintf("ai.processed.%s", appID)

	ctxJS := context.Background()
	streamName := "AI_PROCESSED"
	_, _ = js.CreateStream(ctxJS, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"ai.processed.>"},
	})

	cons, err := js.CreateOrUpdateConsumer(ctxJS, streamName, jetstream.ConsumerConfig{
		Durable:       "test_client_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		logger.Fatal("AI_Service", "failed to create JetStream consumer: %v", err)
	}

	// Setup a channel to block until we get all responses
	done := make(chan []byte, len(filesToProcess))

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		err := msg.Ack()
		if err != nil {
			fmt.Println("Failed to ack message: ", err)
		}
		done <- msg.Data()
	})
	if err != nil {
		logger.Fatal("AI_Service", "failed to consume from JetStream: %v", err)
	}
	defer cc.Stop()

	startTime := time.Now()
	var wg sync.WaitGroup

	// Dispatch all requests concurrently
	for _, filePath := range filesToProcess {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()

			fileBytes, err := os.ReadFile(path)
			if err != nil {
				logger.Info("AI_Service", "Failed to read file %s: %v", path, err)
				return
			}
			fileName := filepath.Base(path)

			req := &pb.ProcessDocumentRequest{
				FileContent: fileBytes,
				FileName:    fileName,
				ModelChoice: pb.ModelChoice_MODEL_CHOICE_EXTRACTION,
				Identity: &pb.InfrastructureIdentity{
					AppId: appID,
				},
				DocumentType: "Attorney_Closing_Invoice",
				TargetFields: []string{
					"AttorneyName", "OrderNumber", "LoanNumber",
					"AppointmentDateTime", "BorrowerNames", "BorrowerAddress",
					"ClosingFee", "BorrowerSigned",
				},
				CustomInstructions: "Extract precisely. Missing values return null.",
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Only need short timeout for async dispatch
			defer cancel()

			stream, err := client.ProcessDocument(ctx)
			if err != nil {
				logger.Info("AI_Service", "gRPC stream init failed for %s: %v", fileName, err)
				return
			}

			if err := stream.Send(req); err != nil {
				logger.Info("AI_Service", "gRPC send failed for %s: %v", fileName, err)
				return
			}

			if err := stream.CloseSend(); err != nil {
				logger.Info("AI_Service", "gRPC closesend failed for %s: %v", fileName, err)
				return
			}

			res, err := stream.Recv()
			if err != nil {
				logger.Info("AI_Service", "gRPC call failed for %s: %v", fileName, err)
				return
			}

			if !res.GetSuccess() {
				logger.Info("AI_Service", "Dispatch Error for %s: %s", fileName, res.GetErrorMessage())
				return
			}

			fmt.Printf("[Dispatched] %s -> Correlation ID: %s\n", fileName, res.GetCorrelationId())
		}(filePath)
	}

	// Wait for all gRPC dispatches to finish
	wg.Wait()
	fmt.Printf("\nAll %d files successfully dispatched to the async queue in %v.\n", len(filesToProcess), time.Since(startTime))
	fmt.Printf("Waiting for all async AI processing to finish via NATS...\n\n")

	// Wait for the background NATS responses
	receivedCount := 0
	timeout := time.After(30 * time.Minute) // Large timeout for batch processing

	for receivedCount < len(filesToProcess) {
		select {
		case payload := <-done:
			receivedCount++
			fmt.Printf("\n--- [%d/%d] Received Async NATS Response ---\n", receivedCount, len(filesToProcess))
			fmt.Printf("%s\n", string(payload))
		case <-timeout:
			logger.Fatal("AI_Service", "\nTimeout waiting for remaining NATS responses! (Received %d/%d)", receivedCount, len(filesToProcess))
		}
	}

	fmt.Printf("\nSuccessfully processed all %d files in %v!\n", len(filesToProcess), time.Since(startTime))
}
