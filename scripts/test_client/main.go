package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	pb "ai_service/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <path-to-document>")
	}
	filePath := os.Args[1]

	// 1. Read the file bytes
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	fileName := filepath.Base(filePath)

	// 2. Connect to our gRPC Server
	// Note: We use insecure credentials here because we are running locally without TLS
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	client := pb.NewDocumentProcessorClient(conn)

	// 3. Create the request with the dynamic prompt instructions
	req := &pb.ProcessDocumentRequest{
		FileContent: fileBytes,
		FileName:    fileName,
		ModelChoice: pb.ModelChoice_MODEL_CHOICE_LOCAL,
		// Example of the new features using a realistic schema!
		DocumentType: "Attorney_Closing_Invoice",
		TargetFields: []string{
			"AttorneyName",
			"OrderNumber",
			"LoanNumber",
			"AppointmentDateTime",
			"BorrowerNames",
			"BorrowerAddress",
			"ClosingFee",
			"BorrowerSigned",
		},
		CustomInstructions: "Please extract all data precisely as it appears on the document. If a value is missing or blank (like the tracking number or signature date), return null instead of making something up. Pay special attention to the Order# and Loan#.",
		JsonSchema: `{
			"type": "object",
			"properties": {
				"AttorneyName": {"type": "string"},
				"OrderNumber": {"type": "string"},
				"LoanNumber": {"type": "string"},
				"AppointmentDateTime": {"type": "string"},
				"BorrowerNames": {"type": "array", "items": {"type": "string"}},
				"BorrowerAddress": {"type": "string"},
				"ClosingFee": {"type": "number"},
				"BorrowerSigned": {"type": "boolean"},
				"Notes": {"type": "string"}
			},
			"required": ["AttorneyName", "OrderNumber", "LoanNumber", "AppointmentDateTime"]
		}`,
	}

	fmt.Printf("Sending %s (%d bytes) to Go Processing Service...\n", fileName, len(fileBytes))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // LLMs can take a moment
	defer cancel()

	// 4. Send the request and wait for the AI response
	startTime := time.Now()
	res, err := client.ProcessDocument(ctx, req)
	if err != nil {
		log.Fatalf("gRPC call failed: %v", err)
	}

	fmt.Printf("\n--- Response received in %v ---\n", time.Since(startTime))
	fmt.Printf("Success: %v\n", res.GetSuccess())
	if !res.GetSuccess() {
		fmt.Printf("Error Message: %s\n", res.GetErrorMessage())
		return
	}

	fmt.Printf("\n--- AI JSON Metadata ---\n%s\n", res.GetMetadataJson())
}
