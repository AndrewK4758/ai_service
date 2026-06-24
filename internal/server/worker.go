package server

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"ai_service/internal/ai"
	"ai_service/internal/parser"
)

type processingResult struct {
	CorrelationID string `json:"correlation_id"`
	AppID         string `json:"app_id"`
	Success       bool   `json:"success"`
	MetadataJSON  string `json:"metadata_json,omitempty"`
	ErrorMessage  string `json:"error_message,omitempty"`
}

func (r *AppRouter) startWorker(appID string, ch <-chan Job) {
	for job := range ch {
		log.Printf("[CorrelationID: %s] Worker started processing document: %s", job.CorrelationID, job.Req.GetFileName())

		result := processingResult{
			CorrelationID: job.CorrelationID,
			AppID:         appID,
			Success:       true,
		}

		metadata, err := r.processDocumentSync(job)
		if err != nil {
			log.Printf("[CorrelationID: %s] Processing failed: %v", job.CorrelationID, err)
			result.Success = false
			result.ErrorMessage = err.Error()
		} else {
			log.Printf("[CorrelationID: %s] Processing succeeded", job.CorrelationID)
			result.MetadataJSON = metadata
		}

		// Publish to NATS JetStream
		if r.js != nil {
			subject := fmt.Sprintf("ai.processed.%s", appID)
			payload, _ := json.Marshal(result)
			_, err := r.js.Publish(subject, payload)
			if err != nil {
				log.Printf("[CorrelationID: %s] Failed to publish to NATS subject %s: %v", job.CorrelationID, subject, err)
			} else {
				log.Printf("[CorrelationID: %s] Successfully published result to NATS subject %s", job.CorrelationID, subject)
			}
		}
	}
}

func (r *AppRouter) processDocumentSync(job Job) (string, error) {
	req := job.Req
	ctx := job.Ctx
	fileName := req.GetFileName()
	ext := strings.ToLower(filepath.Ext(fileName))

	opts := ai.AnalyzeOptions{
		DocumentType:       req.GetDocumentType(),
		TargetFields:       req.GetTargetFields(),
		JsonSchema:         req.GetJsonSchema(),
		CustomInstructions: req.GetCustomInstructions(),
	}

	switch ext {
	case ".docx", ".txt":
		var extractedText string
		var err error
		if ext == ".docx" {
			extractedText, err = parser.ExtractDocxText(req.GetFileContent())
			if err != nil {
				return "", fmt.Errorf("failed to parse DOCX: %w", err)
			}
		} else {
			extractedText = string(req.GetFileContent())
		}
		if len(extractedText) > 60000 {
			extractedText = extractedText[:60000] + "\n...[TRUNCATED due to length]..."
		}
		return r.analyzer.AnalyzeText(ctx, extractedText, opts)

	case ".pdf":
		extractedText, err := parser.ExtractPdfText(req.GetFileContent())
		if err != nil {
			return "", fmt.Errorf("failed to parse PDF: %w", err)
		}

		if len(strings.TrimSpace(extractedText)) < 10 {
			images, errConv := parser.ConvertPdfToImages(req.GetFileContent())
			if errConv != nil {
				return "", fmt.Errorf("failed to convert PDF to images: %w", errConv)
			}
			return r.analyzer.AnalyzeImage(ctx, images, opts)
		}

		if len(extractedText) > 60000 {
			extractedText = extractedText[:60000] + "\n...[TRUNCATED due to length]..."
		}
		return r.analyzer.AnalyzeText(ctx, extractedText, opts)

	case ".png", ".jpg", ".jpeg":
		return r.analyzer.AnalyzeImage(ctx, [][]byte{req.GetFileContent()}, opts)

	default:
		return "", fmt.Errorf("unsupported file extension: %s", ext)
	}
}
