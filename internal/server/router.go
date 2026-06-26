package server

import (
	"context"
	"log"
	"sync"

	"ai_service/internal/ai"
	pb "doc_processor/shared_protos"
	"github.com/nats-io/nats.go"
)

type Job struct {
	Ctx           context.Context
	Req           *pb.ProcessDocumentRequest
	CorrelationID string
}

type AppRouter struct {
	mu             sync.Mutex
	channels       map[string]chan Job
	analyzer       ai.Analyzer
	js             nats.JetStreamContext
	queueSize      int
	workerPoolSize int
}

func NewAppRouter(analyzer ai.Analyzer, js nats.JetStreamContext, queueSize int, workerPoolSize int) *AppRouter {
	return &AppRouter{
		channels:       make(map[string]chan Job),
		analyzer:       analyzer,
		js:             js,
		queueSize:      queueSize,
		workerPoolSize: workerPoolSize,
	}
}

func (r *AppRouter) Dispatch(job Job) {
	appID := job.Req.GetAppId()
	if appID == "" {
		appID = "default"
	}

	r.mu.Lock()
	ch, exists := r.channels[appID]
	if !exists {
		// Create a new buffered channel for this app
		ch = make(chan Job, r.queueSize)
		r.channels[appID] = ch

		// Spin up a dedicated worker pool for this app's channel
		for i := 0; i < r.workerPoolSize; i++ {
			go r.startWorker(appID, ch)
		}
		log.Printf("Spun up new dedicated worker pool (size: %d) for app_id: %s", r.workerPoolSize, appID)
	}
	r.mu.Unlock()

	// Push job to the app's channel
	ch <- job
	log.Printf("[CorrelationID: %s] Job queued for app_id: %s", job.CorrelationID, appID)
}
