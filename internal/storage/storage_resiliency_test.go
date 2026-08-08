package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
)

// TestResiliency_RetryCountAndSleepVerification verifies STORAGE_FETCH_RETRY_COUNT and STORAGE_FETCH_RETRY_SLEEP_MS.
func TestResiliency_RetryCountAndSleepVerification(t *testing.T) {
	t.Setenv("STORAGE_FETCH_RETRY_COUNT", "3")
	t.Setenv("STORAGE_FETCH_RETRY_SLEEP_MS", "10")

	var callCount int64
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			atomic.AddInt64(&callCount, 1)
			return nil, fmt.Errorf("simulated storage failure attempt %d", call)
		},
	}

	store := NewGatewayStorage(client)
	start := time.Now()
	_, err := store.FetchFile(context.Background(), "internal-store://tenant/app/job/doc.pdf", nil)
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected error after exhausted retries, got nil")
	}

	finalCalls := atomic.LoadInt64(&callCount)
	if finalCalls != 4 { // 1 initial + 3 retries = 4
		t.Errorf("expected 4 total attempts for retryCount=3, got %d", finalCalls)
	}

	// 3 retries with 10ms sleep each should take at least 25ms
	if duration < 25*time.Millisecond {
		t.Errorf("expected sleep duration >= 25ms, got %v", duration)
	}
}

// TestResiliency_ImmediateContextTimeout verifies FetchFile fails immediately when context is pre-cancelled.
func TestResiliency_ImmediateContextTimeout(t *testing.T) {
	var callCount int64
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			atomic.AddInt64(&callCount, 1)
			return nil, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 50)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before start

	_, err := store.FetchFile(ctx, "internal-store://tenant/app/job/doc.pdf", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if atomic.LoadInt64(&callCount) != 0 {
		t.Errorf("expected 0 RPC calls when context is pre-canceled, got %d", atomic.LoadInt64(&callCount))
	}
}

// TestResiliency_StreamRecvContextError verifies context error during chunk receiving.
func TestResiliency_StreamRecvContextError(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return &contextAwareDownloadStream{}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	time.Sleep(30 * time.Millisecond) // Ensure context expires before Recv

	_, err := store.FetchFile(ctx, "internal-store://tenant/app/job/slow.pdf", nil)
	if err == nil {
		t.Fatalf("expected error on context timeout, got nil")
	}
}

type contextAwareDownloadStream struct {
	grpc.ClientStream
}

func (s *contextAwareDownloadStream) Recv() (*pb.DownloadResponse, error) {
	return nil, fmt.Errorf("rpc error: code = Canceled desc = context canceled")
}

// TestResiliency_EOFWithExpiredContext tests when stream returns EOF after context has expired.
func TestResiliency_EOFWithExpiredContext(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return &eofAfterDelayStream{ctx: ctx,delay: 30 * time.Millisecond}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 1, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	data, err := store.FetchFile(ctx, "internal-store://tenant/app/job/eof_delay.pdf", nil)
	if err == nil {
		_, err = io.ReadAll(data)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded when context expires during stream recv, got: %v", err)
	}
}

type eofAfterDelayStream struct {
	ctx context.Context
	grpc.ClientStream
	delay time.Duration
}

func (s *eofAfterDelayStream) Recv() (*pb.DownloadResponse, error) {
	select {
	case <-time.After(s.delay):
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
	return &pb.DownloadResponse{ChunkData: []byte("partial")}, io.EOF
}

// TestResiliency_HighConcurrencyRaceDetector stress tests GatewayStorage under high parallel load.
func TestResiliency_HighConcurrencyRaceDetector(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return &mockDownloadStream{
				responses: []*pb.DownloadResponse{
					{ChunkData: []byte("concurrent_data_chunk")},
				},
				errs: []error{nil},
			}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 2, 1)
	concurrency := 100
	done := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			data, err := store.FetchFile(ctx, fmt.Sprintf("internal-store://tenant/app/job/doc_%d.pdf", id), nil)
			if err != nil {
				done <- err
				return
			}
			if func() string { buf, _ := io.ReadAll(data); return string(buf) }() != "concurrent_data_chunk" {
				done <- fmt.Errorf("unexpected content: %s", func() string { buf, _ := io.ReadAll(data); return string(buf) }())
				return
			}
			done <- nil
		}(i)
	}

	for i := 0; i < concurrency; i++ {
		if err := <-done; err != nil {
			t.Errorf("High concurrency fetch %d failed: %v", i, err)
		}
	}
}
