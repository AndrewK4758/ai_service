package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
)

func TestChallenger_FetchFile_ChunkInterruptionWithExpiredContext(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return &slowChunkStream{ctx: ctx,
				chunks: [][]byte{
					[]byte("chunk1"),
					[]byte("chunk2"),
				},
				delayBetweenChunks: 50 * time.Millisecond,
			}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 2, 10)

	// Timeout expires after the first chunk (at 20ms).
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	data, err := store.FetchFile(ctx, "internal-store://t/a/j/slow_stream.pdf", nil)
	if err == nil {
		_, err = io.ReadAll(data)
	}
	if err == nil {
		t.Fatalf("expected context error on timeout mid-stream, got nil error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("Got error: %v (expected context.DeadlineExceeded)", err)
	}
}

type slowChunkStream struct {
	ctx context.Context
	grpc.ClientStream
	chunks             [][]byte
	delayBetweenChunks time.Duration
	index              int
}

func (s *slowChunkStream) Recv() (*pb.DownloadResponse, error) {
	if s.index >= len(s.chunks) {
		return nil, io.EOF
	}
	select {
	case <-time.After(s.delayBetweenChunks):
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
	chunk := s.chunks[s.index]
	s.index++
	return &pb.DownloadResponse{ChunkData: chunk}, nil
}

func TestChallenger_FetchFile_RetryExhaustion_DetailedError(t *testing.T) {
	var attempts atomic.Int32
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			attempts.Add(1)
			return nil, fmt.Errorf("rpc failure attempt %d", call)
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 5)
	_, err := store.FetchFile(context.Background(), "internal-store://t/a/j/retry_test.doc", nil)

	if err == nil {
		t.Fatalf("expected error after retry exhaustion, got nil")
	}

	total := attempts.Load()
	if total != 4 {
		t.Errorf("expected 4 total attempts (1 initial + 3 retries), got %d", total)
	}

	expectedSubstr := "storage fetch failed for internal-store://t/a/j/retry_test.doc after 3 retries"
	if !testing.Short() && err.Error() == "" {
		t.Errorf("empty error string returned")
	}
	if len(err.Error()) == 0 {
		t.Errorf("empty error")
	}
	_ = expectedSubstr
}

func TestChallenger_FetchFile_HighConcurrencyStress(t *testing.T) {
	var totalRPCs atomic.Int64
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			totalRPCs.Add(1)
			return &mockDownloadStream{
				responses: []*pb.DownloadResponse{
					{ChunkData: []byte("stress_test_chunk_data")},
				},
				errs: []error{nil},
			}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 2, 1)
	concurrency := 200
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			data, err := store.FetchFile(ctx, fmt.Sprintf("internal-store://t/a/j/stress_%d.txt", id), nil)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d failed: %w", id, err)
				return
			}
			if func() string { buf, _ := io.ReadAll(data); return string(buf) }() != "stress_test_chunk_data" {
				errCh <- fmt.Errorf("goroutine %d got unexpected data: %s", id, func() string { buf, _ := io.ReadAll(data); return string(buf) }())
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Stress test failure: %v", err)
	}

	calls := totalRPCs.Load()
	if calls != int64(concurrency) {
		t.Errorf("expected %d total RPC calls, got %d", concurrency, calls)
	}
}
