package storage

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"os"
)

func init() {
	os.Setenv("STORAGE_FETCH_RETRY_COUNT", "3")
	os.Setenv("STORAGE_FETCH_RETRY_SLEEP_MS", "1")
}

type mockStorageGatewayClient struct {
	uploadStream   *mockUploadStream
	downloadStream *mockDownloadStream
	uploadErr      error
	downloadErr    error
}

func (m *mockStorageGatewayClient) UploadDocument(ctx context.Context, opts ...grpc.CallOption) (pb.StorageGatewayService_UploadDocumentClient, error) {
	if m.uploadErr != nil {
		return nil, m.uploadErr
	}
	return m.uploadStream, nil
}

func (m *mockStorageGatewayClient) DownloadDocument(ctx context.Context, in *pb.DownloadRequest, opts ...grpc.CallOption) (pb.StorageGatewayService_DownloadDocumentClient, error) {
	if m.downloadErr != nil {
		return nil, m.downloadErr
	}
	return m.downloadStream, nil
}

func (m *mockStorageGatewayClient) ClearJobData(ctx context.Context, in *pb.ClearJobDataRequest, opts ...grpc.CallOption) (*pb.ClearJobDataResponse, error) {
	return &pb.ClearJobDataResponse{Success: true}, nil
}

type mockUploadStream struct {
	grpc.ClientStream
	sendErr      error
	closeResp    *pb.UploadResponse
	closeErr     error
	receivedReqs []*pb.UploadRequest
}

func (m *mockUploadStream) Send(req *pb.UploadRequest) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.receivedReqs = append(m.receivedReqs, req)
	return nil
}

func (m *mockUploadStream) CloseAndRecv() (*pb.UploadResponse, error) {
	return m.closeResp, m.closeErr
}

type mockDownloadStream struct {
	grpc.ClientStream
	responses []*pb.DownloadResponse
	errs      []error
	idx       int
}

func (m *mockDownloadStream) Recv() (*pb.DownloadResponse, error) {
	if m.idx >= len(m.responses) {
		if m.idx < len(m.errs) && m.errs[m.idx] != nil {
			return nil, m.errs[m.idx]
		}
		return nil, io.EOF
	}
	resp := m.responses[m.idx]
	err := m.errs[m.idx]
	m.idx++
	return resp, err
}

func TestGatewayStorage_FetchFile_Success(t *testing.T) {
	mockClient := &mockStorageGatewayClient{
		downloadStream: &mockDownloadStream{
			responses: []*pb.DownloadResponse{
				{ChunkData: []byte("hello ")},
				{ChunkData: []byte("world")},
			},
			errs: []error{nil, nil, io.EOF},
		},
	}
	store := NewGatewayStorage(mockClient)

	ctx := context.Background()
	data, err := store.FetchFile(ctx, "internal-store://test", nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if string(data) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", string(data))
	}
}

func TestGatewayStorage_SaveFile_Success(t *testing.T) {
	uploadStream := &mockUploadStream{
		closeResp: &pb.UploadResponse{
			Success:    true,
			StorageUri: "internal-store://job123/file.pdf",
		},
	}
	mockClient := &mockStorageGatewayClient{
		uploadStream: uploadStream,
	}
	store := NewGatewayStorage(mockClient)

	ctx := context.Background()
	identity := &pb.InfrastructureIdentity{JobId: "job123"}
	uri, err := store.SaveFile(ctx, "job123/file.pdf", identity, []byte("data"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if uri != "internal-store://job123/file.pdf" {
		t.Errorf("Unexpected URI: %s", uri)
	}
	if len(uploadStream.receivedReqs) != 2 {
		t.Errorf("Expected 2 chunks sent, got %d", len(uploadStream.receivedReqs))
	}
}

func TestGatewayStorage_SaveFile_InvalidPath(t *testing.T) {
	store := NewGatewayStorage(&mockStorageGatewayClient{})
	identity := &pb.InfrastructureIdentity{JobId: "job123"}
	_, err := store.SaveFile(context.Background(), "invalidpath", identity, []byte("data"))
	if err == nil {
		t.Fatal("Expected error for invalid path, got nil")
	}
}

func TestGatewayStorage_FetchFile_StreamError(t *testing.T) {
	mockClient := &mockStorageGatewayClient{
		downloadStream: &mockDownloadStream{
			responses: []*pb.DownloadResponse{
				{ChunkData: []byte("hello ")},
				nil,
			},
			errs: []error{nil, fmt.Errorf("network partition")},
		},
	}
	store := NewGatewayStorageWithRetry(mockClient, 0, 1)

	_, err := store.FetchFile(context.Background(), "internal-store://test", nil)
	if err == nil {
		t.Fatal("Expected error on stream interruption, got nil")
	}
}

func TestGatewayStorage_SaveFile_Rejected(t *testing.T) {
	mockClient := &mockStorageGatewayClient{
		uploadStream: &mockUploadStream{
			closeResp: &pb.UploadResponse{
				Success:      false,
				ErrorMessage: "quota exceeded",
			},
		},
	}
	store := NewGatewayStorage(mockClient)

	identity := &pb.InfrastructureIdentity{JobId: "job123"}
	_, err := store.SaveFile(context.Background(), "job123/file.pdf", identity, []byte("data"))
	if err == nil {
		t.Fatal("Expected error on server rejection, got nil")
	}
	if err.Error() != "gateway rejected upload for file.pdf: quota exceeded" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestGatewayStorage_ContextCancellation(t *testing.T) {
	store := NewGatewayStorage(&mockStorageGatewayClient{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.FetchFile(ctx, "internal-store://test", nil)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled on FetchFile, got %v", err)
	}

	identity := &pb.InfrastructureIdentity{JobId: "job123"}
	_, err = store.SaveFile(ctx, "job123/file.pdf", identity, []byte("data"))
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled on SaveFile, got %v", err)
	}
}

func TestGatewayStorage_ForwardMetadata(t *testing.T) {
	md := metadata.Pairs("tenant_id", "test_tenant")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	reqCtx := forwardMetadata(ctx)

	outMD, ok := metadata.FromOutgoingContext(reqCtx)
	if !ok {
		t.Fatal("Expected outgoing metadata")
	}
	if len(outMD["tenant_id"]) == 0 || outMD["tenant_id"][0] != "test_tenant" {
		t.Errorf("Expected tenant_id=test_tenant in outgoing metadata, got %v", outMD)
	}
}

type mockMultiAttemptGatewayClient struct {
	pb.StorageGatewayServiceClient
	calls      int64
	streamFunc func(call int) (pb.StorageGatewayService_DownloadDocumentClient, error)
}

func (m *mockMultiAttemptGatewayClient) DownloadDocument(ctx context.Context, in *pb.DownloadRequest, opts ...grpc.CallOption) (pb.StorageGatewayService_DownloadDocumentClient, error) {
	call := atomic.AddInt64(&m.calls, 1)
	if m.streamFunc != nil {
		return m.streamFunc(int(call))
	}
	return nil, fmt.Errorf("no streamFunc configured")
}

func TestGatewayStorage_FetchFile_RetrySuccess(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			if call == 1 {
				return nil, fmt.Errorf("transient connection error")
			}
			return &mockDownloadStream{
				responses: []*pb.DownloadResponse{
					{ChunkData: []byte("recovered data")},
				},
				errs: []error{nil},
			}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 1)
	data, err := store.FetchFile(context.Background(), "internal-store://test/doc.pdf", nil)
	if err != nil {
		t.Fatalf("Expected successful fetch on 2nd attempt, got err: %v", err)
	}
	if string(data) != "recovered data" {
		t.Errorf("Expected 'recovered data', got %q", string(data))
	}
	if atomic.LoadInt64(&client.calls) != 2 {
		t.Errorf("Expected exactly 2 calls (1 failure + 1 success), got %d", atomic.LoadInt64(&client.calls))
	}
}

func TestGatewayStorage_FetchFile_ExhaustedRetries(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return nil, fmt.Errorf("persistent gateway error")
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 1)
	_, err := store.FetchFile(context.Background(), "internal-store://test/doc.pdf", nil)
	if err == nil {
		t.Fatal("Expected error after exhausted retries, got nil")
	}
	if atomic.LoadInt64(&client.calls) != 4 {
		t.Errorf("Expected 4 calls for 3 retries, got %d", atomic.LoadInt64(&client.calls))
	}
}

func TestGatewayStorage_FetchFile_ContextCancelledDuringRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			cancel()
			return nil, fmt.Errorf("initial connection failure")
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 100)
	_, err := store.FetchFile(ctx, "internal-store://test/doc.pdf", nil)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled when context is cancelled during retry backoff, got: %v", err)
	}
}

func TestGatewayStorage_FetchFile_ZeroRetries(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return nil, fmt.Errorf("single attempt failure")
		},
	}

	store := NewGatewayStorageWithRetry(client, 0, 1)
	_, err := store.FetchFile(context.Background(), "internal-store://test/doc.pdf", nil)
	if err == nil {
		t.Fatal("Expected error with zero retries, got nil")
	}
	if atomic.LoadInt64(&client.calls) != 1 {
		t.Errorf("Expected exactly 1 call when retryCount is 0, got %d", atomic.LoadInt64(&client.calls))
	}
}

func TestGatewayStorage_AdversarialMidStreamInterruption(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			if call == 1 {
				return &mockDownloadStream{
					responses: []*pb.DownloadResponse{
						{ChunkData: []byte("part1_")},
						nil,
					},
					errs: []error{nil, fmt.Errorf("connection reset by peer mid-stream")},
				}, nil
			}
			return &mockDownloadStream{
				responses: []*pb.DownloadResponse{
					{ChunkData: []byte("part1_")},
					{ChunkData: []byte("part2_complete")},
				},
				errs: []error{nil, nil},
			}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 5)
	data, err := store.FetchFile(context.Background(), "internal-store://test/midstream.pdf", nil)
	if err != nil {
		t.Fatalf("Expected recovery after mid-stream interruption, got err: %v", err)
	}
	if string(data) != "part1_part2_complete" {
		t.Errorf("Expected 'part1_part2_complete', got %q", string(data))
	}
	if atomic.LoadInt64(&client.calls) != 2 {
		t.Errorf("Expected 2 calls (1 mid-stream failure + 1 success), got %d", atomic.LoadInt64(&client.calls))
	}
}

func TestGatewayStorage_AdversarialConcurrentStreams(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return &mockDownloadStream{
				responses: []*pb.DownloadResponse{
					{ChunkData: []byte("concurrent_chunk_1 ")},
					{ChunkData: []byte("concurrent_chunk_2")},
				},
				errs: []error{nil, nil},
			}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 2, 1)
	concurrency := 20
	errChan := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			data, err := store.FetchFile(context.Background(), fmt.Sprintf("internal-store://tenant/app/job/doc_%d.pdf", id), nil)
			if err != nil {
				errChan <- err
				return
			}
			if string(data) != "concurrent_chunk_1 concurrent_chunk_2" {
				errChan <- fmt.Errorf("unexpected content: %s", string(data))
				return
			}
			errChan <- nil
		}(i)
	}

	for i := 0; i < concurrency; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent fetch failed: %v", err)
		}
	}
}
