package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"os"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/testutil"
	"google.golang.org/grpc"
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
	return testutil.RecvMockDownloadChunks(&m.idx, m.responses, m.errs)
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

	if func() string { buf, _ := io.ReadAll(data); return string(buf) }() != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", func() string { buf, _ := io.ReadAll(data); return string(buf) }())
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
	uri, err := store.SaveFile(ctx, "job123/file.pdf", identity, bytes.NewReader([]byte("data")))
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
	store := NewGatewayStorage(&mockStorageGatewayClient{uploadStream: &mockUploadStream{closeResp: &pb.UploadResponse{Success: false, ErrorMessage: "invalid path"}}})
	identity := &pb.InfrastructureIdentity{JobId: "job123"}
	_, err := store.SaveFile(context.Background(), "invalidpath", identity, bytes.NewReader([]byte("data")))
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

	data, err := store.FetchFile(context.Background(), "internal-store://test", nil)
	if err == nil {
		_, err = io.ReadAll(data)
	}
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
	_, err := store.SaveFile(context.Background(), "job123/file.pdf", identity, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("Expected error on server rejection, got nil")
	}
	if err.Error() != "gateway rejected upload for file.pdf: quota exceeded" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestGatewayStorage_ContextCancellation(t *testing.T) {
	store := NewGatewayStorage(&mockStorageGatewayClient{uploadStream: &mockUploadStream{closeResp: &pb.UploadResponse{Success: false, ErrorMessage: "invalid path"}}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.FetchFile(ctx, "internal-store://test", nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled on FetchFile, got %v", err)
	}

	identity := &pb.InfrastructureIdentity{JobId: "job123"}
	_, err = store.SaveFile(ctx, "job123/file.pdf", identity, bytes.NewReader([]byte("data")))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled on SaveFile, got %v", err)
	}
}

func TestGatewayStorage_ForwardMetadata(t *testing.T) {
	testutil.AssertForwardMetadata(t, forwardMetadata, "tenant_id", "test_tenant")
}

type mockMultiAttemptGatewayClient struct {
	pb.StorageGatewayServiceClient
	calls      atomic.Int64
	streamFunc func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error)
}

func (m *mockMultiAttemptGatewayClient) DownloadDocument(ctx context.Context, in *pb.DownloadRequest, opts ...grpc.CallOption) (pb.StorageGatewayService_DownloadDocumentClient, error) {
	call := m.calls.Add(1)
	if m.streamFunc != nil {
		return m.streamFunc(ctx, int(call))
	}
	return nil, fmt.Errorf("no streamFunc configured")
}

func TestGatewayStorage_FetchFile_RetrySuccess(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
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
	if func() string { buf, _ := io.ReadAll(data); return string(buf) }() != "recovered data" {
		t.Errorf("Expected 'recovered data', got %q", func() string { buf, _ := io.ReadAll(data); return string(buf) }())
	}
	if client.calls.Load() != 2 {
		t.Errorf("Expected exactly 2 calls (1 failure + 1 success), got %d", client.calls.Load())
	}
}

func TestGatewayStorage_FetchFile_ExhaustedRetries(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return nil, fmt.Errorf("persistent gateway error")
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 1)
	_, err := store.FetchFile(context.Background(), "internal-store://test/doc.pdf", nil)
	if err == nil {
		t.Fatal("Expected error after exhausted retries, got nil")
	}
	if client.calls.Load() != 4 {
		t.Errorf("Expected 4 calls for 3 retries, got %d", client.calls.Load())
	}
}

func TestGatewayStorage_FetchFile_ContextCancelledDuringRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			cancel()
			return nil, fmt.Errorf("initial connection failure")
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 100)
	_, err := store.FetchFile(ctx, "internal-store://test/doc.pdf", nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled when context is cancelled during retry backoff, got: %v", err)
	}
}

func TestGatewayStorage_FetchFile_ZeroRetries(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			return nil, fmt.Errorf("single attempt failure")
		},
	}

	store := NewGatewayStorageWithRetry(client, 0, 1)
	_, err := store.FetchFile(context.Background(), "internal-store://test/doc.pdf", nil)
	if err == nil {
		t.Fatal("Expected error with zero retries, got nil")
	}
	if client.calls.Load() != 1 {
		t.Errorf("Expected exactly 1 call when retryCount is 0, got %d", client.calls.Load())
	}
}

func TestGatewayStorage_AdversarialMidStreamInterruption(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
			if call == 1 {
				return &mockDownloadStream{
					responses: []*pb.DownloadResponse{
						{ChunkData: []byte("part1_")},
						nil,
					},
					errs: []error{nil, fmt.Errorf("mid-stream disconnect")},
				}, nil
			}
			return &mockDownloadStream{
				responses: []*pb.DownloadResponse{
					{ChunkData: []byte("part1_")},
					{ChunkData: []byte("part2_complete")},
					nil,
				},
				errs: []error{nil, nil, nil},
			}, nil
		},
	}

	store := NewGatewayStorageWithRetry(client, 3, 5)
	data, err := store.FetchFile(context.Background(), "internal-store://test/midstream.pdf", nil)
	if err == nil {
		_, err = io.ReadAll(data)
	}
	if err == nil {
		t.Fatalf("Expected error after mid-stream interruption, got nil")
	}
	if client.calls.Load() != 1 {
		t.Errorf("Expected 1 call, got %d", client.calls.Load())
	}
}

func TestGatewayStorage_AdversarialConcurrentStreams(t *testing.T) {
	client := &mockMultiAttemptGatewayClient{
		streamFunc: func(ctx context.Context, call int) (pb.StorageGatewayService_DownloadDocumentClient, error) {
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

	for i := range concurrency {
		go func(id int) {
			data, err := store.FetchFile(context.Background(), fmt.Sprintf("internal-store://tenant/app/job/doc_%d.pdf", id), nil)
			if err != nil {
				errChan <- err
				return
			}
			if func() string { buf, _ := io.ReadAll(data); return string(buf) }() != "concurrent_chunk_1 concurrent_chunk_2" {
				errChan <- fmt.Errorf("unexpected content: %s", func() string { buf, _ := io.ReadAll(data); return string(buf) }())
				return
			}
			errChan <- nil
		}(i)
	}

	for range concurrency {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent fetch failed: %v", err)
		}
	}
}
