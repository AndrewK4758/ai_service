package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/envutil"
	"google.golang.org/grpc/metadata"
)

type GatewayStorage struct {
	client       pb.StorageGatewayServiceClient
	retryCount   int
	retrySleepMs int
}

func NewGatewayStorage(client pb.StorageGatewayServiceClient) *GatewayStorage {
	return &GatewayStorage{
		client:       client,
		retryCount:   envutil.MustGetInt("STORAGE_FETCH_RETRY_COUNT"),
		retrySleepMs: envutil.MustGetInt("STORAGE_FETCH_RETRY_SLEEP_MS"),
	}
}

func NewGatewayStorageWithRetry(client pb.StorageGatewayServiceClient, retryCount, retrySleepMs int) *GatewayStorage {
	return &GatewayStorage{
		client:       client,
		retryCount:   retryCount,
		retrySleepMs: retrySleepMs,
	}
}

func forwardMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md)
}

type grpcStreamReader struct {
	stream pb.StorageGatewayService_DownloadDocumentClient
	buffer []byte
}

func (r *grpcStreamReader) Read(p []byte) (int, error) {
	if len(r.buffer) > 0 {
		n := copy(p, r.buffer)
		r.buffer = r.buffer[n:]
		return n, nil
	}
	resp, err := r.stream.Recv()
	if err != nil {
		return 0, err
	}
	if len(resp.ChunkData) == 0 {
		return 0, nil
	}
	n := copy(p, resp.ChunkData)
	if n < len(resp.ChunkData) {
		r.buffer = resp.ChunkData[n:]
	}
	return n, nil
}

func (g *GatewayStorage) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) (io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context error: %w", err)
	}

	if g.retryCount < 0 {
		return nil, fmt.Errorf("invalid storage configuration: negative retry count")
	}
	if g.retrySleepMs <= 0 {
		return nil, fmt.Errorf("invalid storage configuration: zero or negative retry sleep")
	}

	reqCtx := forwardMetadata(ctx)
	req := &pb.DownloadRequest{
		StorageUri: relativePath,
		Identity:   identity,
	}

	var lastErr error
	for attempt := 0; attempt <= g.retryCount; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context error: %w", err)
		}

		if err := g.sleepBeforeRetry(ctx, attempt, g.retrySleepMs); err != nil {
			return nil, err
		}

		stream, err := g.client.DownloadDocument(reqCtx, req)
		if err != nil {
			lastErr = fmt.Errorf("failed to initiate download stream for %s (attempt %d/%d): %w", relativePath, attempt+1, g.retryCount+1, err)
			continue
		}

		return &grpcStreamReader{stream: stream}, nil
	}

	return nil, fmt.Errorf("storage fetch failed for %s after %d retries: %w", relativePath, g.retryCount, lastErr)
}

func (g *GatewayStorage) sleepBeforeRetry(ctx context.Context, attempt, retrySleepMs int) error {
	if attempt == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(retrySleepMs) * time.Millisecond):
		return nil
	}
}

func (g *GatewayStorage) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data io.Reader) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("context error: %w", err)
	}

	filename := filepath.Base(relativePath)

	reqCtx := forwardMetadata(ctx)
	stream, err := g.client.UploadDocument(reqCtx)
	if err != nil {
		return "", fmt.Errorf("failed to initiate upload stream: %w", err)
	}

	err = stream.Send(&pb.UploadRequest{
		UploadElement: &pb.UploadRequest_Metadata{
			Metadata: &pb.UploadMetadata{
				Identity:         identity,
				OriginalFilename: filename,
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to send upload metadata for %s: %w", filename, err)
	}

	chunkSize := envutil.MustGetInt("UPLOAD_CHUNK_SIZE")
	if chunkSize <= 0 {
		chunkSize = 64 * 1024
	}

	buf := make([]byte, chunkSize)
	for {
		n, err := data.Read(buf)
		if n > 0 {
			sendErr := stream.Send(&pb.UploadRequest{
				UploadElement: &pb.UploadRequest_ChunkData{
					ChunkData: buf[:n],
				},
			})
			if sendErr != nil {
				return "", fmt.Errorf("failed to send chunk for %s: %w", filename, sendErr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("error reading data for %s: %w", filename, err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", fmt.Errorf("failed to close upload stream for %s: %w", filename, err)
	}

	if !resp.Success {
		return "", fmt.Errorf("gateway rejected upload for %s: %s", filename, resp.ErrorMessage)
	}

	return resp.StorageUri, nil
}
