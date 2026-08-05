package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
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

func (g *GatewayStorage) FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	retryCount := g.retryCount
	if retryCount < 0 {
		retryCount = envutil.MustGetInt("STORAGE_FETCH_RETRY_COUNT")
	}

	retrySleepMs := g.retrySleepMs
	if retrySleepMs <= 0 {
		retrySleepMs = envutil.MustGetInt("STORAGE_FETCH_RETRY_SLEEP_MS")
	}

	reqCtx := forwardMetadata(ctx)
	req := &pb.DownloadRequest{
		StorageUri: relativePath,
		Identity:   identity,
	}

	var lastErr error
	for attempt := 0; attempt <= retryCount; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := g.sleepBeforeRetry(ctx, attempt, retrySleepMs); err != nil {
			return nil, err
		}

		stream, err := g.client.DownloadDocument(reqCtx, req)
		if err != nil {
			lastErr = fmt.Errorf("failed to initiate download stream for %s (attempt %d/%d): %w", relativePath, attempt+1, retryCount+1, err)
			continue
		}

		data, streamErr := g.receiveStreamChunks(stream, relativePath)
		if streamErr != nil {
			lastErr = fmt.Errorf("failed during chunk receive for %s (attempt %d/%d): %w", relativePath, attempt+1, retryCount+1, streamErr)
			continue
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		return data, nil
	}

	return nil, fmt.Errorf("storage fetch failed for %s after %d retries: %w", relativePath, retryCount, lastErr)
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

func (g *GatewayStorage) receiveStreamChunks(stream pb.StorageGatewayService_DownloadDocumentClient, relativePath string) ([]byte, error) {
	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return data, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive chunk for %s: %w", relativePath, err)
		}
		data = append(data, resp.ChunkData...)
	}
}

func (g *GatewayStorage) SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	parts := strings.SplitN(relativePath, "/", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid save path format, expected jobID/filename, got: %s", relativePath)
	}
	filename := parts[1]

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

	chunkSize := 64 * 1024
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		err = stream.Send(&pb.UploadRequest{
			UploadElement: &pb.UploadRequest_ChunkData{
				ChunkData: data[i:end],
			},
		})
		if err != nil {
			return "", fmt.Errorf("failed to send chunk for %s: %w", filename, err)
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
