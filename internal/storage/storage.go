package storage

import (
	"context"
	"io"

	pb "github.com/AndrewK4758/shared_protos"
)

type Manager interface {
	FetchFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity) (io.Reader, error)
	SaveFile(ctx context.Context, relativePath string, identity *pb.InfrastructureIdentity, data io.Reader) (string, error)
}
