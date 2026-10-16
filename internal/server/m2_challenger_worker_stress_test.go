package server

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func createTestPayloadStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

func TestM2Challenger_AIService_IsScannedRootIgnored(t *testing.T) {
	rootPayload := createTestPayloadStruct(map[string]any{
		"IsScanned":  true,
		"is_scanned": "true",
	})
	req := &pb.ExecuteTaskRequest{Payload: rootPayload}

	assert.Panics(t, func() {
		extractIsScannedFlag(req)
	}, "extractIsScannedFlag should panic on missing global_state, ignoring legacy root payload")

	validPayload := createTestPayloadStruct(map[string]any{
		"global_state": map[string]any{
			"functional": map[string]any{
				"is_scanned": "true",
			},
		},
	})
	reqValid := &pb.ExecuteTaskRequest{Payload: validPayload}

	valValid, foundValid, _ := extractIsScannedFlag(reqValid)
	if !foundValid || !valValid {
		t.Fatalf("expected extractIsScannedFlag to find true in global_state.functional, got found=%v, val=%v", foundValid, valValid)
	}
}

func TestM2Challenger_AIService_DocumentUriRootSupported(t *testing.T) {
	rootPayload := createTestPayloadStruct(map[string]any{
		"node_config":  map[string]any{},
		"global_state": map[string]any{},
		"storage":      map[string]any{"document_uri": "s3://root/file.pdf"},
	})
	req := &pb.ExecuteTaskRequest{Payload: rootPayload}

	uriRoot, _ := getDocumentUri(req)
	if uriRoot != "s3://root/file.pdf" {
		t.Errorf("expected getDocumentUri to find 's3://root/file.pdf', got %q", uriRoot)
	}
}
