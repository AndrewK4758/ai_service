package server

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func createTestPayloadStruct(m map[string]interface{}) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

// TestM2Challenger_AIService_IsScannedRootIgnored verifies that root payload IsScanned is ignored
func TestM2Challenger_AIService_IsScannedRootIgnored(t *testing.T) {
	// Root level IsScanned = true, but metadata and global_state have no IsScanned
	rootPayload := createTestPayloadStruct(map[string]interface{}{
		"IsScanned":  true,
		"is_scanned": "true",
	})
	req := &pb.ExecuteTaskRequest{Payload: rootPayload}

	assert.Panics(t, func() {
		extractIsScannedFlag(req)
	}, "extractIsScannedFlag should panic on missing global_state, ignoring legacy root payload")

	// Valid IsScanned inside global_state.functional
	validPayload := createTestPayloadStruct(map[string]interface{}{
		"global_state": map[string]interface{}{
			"functional": map[string]interface{}{
				"is_scanned": "true",
			},
		},
	})
	reqValid := &pb.ExecuteTaskRequest{Payload: validPayload}

	valValid, foundValid := extractIsScannedFlag(reqValid)
	if !foundValid || !valValid {
		t.Fatalf("expected extractIsScannedFlag to find true in global_state.functional, got found=%v, val=%v", foundValid, valValid)
	}
}

// TestM2Challenger_AIService_DocumentUriRootIgnored verifies that root payload DocumentUri is ignored
func TestM2Challenger_AIService_DocumentUriRootIgnored(t *testing.T) {
	// Root level DocumentUri = "s3://root/file.pdf", but storage, global_state, metadata, node_config have no DocumentUri
	rootPayload := createTestPayloadStruct(map[string]interface{}{
		"DocumentUri":  "s3://root/file.pdf",
		"document_uri": "s3://root/file.pdf",
	})
	req := &pb.ExecuteTaskRequest{Payload: rootPayload}

	assert.Panics(t, func() {
		_ = getDocumentUri(req)
	})

	// Valid DocumentUri inside global_state.functional
	validPayload := createTestPayloadStruct(map[string]interface{}{
		"global_state": map[string]interface{}{
			"functional": map[string]interface{}{
				"document_uri": "s3://valid/storage/file.pdf",
			},
		},
	})
	reqValid := &pb.ExecuteTaskRequest{Payload: validPayload}

	uriValid := getDocumentUri(reqValid)
	if uriValid != "s3://valid/storage/file.pdf" {
		t.Fatalf("expected getDocumentUri to find 's3://valid/storage/file.pdf', got %q", uriValid)
	}
}
