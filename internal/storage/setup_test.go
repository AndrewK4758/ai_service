package storage

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("UPLOAD_CHUNK_SIZE", "1024")
	os.Exit(m.Run())
}
