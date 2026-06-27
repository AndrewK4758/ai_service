package parser

import (
	"strings"
	"testing"
)

func TestConvertPdfToImages_InvalidPDF(t *testing.T) {
	// Without pdftoppm available, or with an invalid PDF, this should return an error
	_, err := ConvertPdfToImages([]byte("not a pdf"))
	if err == nil {
		t.Fatal("Expected error for invalid PDF, got nil")
	}
	if !strings.Contains(err.Error(), "pdftoppm failed") && !strings.Contains(err.Error(), "no images generated") {
		t.Errorf("Expected pdftoppm failure or no images, got %v", err)
	}
}
