package parser

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
)

func TestExtractPdfText_Empty(t *testing.T) {
	_, err := ExtractPdfText(nil)
	if err == nil {
		t.Fatal("Expected error for empty bytes, got nil")
	}
	if !strings.Contains(err.Error(), "empty byte slice") {
		t.Errorf("Expected 'empty byte slice' error, got %v", err)
	}
}

func TestExtractDocxText_InvalidZip(t *testing.T) {
	_, err := ExtractDocxText([]byte("not a zip file"))
	if err == nil {
		t.Fatal("Expected error for invalid zip, got nil")
	}
}

func TestExtractDocxText_NoDocumentXML(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	w.Create("dummy.txt")
	w.Close()

	_, err := ExtractDocxText(buf.Bytes())
	if err == nil {
		t.Fatal("Expected error for missing word/document.xml, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Expected 'not found' error, got %v", err)
	}
}

func TestExtractDocxText_Valid(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)

	f, _ := w.Create("word/document.xml")
	f.Write([]byte(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
	<w:body>
		<w:p><w:r><w:t>Hello </w:t></w:r><w:r><w:t>World!</w:t></w:r></w:p>
		<w:p><w:r><w:t>Second paragraph.</w:t></w:r></w:p>
	</w:body>
</w:document>`))
	w.Close()

	text, err := ExtractDocxText(buf.Bytes())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := "Hello World!\nSecond paragraph.\n"
	if text != expected {
		t.Errorf("Expected %q, got %q", expected, text)
	}
}
