package parser

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"os/exec"
)

// ExtractPdfText uses poppler-utils (pdftotext) for extremely robust extraction
func ExtractPdfText(fileBytes []byte) (string, error) {
	if len(fileBytes) == 0 {
		return "", fmt.Errorf("cannot extract text from empty byte slice")
	}

	cmd := exec.Command("pdftotext", "-", "-")
	cmd.Stdin = bytes.NewReader(fileBytes)

	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("pdftotext failed: %w, stderr: %s", err, errBuf.String())
	}

	return outBuf.String(), nil
}

// docxDocument represents the internal word/document.xml structure
type docxDocument struct {
	XMLName xml.Name `xml:"document"`
	Body    struct {
		Paragraphs []struct {
			Texts []string `xml:"r>t"`
		} `xml:"p"`
	} `xml:"body"`
}

// ExtractDocxText unzips the docx and extracts pure text from the XML natively
func ExtractDocxText(fileBytes []byte) (string, error) {
	reader, err := zip.NewReader(bytes.NewReader(fileBytes), int64(len(fileBytes)))
	if err != nil {
		return "", fmt.Errorf("failed to create zip reader: %w", err)
	}

	var docXML io.ReadCloser
	for _, f := range reader.File {
		if f.Name == "word/document.xml" {
			rc, err := f.Open()
			if err != nil {
				return "", fmt.Errorf("failed to open word/document.xml: %w", err)
			}
			docXML = rc
			break
		}
	}

	if docXML == nil {
		return "", fmt.Errorf("word/document.xml not found in docx")
	}
	defer func(docXML io.ReadCloser) {
		if err := docXML.Close(); err != nil {
			fmt.Printf("warning: failed to close word/document.xml: %v\n", err)
		}
	}(docXML)

	var doc docxDocument
	decoder := xml.NewDecoder(docXML)
	if err := decoder.Decode(&doc); err != nil {
		return "", fmt.Errorf("failed to decode docx xml: %w", err)
	}

	var buf bytes.Buffer
	for _, p := range doc.Body.Paragraphs {
		for _, t := range p.Texts {
			buf.WriteString(t)
		}
		buf.WriteString("\n")
	}

	return buf.String(), nil
}
