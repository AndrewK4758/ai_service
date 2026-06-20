package parser

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"io"

	"github.com/ledongthuc/pdf"
)

// ExtractPdfText uses a pure Go library to read raw text from a PDF
func ExtractPdfText(fileBytes []byte) (string, error) {
	reader := bytes.NewReader(fileBytes)
	r, err := pdf.NewReader(reader, int64(len(fileBytes)))
	if err != nil {
		return "", err
	}

	b, err := r.GetPlainText()
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(b); err != nil {
		return "", err
	}

	return buf.String(), nil
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
		return "", err
	}

	var docXML io.ReadCloser
	for _, f := range reader.File {
		if f.Name == "word/document.xml" {
			rc, err := f.Open()
			if err != nil {
				return "", err
			}
			docXML = rc
			break
		}
	}

	if docXML == nil {
		return "", errors.New("word/document.xml not found in docx")
	}
	defer func(docXML io.ReadCloser) {
		err := docXML.Close()
		if err != nil {

		}
	}(docXML)

	var doc docxDocument
	decoder := xml.NewDecoder(docXML)
	if err := decoder.Decode(&doc); err != nil {
		return "", err
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
