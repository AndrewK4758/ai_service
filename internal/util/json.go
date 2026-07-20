package util

import (
	"strings"
)

// CleanJSON prepares an AI-generated string for JSON parsing (e.g. before converting to structpb.Struct).
// It strips markdown formatting (e.g. ```json ... ```) and extraneous conversational text.
func CleanJSON(input string) string {
	input = strings.TrimSpace(input)

	// Attempt to extract content inside markdown blocks
	if start := strings.Index(input, "```"); start != -1 {
		if end := strings.LastIndex(input, "```"); end > start+2 {
			content := input[start+3 : end]
			content = strings.TrimSpace(content)
			if strings.HasPrefix(strings.ToLower(content), "json") {
				content = strings.TrimSpace(content[4:])
			}
			input = content
		}
	}

	// Heuristic: isolate the JSON object or array to ignore conversational preamble/postamble
	firstBrace := strings.Index(input, "{")
	firstBracket := strings.Index(input, "[")

	first := -1
	if firstBrace != -1 && firstBracket != -1 {
		if firstBrace < firstBracket {
			first = firstBrace
		} else {
			first = firstBracket
		}
	} else if firstBrace != -1 {
		first = firstBrace
	} else if firstBracket != -1 {
		first = firstBracket
	}

	if first == -1 {
		return input // Couldn't find a JSON start, return as is
	}

	lastBrace := strings.LastIndex(input, "}")
	lastBracket := strings.LastIndex(input, "]")

	last := -1
	if lastBrace != -1 && lastBracket != -1 {
		if lastBrace > lastBracket {
			last = lastBrace
		} else {
			last = lastBracket
		}
	} else if lastBrace != -1 {
		last = lastBrace
	} else if lastBracket != -1 {
		last = lastBracket
	}

	if last != -1 && last > first {
		return input[first : last+1]
	}

	return input
}
