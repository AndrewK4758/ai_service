package util

import (
	"testing"
)

func TestCleanJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean json",
			input:    `{"key": "value"}`,
			expected: `{"key": "value"}`,
		},
		{
			name: "markdown json block",
			input: `Here is the output:
` + "```json\n" + `{"key": "value"}
` + "```" + `
Have a nice day!`,
			expected: `{"key": "value"}`,
		},
		{
			name: "markdown generic block",
			input: "```\n" + `{"key": "value"}` + "\n```",
			expected: `{"key": "value"}`,
		},
		{
			name: "extraneous text",
			input: `Sure, here is your json: {"a": 1, "b": [2, 3]} Thank you!`,
			expected: `{"a": 1, "b": [2, 3]}`,
		},
		{
			name: "array top level",
			input: "```json\n[\n  {\"a\": 1},\n  {\"b\": 2}\n]\n```",
			expected: "[\n  {\"a\": 1},\n  {\"b\": 2}\n]",
		},
		{
			name:     "no braces",
			input:    `just some text`,
			expected: `just some text`, // unmodified if no braces found
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := CleanJSON(tc.input)
			if result != tc.expected {
				t.Errorf("expected:\n%s\ngot:\n%s", tc.expected, result)
			}
		})
	}
}
