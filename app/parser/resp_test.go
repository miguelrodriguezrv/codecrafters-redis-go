package parser

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    [][]byte
		wantErr error
	}{
		{
			name:    "empty input",
			input:   []byte{},
			want:    nil,
			wantErr: nil,
		},
		{
			name:    "invalid array start",
			input:   []byte("+invalid\r\n"),
			want:    nil,
			wantErr: ErrInvalidArrayAst,
		},
		{
			name:    "zero length array",
			input:   []byte("*0\r\n"),
			want:    nil,
			wantErr: nil,
		},
		{
			name:  "simple PING command",
			input: []byte("*1\r\n$4\r\nPING\r\n"),
			want:  [][]byte{[]byte("PING")},
		},
		{
			name:  "SET command with key and value",
			input: []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
			want: [][]byte{
				[]byte("SET"),
				[]byte("key"),
				[]byte("value"),
			},
		},
		{
			name:    "missing CRLF",
			input:   []byte("*1\n$4\r\nPING\r\n"),
			want:    nil,
			wantErr: ErrInvalidArrayCRLF,
		},
		{
			name:    "invalid bulk string marker",
			input:   []byte("*1\r\n#4\r\nPING\r\n"),
			want:    nil,
			wantErr: errors.New("expected '$', got '#'"),
		},
		{
			name:    "invalid array length",
			input:   []byte("*-1\r\n"),
			want:    nil,
			wantErr: errors.New("invalid array length: negative number not allowed"),
		},
		{
			name:    "incomplete array length",
			input:   []byte("*1"),
			want:    nil,
			wantErr: ErrIncomplete,
		},
		{
			name:    "incomplete bulk string length",
			input:   []byte("*1\r\n$"),
			want:    nil,
			wantErr: ErrIncomplete,
		},
		{
			name:    "incomplete bulk string content",
			input:   []byte("*1\r\n$4\r\nPI"),
			want:    nil,
			wantErr: ErrIncomplete,
		},
		{
			name:    "partial second argument",
			input:   []byte("*2\r\n$4\r\nPING\r\n$3\r\nfo"),
			want:    nil,
			wantErr: ErrIncomplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := ParseCommand(tt.input)

			// Check error
			if tt.wantErr != nil {
				if err == nil {
					t.Errorf("ParseCommand() error = nil, wantErr %v", tt.wantErr)
					return
				}
				if err.Error() != tt.wantErr.Error() {
					t.Errorf("ParseCommand() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			} else if err != nil {
				t.Errorf("ParseCommand() unexpected error = %v", err)
				return
			}

			// Check result
			if !compareByteSlices(got, tt.want) {
				t.Errorf("ParseCommand() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Helper function to compare two [][]byte
func compareByteSlices(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
