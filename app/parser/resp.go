package parser

import (
	"errors"
	"strconv"
	"strings"
)

type Type byte

const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

var ErrInvalidArrayAst = errors.New("invalid array, expected *")
var ErrInvalidArrayCRLF = errors.New("invalid array, expected \\r\\n")
var ErrIncomplete = errors.New("incomplete command")

func ParseCommand(packet []byte) ([][]byte, []byte, error) {
	if len(packet) == 0 {
		return nil, nil, nil
	}
	if packet[0] != Array {
		return nil, packet, ErrInvalidArrayAst
	}
	args := make([][]byte, 0)
	for i := 1; i < len(packet); i++ {
		if packet[i] == '\n' {
			if packet[i-1] != '\r' {
				return nil, packet, ErrInvalidArrayCRLF
			}
			count, err := strconv.Atoi(string(packet[1 : i-1]))
			if err != nil {
				return nil, packet, errors.New("invalid bulk count: '" + string(packet[1:i-1]) + "' - " + err.Error())
			}
			if count < 0 {
				return nil, packet, errors.New("invalid array length: negative number not allowed")
			}
			if count == 0 {
				return nil, packet[i:], nil
			}
			i++
		nextArg:
			for j := 0; j < count; j++ {
				if i >= len(packet) {
					return nil, packet, ErrIncomplete
				}
				if packet[i] != '$' {
					return nil, packet, errors.New("expected '$', got '" + string(packet[i]) + "'")
				}
				for s := i + 1; i < len(packet); i++ {
					if packet[i] == '\n' {
						if packet[i-1] != '\r' {
							return nil, packet, ErrInvalidArrayCRLF
						}
						n, err := strconv.Atoi(string(packet[s : i-1]))
						if err != nil || count < 0 {
							return nil, packet, errors.New("Invalid bulk count: '" + string(packet[1:i-1]) + "' - " + err.Error())
						}
						i++
						if len(packet)-i >= n+2 {
							if packet[i+n] != '\r' || packet[i+n+1] != '\n' {
								return nil, packet, ErrInvalidArrayCRLF
							}
							args = append(args, packet[i:i+n])
							i += n + 2
							if j == count-1 {
								return args, packet[i:], nil
							}
							continue nextArg
						}
						return nil, packet, ErrIncomplete
					}
				}
				return nil, packet, ErrIncomplete
			}
		}
	}
	return nil, packet, ErrIncomplete
}

// appendPrefix will append a "$3\r\n" style redis prefix for a message.
func appendPrefix(b []byte, c byte, n int64) []byte {
	if n >= 0 && n <= 9 {
		return append(b, c, byte('0'+n), '\r', '\n')
	}
	b = append(b, c)
	b = strconv.AppendInt(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendUint appends a Redis protocol uint64 to the input bytes.
func AppendUint(b []byte, n uint64) []byte {
	b = append(b, ':')
	b = strconv.AppendUint(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendInt appends a Redis protocol int64 to the input bytes.
func AppendInt(b []byte, n int64) []byte {
	return appendPrefix(b, ':', n)
}

// AppendArray appends a Redis protocol array to the input bytes.
func AppendArray(b []byte, n int) []byte {
	return appendPrefix(b, '*', int64(n))
}

// AppendBulk appends a Redis protocol bulk byte slice to the input bytes.
func AppendBulk(b []byte, bulk []byte) []byte {
	b = appendPrefix(b, '$', int64(len(bulk)))
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendBulkString appends a Redis protocol bulk string to the input bytes.
func AppendBulkString(b []byte, bulk string) []byte {
	b = appendPrefix(b, '$', int64(len(bulk)))
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendString appends a Redis protocol string to the input bytes.
func AppendString(b []byte, s string) []byte {
	b = append(b, '+')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendError appends a Redis protocol error to the input bytes.
func AppendError(b []byte, s string) []byte {
	b = append(b, '-')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendOK appends a Redis protocol OK to the input bytes.
func OK() []byte {
	return []byte("+OK\r\n")
}

func NullBulkString() []byte {
	return []byte("$-1\r\n")
}

func NullArray() []byte {
	return []byte("*-1\r\n")
}

func stripNewlines(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' {
			s = strings.Replace(s, "\r", " ", -1)
			s = strings.Replace(s, "\n", " ", -1)
			break
		}
	}
	return s
}

func EncodeStringArray(args ...string) []byte {
	b := AppendArray(nil, len(args))
	for _, str := range args {
		b = AppendBulkString(b, str)
	}
	return b
}
