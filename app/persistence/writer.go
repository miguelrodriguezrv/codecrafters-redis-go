package persistence

import (
	"encoding/binary"
	"io"
)

func WriteHeader(w io.Writer) error {
	_, err := w.Write([]byte(header))
	return err
}

func WriteSize(w io.Writer, size int) error {
	// For values 0-63 (6 bits)
	if size < 64 {
		return binary.Write(w, binary.LittleEndian, uint8(size))
	}

	// For values 64-16383 (14 bits)
	if size < 16384 {
		// Use 0b01 prefix and split into two bytes
		firstByte := uint8(0x40 | (size >> 8)) // 0x40 is 0b01000000
		secondByte := uint8(size & 0xFF)
		if err := binary.Write(w, binary.LittleEndian, firstByte); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, secondByte)
	}

	// For values >= 16384 (32 bits)
	// Use 0b10 prefix
	firstByte := uint8(0x80) // 0x80 is 0b10000000
	if err := binary.Write(w, binary.LittleEndian, firstByte); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, uint32(size))
}

// WriteString writes a size-encoded string to the writer.
func WriteString(w io.Writer, s string) error {
	// Write the size first
	if err := WriteSize(w, len(s)); err != nil {
		return err
	}

	// Write the string contents
	_, err := w.Write([]byte(s))
	return err
}
