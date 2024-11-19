package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Entry struct {
	Key     string
	Value   string
	Expires *int64
}

func ReadKeyValue(r io.Reader) (Entry, error) {
	entry := Entry{}

	// Check for expiry (0xFC for milliseconds or 0xFD for seconds)
	var b byte
	if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
		return entry, err
	}
	if b == expireMilliSec || b == expireSec {
		expiry, err := readExpiry(r, b)
		if err != nil {
			return entry, err
		}
		entry.Expires = &expiry

		// Read the next byte to get the value type
		if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
			return entry, err
		}
	}

	// Read the value type (only handling 0x00 for strings here)
	if b != 0x00 {
		return entry, fmt.Errorf("unsupported value type: %x", b)
	}

	// Read the key
	key, err := ReadString(r)
	if err != nil {
		return entry, err
	}
	entry.Key = key

	// Read the value
	value, err := ReadString(r)
	if err != nil {
		return entry, err
	}
	entry.Value = value

	return entry, nil
}

func WriteKeyValue(w io.Writer, entry Entry) error {
	// Write expiry if it exists
	if entry.Expires != nil {
		if err := binary.Write(w, binary.LittleEndian, byte(expireMilliSec)); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, *entry.Expires); err != nil {
			return err
		}
	}

	// Write value type (0x00 for string)
	if err := binary.Write(w, binary.LittleEndian, byte(0x00)); err != nil {
		return err
	}

	// Write the key and value
	if err := WriteString(w, entry.Key); err != nil {
		return err
	}
	if err := WriteString(w, entry.Value); err != nil {
		return err
	}

	return nil
}

// readExpiry reads the expiry timestamp from the reader based on the encoding type.
func readExpiry(r io.Reader, encoding byte) (int64, error) {
	var expiry int64
	if encoding == expireMilliSec {
		// 8-byte expiry in milliseconds, little-endian
		if err := binary.Read(r, binary.LittleEndian, &expiry); err != nil {
			return 0, err
		}
	} else if encoding == expireSec {
		// 4-byte expiry in seconds, little-endian
		var expiry32 int32
		if err := binary.Read(r, binary.LittleEndian, &expiry32); err != nil {
			return 0, err
		}
		expiry = int64(expiry32)
	} else {
		return 0, fmt.Errorf("invalid expiry encoding: %x", encoding)
	}
	return expiry, nil
}
