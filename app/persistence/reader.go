package persistence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// ReadSize decodes a size-encoded integer from the reader.
func ReadSize(r io.Reader) (int, error) {
	var b byte
	if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
		return 0, err
	}

	switch b >> 6 {
	case 0b00:
		return int(b & 0x3F), nil
	case 0b01:
		var nextByte byte
		if err := binary.Read(r, binary.BigEndian, &nextByte); err != nil {
			return 0, err
		}
		return int(b&0x3F)<<8 | int(nextByte), nil
	case 0b10:
		var size uint32
		if err := binary.Read(r, binary.BigEndian, &size); err != nil {
			return 0, err
		}
		return int(size), nil
	case 0b11:
		switch b {
		case 0xC0:
			return 1, nil
		case 0xC1:
			return 2, nil
		case 0xC2:
			return 4, nil
		case 0xC3:
			return 0, fmt.Errorf("found an LZF encoding. Unimplemented.")
		default:
			return 0, fmt.Errorf("unsupported string size encoding")
		}
	default:
		return 0, fmt.Errorf("unsupported size encoding")
	}
}

func ReadHeader(r io.Reader) error {
	buf := make([]byte, len(header))
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	if !bytes.Equal(buf, []byte(header)) {
		return fmt.Errorf("invalid RDB header: %s", string(buf))
	}
	return nil
}

func ReadMetadata(r io.Reader) (map[string]string, io.Reader, error) {
	metadata := make(map[string]string)
	for {
		var startByte byte
		if err := binary.Read(r, binary.LittleEndian, &startByte); err != nil {
			return nil, r, err
		}
		if startByte == databaseStart || startByte == endOfFileSection {
			r = io.MultiReader(bytes.NewReader([]byte{startByte}), r) // reinsert start byte
			break
		} else if startByte == metadataStart {
			key, err := ReadString(r)
			if err != nil {
				return nil, r, err
			}
			value, err := ReadString(r)
			if err != nil {
				return nil, r, err
			}
			metadata[key] = value
		} else {
			return nil, r, fmt.Errorf("unexpected byte in metadata section: %x", startByte)
		}
	}
	return metadata, r, nil
}

// ReadString reads a size-encoded string from the reader.
func ReadString(r io.Reader) (string, error) {
	size, err := ReadSize(r)
	if err != nil {
		return "", err
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}
