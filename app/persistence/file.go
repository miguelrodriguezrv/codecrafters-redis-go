package persistence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/codecrafters-io/redis-starter-go/app/persistence/crc64"
)

const (
	headerMagic      = "REDIS"
	versionNumber    = "0011"
	header           = headerMagic + versionNumber
	metadataStart    = 0xFA
	hashTableStart   = 0xFB
	expireMilliSec   = 0xFC
	expireSec        = 0xFD
	databaseStart    = 0xFE
	endOfFileSection = 0xFF
)

// LoadRDB reads an entire RDB file and returns its entries.
func LoadRDB(r io.Reader) ([]*Database, error) {
	// Verify the header
	if err := ReadHeader(r); err != nil {
		return nil, err
	}
	// Read the metadata section
	_, r, err := ReadMetadata(r)
	if err != nil {
		return nil, err
	}

	// Read the database section(s)
	databases := []*Database{}
	for {
		startByte := make([]byte, 1)
		if _, err := r.Read(startByte); err != nil {
			return nil, err
		}

		if startByte[0] == endOfFileSection {
			break
		} else if startByte[0] == databaseStart {
			database, err := ReadDatabaseSection(r)
			if err != nil {
				return nil, err
			}
			databases = append(databases, database)
		} else {
			return nil, fmt.Errorf("unexpected byte: %x", startByte[0])
		}
	}

	return databases, nil
}

func SaveRDB(dir, dbFilename string, databases []*Database) error {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	file, err := os.Create(path.Join(dir, dbFilename))
	if err != nil {
		return err
	}
	defer file.Close()

	if err := WriteHeader(file); err != nil {
		return err
	}
	file.Write([]byte{databaseStart})
	for _, value := range databases {
		err := SaveDatabaseSection(file, value)
		if err != nil {
			return err
		}
	}

	// Write end of file indicator and checksum
	file.Write([]byte{endOfFileSection})
	checksum, err := GetFileChecksum(file)
	if err != nil {
		return err
	}
	if _, err := file.Write(checksum); err != nil {
		return err
	}
	return nil
}

func GetFileChecksum(file *os.File) ([]byte, error) {
	fileStat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	// Seek back to start
	file.Seek(0, 0)

	// Read the entire file content
	content := make([]byte, fileStat.Size())
	if _, err := file.Read(content); err != nil {
		return nil, err
	}

	checksum := crc64.Digest(content)
	checksumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(checksumBytes, checksum)
	return checksumBytes, nil
}

// VerifyChecksum reads and verifies the final checksum in the RDB file.
func VerifyChecksum(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if len(data) < 10 {
		return errors.New("file too short to contain a checksum")
	}

	content := data[:len(data)-8]

	// Compute the CRC64 checksum of the content
	computedChecksum := crc64.Digest(content)
	storedChecksum := binary.LittleEndian.Uint64(data[len(data)-8:])

	if computedChecksum != storedChecksum {
		return errors.New("CRC64 Checksum failed")
	}
	return nil
}
