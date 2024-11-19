package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Database struct {
	Index   int
	Entries []Entry
}

func ReadDatabaseSection(r io.Reader) (*Database, error) {
	entries := []Entry{}

	// Read database index
	index, err := ReadSize(r)
	if err != nil {
		return nil, err
	}

	// log.Printf("%x\n", index)
	// Read hash table
	var hashTableByte byte
	if err := binary.Read(r, binary.LittleEndian, &hashTableByte); err != nil {
		return nil, err
	}
	if hashTableByte != hashTableStart {
		return nil, fmt.Errorf("invalid start of hash table section: %x", hashTableByte)
	}

	var kvSize, expireSize int
	kvSize, err = ReadSize(r)
	if err != nil {
		return nil, err
	}
	expireSize, err = ReadSize(r)
	if err != nil {
		return nil, err
	}

	for keyValues, expiries := 0, 0; keyValues+expiries < kvSize+expireSize; keyValues++ {
		entry, err := ReadKeyValue(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
		if entry.Expires != nil {
			expiries++
		}
	}
	return &Database{
		Index:   index,
		Entries: entries,
	}, nil
}

func SaveDatabaseSection(w io.Writer, db *Database) error {
	// Write database index
	if err := WriteSize(w, db.Index); err != nil {
		return err
	}

	// Write hash table marker
	if err := binary.Write(w, binary.LittleEndian, byte(hashTableStart)); err != nil {
		return err
	}

	// Count key-value pairs and expiry entries
	kvSize := len(db.Entries)
	expireSize := 0
	for _, entry := range db.Entries {
		if entry.Expires != nil {
			expireSize++
		}
	}

	// Write key-value size
	if err := WriteSize(w, kvSize); err != nil {
		return err
	}

	// Write expire size
	if err := WriteSize(w, expireSize); err != nil {
		return err
	}

	// Write all entries
	for _, entry := range db.Entries {
		if err := WriteKeyValue(w, entry); err != nil {
			return err
		}
	}

	return nil
}
