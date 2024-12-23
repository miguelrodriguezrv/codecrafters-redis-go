package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/store/art"
)

type StreamValue struct {
	tree                 *art.ART
	lastEntryIDTimestamp int64
	lastEntryIDSequence  int64
}

func (_ StreamValue) Type() Type {
	return StreamType
}

func (s *StreamValue) setLastEntryID(timestamp, sequence int64) {
	s.lastEntryIDTimestamp = timestamp
	s.lastEntryIDSequence = sequence
}

func (s *InMemoryStore) SetStream(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = Item{
		value: &StreamValue{
			tree:                 art.NewART(),
			lastEntryIDTimestamp: 0,
			lastEntryIDSequence:  0,
		},
	}
	return nil
}

func (s *InMemoryStore) AddStreamEntry(key string, entryID []byte, value interface{}) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok {
		return "", fmt.Errorf("Stream key %s does not exist", key)
	}
	switch item.value.Type() {
	case StreamType:
		stream := item.value.(*StreamValue)
		EIDTimestamp, EIDSequence, err := stream.parseEntryID(entryID)
		if err != nil {
			return "", err
		}
		if err := stream.validateEntryID(EIDTimestamp, EIDSequence); err != nil {
			return "", err
		}
		strEntryID := fmt.Sprintf("%d-%d", EIDTimestamp, EIDSequence)
		stream.tree.Insert([]byte(strEntryID), value)
		stream.setLastEntryID(EIDTimestamp, EIDSequence)
		return strEntryID, nil
	default:
		return "", fmt.Errorf("Invalid type for key %s - %v", key, item.value.Type())
	}
}

func (s *StreamValue) parseEntryID(entryID []byte) (timestamp int64, sequence int64, err error) {
	if string(entryID) == "*" {
		timestamp = time.Now().UnixMilli()
		sequence = s.generateEntryIDSequence(timestamp)
	} else {
		entry := strings.Split(string(entryID), "-")
		if len(entry) != 2 {
			return 0, 0, errors.New("ERR Invalid EntryID format")
		}

		timestamp, err = strconv.ParseInt(entry[0], 10, 64)
		if err != nil {
			return 0, 0, errors.New("ERR Invalid EntryID format")
		}

		if entry[1] == "*" {
			sequence = s.generateEntryIDSequence(timestamp)
		} else {
			sequence, err = strconv.ParseInt(entry[1], 10, 64)
			if err != nil {
				return 0, 0, errors.New("ERR Invalid EntryID format")
			}
		}
	}
	return timestamp, sequence, err
}

func (s *StreamValue) generateEntryIDSequence(timestamp int64) (sequence int64) {
	if timestamp == s.lastEntryIDTimestamp {
		sequence = s.lastEntryIDSequence + 1
	}
	if timestamp == 0 {
		sequence = 1
	}
	return sequence
}

func (s *StreamValue) validateEntryID(timestamp, sequence int64) error {
	if timestamp <= 0 && sequence <= 0 {
		return errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}
	if timestamp < s.lastEntryIDTimestamp ||
		(timestamp == s.lastEntryIDTimestamp && sequence <= s.lastEntryIDSequence) {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return nil
}
