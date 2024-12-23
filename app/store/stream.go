package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

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

func (s *StreamValue) validateEntryID(entryID []byte) error {
	entry := strings.Split(string(entryID), "-")
	if len(entry) != 2 {
		return errors.New("ERR Invalid EntryID format")
	}
	entryTimestamp, err := strconv.ParseInt(entry[0], 10, 64)
	if err != nil {
		return errors.New("ERR Invalid EntryID format")
	}
	entrySequence, err := strconv.ParseInt(entry[1], 10, 64)
	if err != nil {
		return errors.New("ERR Invalid EntryID format")
	}

	if entryTimestamp <= 0 && entrySequence <= 0 {
		return errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}
	if entryTimestamp < s.lastEntryIDTimestamp ||
		(entryTimestamp == s.lastEntryIDTimestamp && entrySequence <= s.lastEntryIDSequence) {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return nil
}

func (s *StreamValue) setLastEntryID(entryID []byte) {
	entry := strings.Split(string(entryID), "-")
	// No need to do sanity checks since this entryID passed ValidateEntryID
	entryTimestamp, _ := strconv.ParseInt(entry[0], 10, 64)
	entrySequence, _ := strconv.ParseInt(entry[1], 10, 64)
	s.lastEntryIDTimestamp = entryTimestamp
	s.lastEntryIDSequence = entrySequence
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

func (s *InMemoryStore) AddStreamEntry(key string, entryID []byte, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok {
		return fmt.Errorf("Stream key %s does not exist", key)
	}
	switch item.value.Type() {
	case StreamType:
		stream := item.value.(*StreamValue)
		if err := stream.validateEntryID(entryID); err != nil {
			return err
		}
		stream.tree.Insert(entryID, value)
		stream.setLastEntryID(entryID)
	default:
		return fmt.Errorf("Invalid type for key %s - %v", key, item.value.Type())
	}
	return nil
}
