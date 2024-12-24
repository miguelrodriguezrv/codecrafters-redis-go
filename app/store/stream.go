package store

import (
	"errors"
	"fmt"
	"log"
	"sort"
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

type StreamEntry struct {
	ID    string
	Value []KeyVal
}

type KeyVal struct {
	Key   string
	Value string
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

func (s *InMemoryStore) AddStreamEntry(key string, entryID []byte, fields []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return "", fmt.Errorf("ERR stream key %s does not exist", key)
	}

	if len(fields)%2 != 0 {
		return "", errors.New("ERR wrong number of arguments for XADD")
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
		stream.tree.Insert([]byte(strEntryID), fields)
		stream.setLastEntryID(EIDTimestamp, EIDSequence)
		return strEntryID, nil
	default:
		return "", fmt.Errorf("ERR Invalid type for key %s - %v", key, item.value.Type())
	}
}

func (s *InMemoryStore) Range(key string, start, end []byte) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[key]
	if !ok {
		return nil
	}

	switch item.value.Type() {
	case StreamType:
		stream := item.value.(*StreamValue)

		startID := string(start)
		endID := string(end)
		// Convert "-" to minimum possible ID
		if startID == "-" {
			startID = "0-0"
		}

		// Convert "+" to maximum possible ID
		if endID == "+" {
			endID = "9223372036854775807-9223372036854775807" // Max int64-int64
		}

		// Get range from the ART
		entries := stream.tree.Range([]byte(startID), []byte(endID))

		// Convert to array of entries
		result := make([]StreamEntry, 0, len(entries))
		for id, value := range entries {
			keyVals, err := parseStreamValue(value)
			if err != nil {
				log.Printf("Error parsing stream value: %v", err)
				continue
			}
			result = append(result, StreamEntry{
				ID:    id,
				Value: keyVals,
			})
		}

		// Sort entries by ID
		sort.Slice(result, func(i, j int) bool {
			return result[i].ID < result[j].ID
		})
		return result
	}
	return nil
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

func parseStreamValue(value interface{}) ([]KeyVal, error) {
	strSlice, ok := value.([]string)
	if !ok {
		return nil, errors.New("ERR Invalid stream value format")
	}

	if len(strSlice)%2 != 0 {
		return nil, errors.New("ERR Stream value must have even number of elements")
	}

	result := make([]KeyVal, 0, len(strSlice)/2)
	for i := 0; i < len(strSlice); i += 2 {
		result = append(result, KeyVal{
			Key:   strSlice[i],
			Value: strSlice[i+1],
		})
	}
	return result, nil
}
