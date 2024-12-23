package store

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/store/art"
)

type StreamValue struct {
	tree *art.ART
}

func (_ StreamValue) Type() Type {
	return StreamType
}

func (s *InMemoryStore) SetStream(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = Item{
		value: StreamValue{
			tree: art.NewART(),
		},
	}
	return nil
}

func (s *InMemoryStore) AddStreamEntry(key string, entryID []byte, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.items[key]
	switch item.value.Type() {
	case StreamType:
		stream := item.value.(StreamValue)
		stream.tree.Insert(entryID, value)
	default:
		return fmt.Errorf("Invalid type for key %s - %v", key, item.value.Type())
	}
	return nil
}
