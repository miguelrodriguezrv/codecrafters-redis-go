package store

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

type Type string

const (
	StringType Type = "string"
	StreamType Type = "stream"
)

type Item struct {
	value    []byte
	itemType Type
	expiry   int64
}

type InMemoryStore struct {
	items map[string]Item
	mu    sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	store := &InMemoryStore{
		items: make(map[string]Item, 0),
	}
	go store.cleanupExpiredItems()
	return store
}

func (s *InMemoryStore) Keys(pattern string) ([]string, error) {
	keys := make([]string, 0)
	for k := range s.items {
		ok, err := filepath.Match(pattern, k)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (s *InMemoryStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	item, ok := s.items[key]
	s.mu.RUnlock()
	if !ok || (item.expiry > 0 && time.Now().UnixMilli() > item.expiry) {
		return nil, false
	}
	return item.value, true
}

func (s *InMemoryStore) Set(key string, value []byte, expiry int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	expirationTime := int64(0)
	if expiry > 0 {
		expirationTime = time.Now().UnixMilli() + expiry
	}
	s.items[key] = Item{
		value:    value,
		itemType: StringType,
		expiry:   expirationTime,
	}
	return nil
}

func (s *InMemoryStore) Type(key string) string {
	s.mu.RLock()
	item, ok := s.items[key]
	s.mu.RUnlock()
	if !ok {
		return "none"
	}
	return string(item.itemType)
}

func (s *InMemoryStore) cleanupExpiredItems() {
	for {
		time.Sleep(time.Minute)
		s.mu.Lock()
		now := time.Now().UnixMilli()
		for key, item := range s.items {
			if item.expiry > 0 && now > item.expiry {
				delete(s.items, key)
			}
		}
		s.mu.Unlock()
	}
}

func (s *InMemoryStore) Load(entries []persistence.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		var expiry int64
		if entry.Expires != nil {
			expiry = *entry.Expires
		}
		s.items[entry.Key] = Item{
			value:  ([]byte(entry.Value)),
			expiry: expiry,
		}
	}
}

func (s *InMemoryStore) Export() []persistence.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]persistence.Entry, 0, len(s.items))
	for key, item := range s.items {
		entries = append(entries, persistence.Entry{
			Key:     key,
			Value:   string(item.value),
			Expires: &item.expiry,
		})
	}
	return entries
}
