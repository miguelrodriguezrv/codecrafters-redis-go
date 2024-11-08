package main

import (
	"sync"
	"time"
)

type Item struct {
	value  []byte
	expiry int64
}

type Store struct {
	items map[string]Item
	mu    sync.RWMutex
}

func NewStore() *Store {
	store := &Store{
		items: make(map[string]Item, 0),
	}
	go store.cleanupExpiredItems()
	return store
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	item, ok := s.items[key]
	s.mu.RUnlock()
	if !ok || (item.expiry > 0 && time.Now().UnixMilli() > item.expiry) {
		return nil, false
	}
	return item.value, true
}

func (s *Store) Set(key string, value []byte, expiry int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	expirationTime := int64(0)
	if expiry > 0 {
		expirationTime = time.Now().UnixMilli() + expiry
	}
	s.items[key] = Item{
		value:  value,
		expiry: expirationTime,
	}
	return nil
}

func (s *Store) cleanupExpiredItems() {
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
