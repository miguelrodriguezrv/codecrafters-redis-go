package main

type Store struct {
	items map[string][]byte
}

func NewStore() *Store {
	return &Store{
		items: make(map[string][]byte, 0),
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	value, ok := s.items[key]
	return value, ok
}

func (s *Store) Set(key string, value []byte) error {
	s.items[key] = value
	return nil
}
