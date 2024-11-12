package store

type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, expiry int64) error
}
