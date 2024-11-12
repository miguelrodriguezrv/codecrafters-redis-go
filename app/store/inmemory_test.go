package store_test

import (
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func TestStore_SetAndGet(t *testing.T) {
	store := store.NewInMemoryStore()

	// Test basic set and get
	err := store.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Errorf("Failed to set value: %v", err)
	}

	value, exists := store.Get("key1")
	if !exists {
		t.Error("Expected key to exist, but it doesn't")
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}

	// Test non-existent key
	_, exists = store.Get("nonexistent")
	if exists {
		t.Error("Expected key to not exist, but it does")
	}
}

func TestStore_Expiration(t *testing.T) {
	store := store.NewInMemoryStore()

	// Test item with expiration
	err := store.Set("expiring", []byte("temp"), 100) // 100ms expiration
	if err != nil {
		t.Errorf("Failed to set value: %v", err)
	}

	// Should exist immediately
	value, exists := store.Get("expiring")
	if !exists {
		t.Error("Expected key to exist immediately after setting")
	}
	if string(value) != "temp" {
		t.Errorf("Expected temp, got %s", string(value))
	}

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Should not exist after expiration
	_, exists = store.Get("expiring")
	if exists {
		t.Error("Expected key to be expired, but it still exists")
	}
}

func TestStore_Overwrite(t *testing.T) {
	store := store.NewInMemoryStore()

	// Set initial value
	err := store.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Errorf("Failed to set initial value: %v", err)
	}

	// Overwrite value
	err = store.Set("key1", []byte("value2"), 0)
	if err != nil {
		t.Errorf("Failed to overwrite value: %v", err)
	}

	value, exists := store.Get("key1")
	if !exists {
		t.Error("Expected key to exist")
	}
	if string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store := store.NewInMemoryStore()
	done := make(chan bool)

	// Concurrent writes
	go func() {
		for i := 0; i < 100; i++ {
			store.Set("key", []byte("value1"), 0)
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 100; i++ {
			store.Get("key")
		}
		done <- true
	}()

	// Wait for both goroutines to finish
	<-done
	<-done
}
