package store_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func TestStore_SetAndGet(t *testing.T) {
	IMstore := store.NewInMemoryStore()

	// Test basic set and get
	err := IMstore.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Errorf("Failed to set value: %v", err)
	}

	value, exists := IMstore.Get("key1")
	if !exists {
		t.Error("Expected key to exist, but it doesn't")
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}

	// Test non-existent key
	_, exists = IMstore.Get("nonexistent")
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
	IMstore := store.NewInMemoryStore()

	// Set initial value
	err := IMstore.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Errorf("Failed to set initial value: %v", err)
	}

	// Overwrite value
	err = IMstore.Set("key1", []byte("value2"), 0)
	if err != nil {
		t.Errorf("Failed to overwrite value: %v", err)
	}

	value, exists := IMstore.Get("key1")
	if !exists {
		t.Error("Expected key to exist")
	}
	if string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	IMstore := store.NewInMemoryStore()
	done := make(chan bool)

	// Concurrent writes
	go func() {
		for i := 0; i < 100; i++ {
			IMstore.Set("key", []byte("value1"), 0)
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 100; i++ {
			IMstore.Get("key")
		}
		done <- true
	}()

	// Wait for both goroutines to finish
	<-done
	<-done
}

func TestStreamRange(t *testing.T) {
	IMstore := store.NewInMemoryStore()

	// Create stream
	err := IMstore.SetStream("mystream")
	if err != nil {
		t.Fatal(err)
	}

	// Add entries with field-value pairs
	entries := []struct {
		id     string
		fields []string
	}{
		{
			"1000-0",
			[]string{"name", "John", "age", "30"},
		},
		{
			"1001-0",
			[]string{"name", "Jane", "age", "25"},
		},
	}

	for _, entry := range entries {
		_, err := IMstore.AddStreamEntry("mystream", []byte(entry.id), entry.fields)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test range query
	streamEntries := IMstore.Range("mystream", []byte("-"), []byte("+"))

	if len(streamEntries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(streamEntries))
	}

	// Verify first entry
	firstEntry := streamEntries[0]
	if firstEntry.ID != "1000-0" {
		t.Errorf("Expected ID 1000-0, got %s", firstEntry.ID)
	}

	expectedKeyVals := []store.KeyVal{
		{Key: "name", Value: "John"},
		{Key: "age", Value: "30"},
	}

	if !reflect.DeepEqual(firstEntry.Value, expectedKeyVals) {
		t.Errorf("Expected KeyVals %v, got %v", expectedKeyVals, firstEntry.Value)
	}
}
