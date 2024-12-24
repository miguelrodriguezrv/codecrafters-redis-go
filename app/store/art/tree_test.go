package art_test

import (
	"bytes"
	"fmt"
	"testing"

	"math/rand/v2"

	"github.com/codecrafters-io/redis-starter-go/app/store/art"
)

func TestART_Basic(t *testing.T) {
	tree := art.NewART()

	tree.Insert([]byte("hello"), "world")
	value, exists := tree.Select([]byte("hello"))
	if !exists {
		t.Error("Key 'hello' should exist")
	}
	if value != "world" {
		t.Errorf("Expected 'world', got %v", value)
	}

	// Test non-existent key
	_, exists = tree.Select([]byte("nonexistent"))
	if exists {
		t.Error("Key 'nonexistent' should not exist")
	}
}

func TestART_PrefixHandling(t *testing.T) {
	tree := art.NewART()

	// Insert keys with common prefixes
	testCases := []struct {
		key   string
		value string
	}{
		{"test", "value1"},
		{"testing", "value2"},
		{"tested", "value3"},
		{"tests", "value4"},
	}

	// Insert all test cases
	for _, tc := range testCases {
		tree.Insert([]byte(tc.key), tc.value)
	}

	// Verify all insertions
	for _, tc := range testCases {
		value, exists := tree.Select([]byte(tc.key))
		if !exists {
			t.Errorf("Key '%s' should exist", tc.key)
		}
		if value != tc.value {
			t.Errorf("Expected '%s', got %v", tc.value, value)
		}
	}
}

func TestART_PrefixEdgeCases(t *testing.T) {
	tree := art.NewART()
	testCases := []struct {
		key   string
		value string
	}{
		{"a", "single"},       // Single character
		{"aa", "double"},      // Double character
		{"aaa", "triple"},     // Triple character
		{"aaaa", "quadruple"}, // Four characters
	}

	// Insert all test cases
	for _, tc := range testCases {
		tree.Insert([]byte(tc.key), tc.value)
	}

	// Verify all insertions
	for _, tc := range testCases {
		value, exists := tree.Select([]byte(tc.key))
		if !exists {
			t.Errorf("Key '%s' should exist", tc.key)
		}
		if value != tc.value {
			t.Errorf("Expected '%s', got %v", tc.value, value)
		}
	}
}

func TestART_NodeExpansion(t *testing.T) {
	tree := art.NewART()

	// Test Node4 to Node16 expansion
	t.Log("Testing Node4 to Node16 expansion")
	for i := byte(1); i < 6; i++ {
		tree.Insert([]byte{i}, i)
		value, exists := tree.Select([]byte{i})
		if !exists || value != i {
			t.Errorf("Failed at i=%d: got value=%v, exists=%v", i, value, exists)
		}
	}

	// Test Node16 to Node48 expansion
	t.Log("Testing Node16 to Node48 expansion")
	for i := byte(6); i < 18; i++ {
		tree.Insert([]byte{i}, i)
		value, exists := tree.Select([]byte{i})
		if !exists || value != i {
			t.Errorf("Failed at i=%d: got value=%v, exists=%v", i, value, exists)
		}
	}

	// Test Node48 to Node256 expansion
	t.Log("Testing Node48 to Node256 expansion")
	for i := byte(18); i < 50; i++ {
		tree.Insert([]byte{i}, i)
		value, exists := tree.Select([]byte{i})
		if !exists || value != i {
			t.Errorf("Failed at i=%d: got value=%v, exists=%v", i, value, exists)
		}
	}

	// Verify all values are still accessible
	for i := byte(1); i < 50; i++ {
		value, exists := tree.Select([]byte{i})
		if !exists {
			t.Errorf("Key %d should exist", i)
		}
		if value != i {
			t.Errorf("Expected %d, got %v", i, value)
		}
	}
}

func TestART_LongKeys(t *testing.T) {
	tree := art.NewART()
	longKey := bytes.Repeat([]byte("a"), 1000)
	tree.Insert(longKey, "long")

	value, exists := tree.Select(longKey)
	if !exists {
		t.Error("Long key should exist")
	}
	if value != "long" {
		t.Errorf("Expected 'long', got %v", value)
	}
}

func generateNonZeroRandomBytes(length int) []byte {
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = byte(rand.IntN(255) + 1)
	}
	return bytes
}

func TestART_RandomOperations(t *testing.T) {
	const (
		numOperations = 1000
		maxKeyLength  = 20
		minKeyLength  = 1
	)
	tree := art.NewART()

	// Generate random keys and values
	keyMap := make(map[string]bool)
	keys := make([][]byte, 0, 1000)
	values := make([]string, 0, 1000)
	for len(keys) < numOperations {
		key := generateNonZeroRandomBytes(rand.IntN(20) + 1)
		keyStr := string(key)
		if !keyMap[keyStr] {
			keyMap[keyStr] = true
			keys = append(keys, key)
			values = append(values, fmt.Sprintf("value%d", len(keys)-1))
		}
	}
	// Insert all unique keys
	for i := 0; i < len(keys); i++ {
		tree.Insert(keys[i], values[i])
		// Verify immediately after insertion
		value, exists := tree.Select(keys[i])
		if !exists {
			t.Errorf("Key %v should exist immediately after insertion", keys[i])
		}
		if value != values[i] {
			t.Errorf("Expected '%s', got %v immediately after insertion", values[i], value)
		}
	}
	// Verify all values are still accessible
	for i := 0; i < len(keys); i++ {
		value, exists := tree.Select(keys[i])
		if !exists {
			t.Errorf("Key %v should exist", keys[i])
		}
		if value != values[i] {
			t.Errorf("Expected '%s', got %v", values[i], value)
		}
	}
}

func TestART_SpecialCharacters(t *testing.T) {
	tree := art.NewART()
	testCases := []struct {
		key   string
		value string
	}{
		{"\n", "newline"},          // Newline
		{"\t", "tab"},              // Tab
		{"\x1f", "unit separator"}, // Unit separator
		{"∑", "summation"},         // Unicode
		{"🌟", "star"},              // Emoji
	}

	// Insert and verify special characters
	for _, tc := range testCases {
		tree.Insert([]byte(tc.key), tc.value)
		value, exists := tree.Select([]byte(tc.key))
		if !exists {
			t.Errorf("Key '%x' should exist", tc.key)
		}
		if value != tc.value {
			t.Errorf("Expected '%s', got %v", tc.value, value)
		}
	}
}

func TestART_PrefixOverlap(t *testing.T) {
	tree := art.NewART()
	testCases := []struct {
		key   string
		value string
	}{
		{"romane", "1"},
		{"romanus", "2"},
		{"romulus", "3"},
		{"rubens", "4"},
		{"ruber", "5"},
		{"rubicon", "6"},
		{"rubicundus", "7"},
	}

	// Insert all test cases
	for _, tc := range testCases {
		tree.Insert([]byte(tc.key), tc.value)
	}

	// Verify all values
	for _, tc := range testCases {
		value, exists := tree.Select([]byte(tc.key))
		if !exists {
			t.Errorf("Key '%s' should exist", tc.key)
		}
		if value != tc.value {
			t.Errorf("Expected '%s', got %v", tc.value, value)
		}
	}
}

func TestART_Range(t *testing.T) {
	tree := art.NewART()

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"apple", "1"},
		{"banana", "2"},
		{"cherry", "3"},
		{"date", "4"},
		{"elderberry", "5"},
		{"fig", "6"},
	}

	for _, td := range testData {
		tree.Insert([]byte(td.key), td.value)
	}

	// Test range queries
	tests := []struct {
		start    string
		end      string
		expected map[string]interface{}
	}{
		{
			start: "banana",
			end:   "elderberry",
			expected: map[string]interface{}{
				"banana":     "2",
				"cherry":     "3",
				"date":       "4",
				"elderberry": "5",
			},
		},
		{
			start: "apple",
			end:   "cherry",
			expected: map[string]interface{}{
				"apple":  "1",
				"banana": "2",
				"cherry": "3",
			},
		},
	}

	for _, test := range tests {
		result := tree.Range([]byte(test.start), []byte(test.end))

		if len(result) != len(test.expected) {
			t.Errorf("Expected %d results, got %d for range [%s, %s]",
				len(test.expected), len(result), test.start, test.end)
		}

		for k, v := range test.expected {
			if rv, exists := result[k]; !exists || rv != v {
				t.Errorf("Expected %s=%v in range [%s, %s], got %v",
					k, v, test.start, test.end, rv)
			}
		}
	}
}

func BenchmarkART_Insert(b *testing.B) {
	tree := art.NewART()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark-key-%d", i)
		tree.Insert([]byte(key), i)
	}
}

func BenchmarkART_Select(b *testing.B) {
	tree := art.NewART()
	// Pre-populate tree
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("benchmark-key-%d", i)
		tree.Insert([]byte(key), i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark-key-%d", i%1000)
		tree.Select([]byte(key))
	}
}
