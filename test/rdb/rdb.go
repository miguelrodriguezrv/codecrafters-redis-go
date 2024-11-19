package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"os"
)

const (
	filename = "test.rdb"
)

func main() {
	var buf bytes.Buffer

	// Step 1: Write Header
	buf.Write([]byte("REDIS0011")) // RDB version 11

	// Step 2: Write Metadata
	buf.WriteByte(0xFA)            // Metadata start marker
	writeString(&buf, "redis-ver") // Metadata key
	writeString(&buf, "6.0.16")    // Metadata value

	// Step 3: Write Database Section
	buf.WriteByte(0xFE)       // Database start marker
	buf.WriteByte(0x00)       // Database index (0)
	buf.WriteByte(0xFB)       // Hash table size marker
	writeSizeEncoded(&buf, 2) // Number of keys in the hash table (size-encoded)
	writeSizeEncoded(&buf, 1) // Number of keys with an expiry (size-encoded)

	// Step 4: Write Key-Value Pairs
	// First key-value pair without expiry
	buf.WriteByte(0x00)         // Type flag for string
	writeString(&buf, "key1")   // Key
	writeString(&buf, "value1") // Value

	// Second key-value pair with expiry
	buf.WriteByte(0xFC)                                            // Expire marker (milliseconds)
	binary.Write(&buf, binary.LittleEndian, uint64(1713824559637)) // Expire timestamp
	buf.WriteByte(0x00)                                            // Type flag for string
	writeString(&buf, "key2")                                      // Key
	writeString(&buf, "value2")                                    // Value

	// Step 5: Write End of File Marker
	buf.WriteByte(0xFF)

	checksum := crc64.Checksum(buf.Bytes(), crc64.MakeTable(crc64.ISO))
	// Step 6: Write a dummy checksum
	if err := binary.Write(&buf, binary.BigEndian, checksum); err != nil {
		fmt.Println("Error getting checksum:", err)
		return
	}

	// Save to file
	err := os.WriteFile(filename, buf.Bytes(), 0644)
	if err != nil {
		fmt.Println("Error saving RDB file:", err)
		return
	}

	fmt.Println("RDB file created successfully ", filename)
}

// Helper function to write a size-encoded integer
func writeSizeEncoded(buf *bytes.Buffer, size int) {
	if size < 1<<6 { // 6-bit encoding
		buf.WriteByte(byte(size))
	} else if size < 1<<14 { // 14-bit encoding
		buf.WriteByte(byte(size>>8) | 0x40)
		buf.WriteByte(byte(size))
	} else { // 32-bit encoding
		buf.WriteByte(0x80)
		binary.Write(buf, binary.BigEndian, uint32(size))
	}
}

// Helper function to write a string with size encoding
func writeString(buf *bytes.Buffer, s string) {
	writeSizeEncoded(buf, len(s)) // Size-encoded length
	buf.WriteString(s)            // String data
}
