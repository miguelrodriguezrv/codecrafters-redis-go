package server

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func (s *Server) handleXAdd(req [][]byte) []byte {
	if len(req) < 5 {
		log.Println("Not enough arguments for XADD")
		return parser.AppendError(nil, "Not enough arguments for XADD")
	}
	key := string(req[1])
	switch s.stores[0].Type(key) {
	case "string":
		log.Printf("XADD for key %s - Already exists as string type", key)
		return parser.AppendError(nil, "1")
	case "none":
		err := s.stores[0].SetStream(key)
		if err != nil {
			return parser.AppendError(nil, "1")
		}
	}
	entryID := req[2]

	entryValues := make([]string, (len(req)-3)%2)
	for i := 3; i < len(req); i++ {
		entryValues = append(entryValues, string(req[i]))
	}

	newEntryID, err := s.stores[0].AddStreamEntry(key, entryID, entryValues)
	if err != nil {
		return parser.AppendError(nil, err.Error())
	}
	return parser.AppendBulkString(nil, string(newEntryID))
}

func (s *Server) handleXRange(req [][]byte) []byte {
	if len(req) < 4 {
		log.Println("Not enough arguments for XRANGE")
		return parser.AppendError(nil, "ERR Not enough arguments for XRANGE")
	}
	key := string(req[1])
	if s.stores[0].Type(key) != "stream" {
		return parser.AppendError(nil, "ERR key is not a stream")
	}
	entries := s.stores[0].Range(key, req[2], req[3])

	response := parser.AppendArray(nil, len(entries))
	for _, entry := range entries {
		response = parser.AppendArray(response, 2)
		response = parser.AppendBulkString(response, entry.ID)
		response = parser.AppendArray(response, len(entry.Value)*2)
		for _, kv := range entry.Value {
			response = parser.AppendBulkString(response, kv.Key)
			response = parser.AppendBulkString(response, kv.Value)
		}
	}
	return response
}

func (s *Server) handleXRead(req [][]byte) []byte {
	if len(req) < 4 {
		log.Println("Not enough arguments for XREAD")
		return parser.AppendError(nil, "ERR Not enough arguments for XREAD")
	}

	// Parse BLOCK option if present
	var blockMillis int64 = -1 // -1 means non-blocking
	for i := 1; i < len(req); i++ {
		if strings.ToUpper(string(req[i])) == "BLOCK" {
			if i+1 >= len(req) {
				return parser.AppendError(nil, "ERR syntax error")
			}
			var err error
			blockMillis, err = strconv.ParseInt(string(req[i+1]), 10, 64)
			if err != nil {
				return parser.AppendError(nil, "ERR invalid BLOCK timeout")
			}
			break
		}
	}

	// Find STREAMS keyword position
	streamsPos := -1
	for i, arg := range req {
		if strings.ToUpper(string(arg)) == "STREAMS" {
			streamsPos = i
			break
		}
	}
	if streamsPos == -1 {
		return parser.AppendError(nil, "ERR syntax error")
	}

	streamKeys := req[streamsPos+1 : (len(req)+streamsPos+1)/2]
	streamIDs := req[(len(req)+streamsPos+1)/2:]

	if len(streamKeys) != len(streamIDs) {
		return parser.AppendError(nil, "ERR Unbalanced STREAMS list")
	}

	// Set up deadline for blocking
	var deadline time.Time
	if blockMillis > 0 {
		deadline = time.Now().Add(time.Duration(blockMillis) * time.Millisecond)
	}

	// Check for $ IDs
	for i, keyBytes := range streamKeys {
		key := string(keyBytes)
		startID := streamIDs[i]
		if s.stores[0].Type(key) != "stream" {
			return parser.AppendError(nil, "ERR key is not a stream")
		}

		if string(startID) == "$" {
			var err error
			startID, err = s.stores[0].GetStreamLastEntryID(key)
			if err != nil {
				return parser.AppendError(nil, err.Error())
			}
		}
		streamIDs[i] = startID
	}

	for {
		// Collect entries from all streams
		var results []struct {
			key     string
			entries []store.StreamEntry
		}

		for i, keyBytes := range streamKeys {
			key := string(keyBytes)
			startID := streamIDs[i]
			if s.stores[0].Type(key) != "stream" {
				return parser.AppendError(nil, "ERR key is not a stream")
			}

			if string(startID) == "$" {
				var err error
				startID, err = s.stores[0].GetStreamLastEntryID(key)
				if err != nil {
					return parser.AppendError(nil, err.Error())
				}
			}
			entries := s.stores[0].Range(key, startID, []byte("+"))

			// Filter entries to make range exclusive
			var validEntries []store.StreamEntry
			for _, entry := range entries {
				if entry.ID > string(startID) {
					validEntries = append(validEntries, entry)
				}
			}

			if len(validEntries) > 0 {
				results = append(results, struct {
					key     string
					entries []store.StreamEntry
				}{key, validEntries})
			}
		}

		if len(results) > 0 || blockMillis == -1 {
			if len(results) == 0 {
				return parser.NullArray()
			}
			response := parser.AppendArray(nil, len(results))
			for _, result := range results {
				response = parser.AppendArray(response, 2)
				response = parser.AppendBulkString(response, result.key)
				response = parser.AppendArray(response, len(result.entries))
				for _, entry := range result.entries {
					response = parser.AppendArray(response, 2)
					response = parser.AppendBulkString(response, entry.ID)
					response = parser.AppendArray(response, len(entry.Value)*2)
					for _, kv := range entry.Value {
						response = parser.AppendBulkString(response, kv.Key)
						response = parser.AppendBulkString(response, kv.Value)
					}
				}
			}
			return response
		}
		if blockMillis > 0 && time.Now().After(deadline) {
			return parser.NullArray()
		}
		time.Sleep(10 * time.Millisecond)
	}

}
