package server

import (
	"log"
	"net"
	"path"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

func (s *Server) handleClient(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}
		resp, err := parser.ParseCommand(buf[:n])
		if err != nil {
			conn.Write(parser.AppendError(nil, err.Error()))
			return
		}
		if len(resp) == 0 {
			continue
		}
		switch strings.ToLower(string(resp[0])) {
		case "ping":
			conn.Write(parser.AppendString(nil, "PONG"))
		case "echo":
			conn.Write(parser.AppendString(nil, string(resp[1])))
		case "config":
			conn.Write(s.handleConfig(resp))
		case "get":
			conn.Write(s.handleGet(resp))
		case "set":
			conn.Write(s.handleSet(resp))
		case "keys":
			conn.Write(s.handleKeys(resp))
		case "save":
			conn.Write(s.handleSave())
		}
	}
}

func (s *Server) handleConfig(resp [][]byte) []byte {
	if len(resp) < 3 {
		return parser.AppendError(nil, "1")
	}
	switch strings.ToLower(string(resp[1])) {
	case "get":
		response := parser.AppendArray(nil, len(resp[2:])*2)
		for _, arg := range resp[2:] {
			switch strings.ToLower(string(arg)) {
			case "dir":
				response = parser.AppendBulkString(response, "dir")
				response = parser.AppendBulkString(response, s.config.Dir)
			case "dbfilename":
				response = parser.AppendBulkString(response, "dbfilename")
				response = parser.AppendBulkString(response, s.config.DBFilename)
			}
		}
		return response
	}
	return parser.AppendError(nil, "-1")
}

func (s *Server) handleGet(resp [][]byte) []byte {
	value, ok := s.stores[0].Get(string(resp[1]))
	if !ok {
		return parser.NullBulkString()
	}
	return parser.AppendBulk(nil, value)
}

func (s *Server) handleSet(resp [][]byte) []byte {
	var expiry int64
	if len(resp) == 5 && strings.ToLower(string(resp[3])) == "px" {
		var err error
		expiry, err = strconv.ParseInt(string(resp[4]), 10, 64)
		if err != nil {
			return parser.AppendError(nil, "1")
		}
	}
	err := s.stores[0].Set(string(resp[1]), resp[2], expiry)
	if err != nil {
		return parser.AppendError(nil, "1")
	}
	return parser.AppendString(nil, "OK")
}

func (s *Server) handleKeys(resp [][]byte) []byte {
	if len(resp) < 2 {
		log.Println("Not enough arguments for KEYS")
		return parser.AppendError(nil, "-1")
	}
	keys, err := s.stores[0].Keys(string(resp[1]))
	if err != nil {
		log.Println(err)
		return parser.AppendError(nil, "-1")
	}
	response := parser.AppendArray(nil, len(keys))
	for _, k := range keys {
		response = parser.AppendBulkString(response, k)
	}
	return response
}

func (s *Server) handleSave() []byte {
	databases := make([]*persistence.Database, 0, len(s.stores))
	for i, store := range s.stores {
		databases = append(databases, &persistence.Database{
			Index:   i,
			Entries: store.Export(),
		})
	}
	if err := persistence.SaveRDB(s.config.Dir, s.config.DBFilename, databases); err != nil {
		log.Println(err)
		return parser.AppendError(nil, "-1")
	}
	log.Printf("Successfully saved RDB to %s\n", path.Join(s.config.Dir, s.config.DBFilename))
	return parser.AppendString(nil, "OK")
}
