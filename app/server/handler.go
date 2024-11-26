package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

func (s *Server) handleClient(conn net.Conn) {
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 1024)

outerLoop:
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading from client:", err)
			}
			conn.Close()
			return
		}
		buf = append(buf, tmp[:n]...)
		for len(buf) > 0 {
			req, remainder, err := parser.ParseCommand(buf[:n])
			if err != nil {
				if err == parser.ErrIncomplete {
					// Command is incomplete, wait for more data
					break
				}
				log.Printf("Error parsing command: %v", err)
				conn.Write(parser.AppendError(nil, err.Error()))
				conn.Close()
				return
			}
			buf = remainder
			if len(req) == 0 {
				continue outerLoop
			}
			var response []byte
			switch strings.ToLower(string(req[0])) {
			case "ping":
				response = parser.AppendString(nil, "PONG")
			case "echo":
				response = parser.AppendString(nil, string(req[1]))
			case "config":
				response = s.handleConfig(req)
			case "info":
				response = s.handleInfo(req)
			case "get":
				response = s.handleGet(req)
			case "set":
				response = s.handleSet(req)
				s.PropagateCommand(req)
			case "keys":
				response = s.handleKeys(req)
			case "save":
				response = s.handleSave()
			case "replconf":
				response = s.handleREPLConf()
			case "psync":
				if s.info.role != "master" {
					response = parser.AppendError(nil, "-1")
				} else {
					s.handlePSync(req, conn)
					return
				}
			case "wait":
				if s.info.role == "master" {
					response = s.handleWait(req)
				} else {
					response = parser.AppendError(nil, "-1")
				}
			default:
				response = parser.AppendError(nil, "-1")
			}
			conn.Write(response)
		}
	}
}

func (s *Server) handleConfig(req [][]byte) []byte {
	if len(req) < 3 {
		return parser.AppendError(nil, "1")
	}
	switch strings.ToLower(string(req[1])) {
	case "get":
		response := parser.AppendArray(nil, len(req[2:])*2)
		for _, arg := range req[2:] {
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

func (s *Server) handleInfo(req [][]byte) []byte {
	for _, section := range req[1:] {
		switch strings.ToLower(string(section)) {
		case "replication":
			return parser.AppendBulk(nil, s.getInfoReplication())
		}
	}
	return parser.AppendBulk(nil, s.getInfoReplication())
}

func (s *Server) handleGet(req [][]byte) []byte {
	value, ok := s.stores[0].Get(string(req[1]))
	if !ok {
		return parser.NullBulkString()
	}
	return parser.AppendBulk(nil, value)
}

func (s *Server) handleSet(req [][]byte) []byte {
	var expiry int64
	if len(req) == 5 && strings.ToLower(string(req[3])) == "px" {
		var err error
		expiry, err = strconv.ParseInt(string(req[4]), 10, 64)
		if err != nil {
			return parser.AppendError(nil, "1")
		}
	}
	err := s.stores[0].Set(string(req[1]), req[2], expiry)
	if err != nil {
		return parser.AppendError(nil, "1")
	}
	return parser.AppendString(nil, "OK")
}

func (s *Server) handleKeys(req [][]byte) []byte {
	if len(req) < 2 {
		log.Println("Not enough arguments for KEYS")
		return parser.AppendError(nil, "-1")
	}
	keys, err := s.stores[0].Keys(string(req[1]))
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
	return parser.AppendOK(nil)
}

func (s *Server) handleREPLConf() []byte {
	if s.info.role != "master" {
		return parser.StringArrayCommand([]string{
			"REPLCONF",
			"ACK",
			strconv.FormatInt(s.info.masterReplOffset, 10),
		})
	}
	return parser.AppendOK(nil)

}

func (s *Server) handlePSync(req [][]byte, conn net.Conn) {
	if len(req) < 3 {
		log.Println("Not enough arguments for PSYNC")
		conn.Write(parser.AppendError(nil, "-1"))
	}
	if string(req[1]) == "?" {
		conn.Write(parser.AppendString(nil, fmt.Sprintf("FULLRESYNC %s %d", s.info.masterReplID, s.info.masterReplOffset)))
		err := s.FullResync(conn)
		if err != nil {
			log.Printf("error handling PSYNC: %v", err)
		}
		s.slaveMutex.Lock()
		s.slaves = append(s.slaves, conn)
		s.slaveMutex.Unlock()
	}
}

func (s *Server) FullResync(conn net.Conn) error {
	if ok := s.handleSave(); string(ok) != string(parser.AppendOK(nil)) {
		return errors.New("error saving RDB")
	}
	file, err := os.Open(path.Join(s.config.Dir, s.config.DBFilename))
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}
	fileSize := fileInfo.Size()
	_, err = conn.Write([]byte(fmt.Sprintf("$%d\r\n", fileSize)))
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	for {
		n, readErr := file.Read(buf)
		if n > 0 {
			if _, writeErr := conn.Write(buf[:n]); writeErr != nil {
				return fmt.Errorf("failed to write to connection: %v", writeErr)
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("failed to read file: %v", readErr)
		}
	}

	log.Println("File streamed successfully")
	return nil
}

func (s *Server) PropagateCommand(req [][]byte) {
	command := parser.AppendArray(nil, len(req))
	for _, r := range req {
		command = parser.AppendBulkString(command, string(r))
	}
	var mu sync.Mutex
	var failedSlaves []int
	var wg sync.WaitGroup

	s.slaveMutex.Lock()
	defer s.slaveMutex.Unlock()
	for i, conn := range s.slaves {
		wg.Add(1)

		go func(i int, conn net.Conn) {
			defer wg.Done()

			_, err := conn.Write(command)
			if err != nil {
				log.Printf("Error propagating command to server [%d] -> %v", i, err)
				mu.Lock()
				failedSlaves = append(failedSlaves, i)
				mu.Unlock()
			}
		}(i, conn)
	}

	wg.Wait()

	if len(failedSlaves) > 0 {
		newSlaves := make([]net.Conn, 0, len(s.slaves)-len(failedSlaves))
		for i, conn := range s.slaves {
			if !slices.Contains(failedSlaves, i) {
				newSlaves = append(newSlaves, conn)
			} else {
				conn.Close()
				log.Printf("Closed connection to server [%d]", i)
			}
		}
		s.slaves = newSlaves
	}
}

func (s *Server) handleWait(req [][]byte) []byte {
	if len(req) < 3 {
		return parser.AppendError(nil, "-1")
	}
	repAmount, err := strconv.Atoi(string(req[1]))
	if err != nil {
		log.Printf("Invalid WAIT amount: %v", err)
		return parser.AppendError(nil, "-1")
	}
	if repAmount == 0 {
		return parser.AppendInt(nil, 0)
	}
	return parser.AppendInt(nil, int64(repAmount))
}
