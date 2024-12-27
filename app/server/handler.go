package server

import (
	"io"
	"log"
	"net"
	"path"
	"strconv"
	"strings"

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
			response := s.handleCommand(req, conn)

			if len(response) > 0 {
				conn.Write(response)
			}
		}
	}
}

func (s *Server) handleCommand(req [][]byte, conn net.Conn) []byte {
	cmd := strings.ToLower(string(req[0]))

	switch cmd {
	case "multi":
		return s.handleMulti(conn)
	case "exec":
		return s.handleExec(conn)
	case "discard":
		return s.handleDiscard(conn)
	}
	s.txMutex.Lock()
	tx, exists := s.transactions[conn]
	s.txMutex.Unlock()

	if exists && tx.inMulti {
		tx.commands = append(tx.commands, req)
		return parser.AppendString(nil, "QUEUED")
	}
	var response []byte
	switch cmd {
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
	case "incr":
		response = s.handleIncr(req)
		s.PropagateCommand(req)
	case "xadd":
		response = s.handleXAdd(req)
		s.PropagateCommand(req)
	case "xrange":
		response = s.handleXRange(req)
	case "xread":
		response = s.handleXRead(req)
	case "type":
		response = s.handleType(req)
	case "keys":
		response = s.handleKeys(req)
	case "save":
		response = s.handleSave()
	case "replconf":
		response = s.handleREPLConf(req)
	case "psync":
		if s.info.role != "master" {
			response = parser.AppendError(nil, "-1")
		} else {
			s.handlePSync(req, conn)
			return nil
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
	return response
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

func (s *Server) handleIncr(req [][]byte) []byte {
	if len(req) < 2 {
		log.Println("Not enough arguments for INCR")
		return parser.AppendError(nil, "-1")
	}
	key := string(req[1])
	value, ok := s.stores[0].Get(key)
	var valInt int64
	if ok {
		var err error
		valInt, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return parser.AppendError(nil, "ERR value is not an integer or out of range")
		}
	}
	valInt++
	s.stores[0].Set(key, []byte(strconv.FormatInt(valInt, 10)), 0)
	return parser.AppendInt(nil, int64(valInt))
}

func (s *Server) handleType(req [][]byte) []byte {
	if len(req) < 2 {
		log.Println("Not enough arguments for TYPE")
		return parser.AppendError(nil, "-1")
	}
	return parser.AppendString(nil, s.stores[0].Type(string(req[1])))
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
	return parser.OK()
}
