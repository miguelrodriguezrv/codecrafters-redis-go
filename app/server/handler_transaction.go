package server

import (
	"log"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
)

func (s *Server) handleMulti(conn net.Conn) []byte {
	s.txMutex.Lock()
	defer s.txMutex.Unlock()

	if tx, exists := s.transactions[conn]; exists && tx.inMulti {
		return parser.AppendError(nil, "ERR MULTI calls can not be nested")
	}

	s.transactions[conn] = &Transaction{
		commands: make([][][]byte, 0),
		inMulti:  true,
	}

	return parser.OK()
}

func (s *Server) handleExec(conn net.Conn) []byte {
	s.txMutex.Lock()

	tx, exists := s.transactions[conn]
	if !exists || !tx.inMulti {
		s.txMutex.Unlock()
		return parser.AppendError(nil, "ERR EXEC without MULTI")
	}

	tx.inMulti = false
	s.txMutex.Unlock()

	responses := make([][]byte, 0, len(tx.commands))
	for _, cmd := range tx.commands {
		log.Printf("Launching cmd %s", cmd)
		response, _ := s.handleCommand(cmd, conn)
		log.Printf("Response -> %s", response)
		responses = append(responses, response)
	}

	delete(s.transactions, conn)

	result := parser.AppendArray(nil, len(responses))
	for _, resp := range responses {
		result = append(result, resp...)
	}
	return result
}

func (s *Server) handleDiscard(conn net.Conn) []byte {
	s.txMutex.Lock()
	defer s.txMutex.Unlock()

	if tx, exists := s.transactions[conn]; !exists || !tx.inMulti {
		return parser.AppendError(nil, "ERR DISCARD without MULTI")
	}

	delete(s.transactions, conn)
	return parser.OK()
}
