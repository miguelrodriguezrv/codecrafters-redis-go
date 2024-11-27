package server

import (
	"fmt"
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
)

type Info struct {
	role             string
	masterReplID     string
	masterReplOffset *atomic.Int64
}

func (s *Server) getInfoReplication() []byte {
	var response []byte
	response = parser.AppendBulkString(nil, fmt.Sprintf(`# Replication
role:%s
master_replid:%s
master_repl_offset:%d`,
		s.info.role,
		s.info.masterReplID,
		s.info.masterReplOffset.Load(),
	))
	return response
}
