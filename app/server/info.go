package server

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
)

type Info struct {
	role             string
	masterReplID     string
	masterReplOffset int64
}

func (s *Server) getInfoReplication() []byte {
	var response []byte
	response = parser.AppendBulkString(nil, fmt.Sprintf(`# Replication
role:%s
master_replid:%s
master_repl_offset:%d`,
		s.info.role,
		s.info.masterReplID,
		s.info.masterReplOffset,
	))
	return response
}
