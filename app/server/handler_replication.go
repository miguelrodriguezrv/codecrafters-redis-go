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
	"sync"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
)

func (s *Server) handleREPLConf(req [][]byte) []byte {
	if s.info.role != "master" {
		if string(req[1]) == "GETACK" {
			return parser.EncodeStringArray("REPLCONF", "ACK", strconv.FormatInt(s.info.masterReplOffset.Load(), 10))
		}
		return parser.AppendError(nil, "-1")
	}
	return parser.OK()

}

func (s *Server) handlePSync(req [][]byte, conn net.Conn) {
	if len(req) < 3 {
		log.Println("Not enough arguments for PSYNC")
		conn.Write(parser.AppendError(nil, "-1"))
	}
	if string(req[1]) == "?" {
		conn.Write(parser.AppendString(nil, fmt.Sprintf("FULLRESYNC %s %d", s.info.masterReplID, s.info.masterReplOffset.Load())))
		err := s.FullResync(conn)
		if err != nil {
			log.Printf("error handling PSYNC: %v", err)
		}
		s.slaveMutex.Lock()
		var offset atomic.Int64
		offset.Store(s.info.masterReplOffset.Load())
		s.slaves = append(s.slaves, Slave{
			conn:   conn,
			offset: &offset,
		})
		s.slaveMutex.Unlock()
	}
}

func (s *Server) FullResync(conn net.Conn) error {
	if ok := s.handleSave(); string(ok) != string(parser.OK()) {
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
	s.info.masterReplOffset.Add(int64(len(command)))
	log.Printf("Propagating %s to %d slaves", req, len(s.slaves))
	for i, slave := range s.slaves {
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
			log.Printf("Sent command to %s -> %s", slave.conn.RemoteAddr(), req)
		}(i, slave.conn)
	}

	wg.Wait()

	if len(failedSlaves) > 0 {
		newSlaves := make([]Slave, 0, len(s.slaves)-len(failedSlaves))
		for i, slave := range s.slaves {
			if !slices.Contains(failedSlaves, i) {
				newSlaves = append(newSlaves, slave)
			} else {
				slave.conn.Close()
				log.Printf("Closed connection to server [%d]", i)
			}
		}
		s.slaves = newSlaves
	}
	log.Println("Finished propagation")
}

func (s *Server) handleWait(req [][]byte) []byte {
	if len(req) < 3 {
		return parser.AppendError(nil, "-1")
	}
	repAmount, err := strconv.Atoi(string(req[1]))
	if err != nil {
		log.Printf("Invalid WAIT replica amount: %v", err)
		return parser.AppendError(nil, "-1")
	}
	timeout, err := strconv.Atoi(string(req[2]))
	if err != nil {
		log.Printf("Invalid WAIT timeout: %v", err)
		return parser.AppendError(nil, "-1")
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	log.Printf("Waiting for %d for %dms", repAmount, timeout)
	s.slaveMutex.Lock()
	defer s.slaveMutex.Unlock()
	var count atomic.Int64
	command := parser.EncodeStringArray("REPLCONF", "GETACK", "*")
	var wg sync.WaitGroup
	for _, slave := range s.slaves {
		if slave.offset.Load() == s.info.masterReplOffset.Load() {
			count.Add(1)
			continue
		}
		wg.Add(1)
		go func(slave Slave) {
			defer wg.Done()
			_, err := slave.conn.Write(command)
			if err != nil {
				log.Printf("Error sending REPLCONF ACK *: %v", err)
				return
			}

			buf := make([]byte, 128)
			slave.conn.SetReadDeadline(deadline)
			defer slave.conn.SetReadDeadline(time.Time{})
			n, err := slave.conn.Read(buf)
			if err != nil {
				log.Printf("Error REPLCONF ACK response: %v", err)
				return
			}
			count.Add(1)
			req, _, err := parser.ParseCommand(buf[:n])
			if err != nil {
				log.Printf("Error parsing REPLCONF ACK response: %v", err)
				return
			}
			if len(req) < 3 {
				log.Printf("Error length REPLCONF ACK: %v", req)
				return
			}
			offset, err := strconv.ParseInt(string(req[2]), 10, 64)
			if err != nil {
				log.Printf("Invalid offset for REPLCONF ACK: %v", err)
				return
			}
			slave.offset.Store(offset)
		}(slave)
	}
	wg.Wait()
	log.Printf("Got %d ACKs", count.Load())
	return parser.AppendInt(nil, count.Load())
}
