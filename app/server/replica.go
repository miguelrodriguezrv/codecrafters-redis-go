package server

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
)

func (s *Server) SetupReplica(replica, rdbPath string) error {
	conn, err := net.Dial("tcp", replica)
	if err != nil {
		return err
	}
	err = s.PingServer(conn)
	if err != nil {
		return err
	}
	err = s.SendReplConf(conn)
	if err != nil {
		return err
	}
	err = s.PSync(conn, rdbPath)
	if err != nil {
		return err
	}

	go s.handleMaster(conn)
	return nil
}

func (s *Server) PingServer(conn net.Conn) error {
	_, err := conn.Write(parser.AppendBulkString(parser.AppendArray(nil, 1), "PING"))
	if err != nil {
		return err
	}
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return err
	}
	response := string(buf[:n])
	if response != "+PONG\r\n" {
		return fmt.Errorf("received invalid PING response: %s", response)
	}
	return nil
}

func (s *Server) SendReplConf(conn net.Conn) error {
	_, err := conn.Write(parser.EncodeStringArray("REPLCONF", "listening-port", strconv.Itoa(int(s.config.Port))))
	if err != nil {
		return err
	}
	err = readOK(conn)
	if err != nil {
		return fmt.Errorf("received invalid REPLCONF response: %s", err)
	}

	_, err = conn.Write(parser.EncodeStringArray("REPLCONF", "capa", "psync2"))
	if err != nil {
		return err
	}

	err = readOK(conn)
	if err != nil {
		return fmt.Errorf("received invalid REPLCONF response: %s", err)
	}
	return nil
}

func (s *Server) PSync(conn net.Conn, rdbPath string) error {
	_, err := conn.Write(parser.EncodeStringArray("PSYNC", "?", "-1"))
	if err != nil {
		return err
	}
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return err
	}
	// Split the received data by \r\n to process the FULLRESYNC line
	data := buf[:n]
	lines := bytes.SplitN(data, []byte("\r\n"), 2)
	if len(lines) < 2 {
		return fmt.Errorf("incomplete FULLRESYNC response: %s", data)
	}

	// Parse the FULLRESYNC line
	firstLine := string(lines[0])
	if !strings.HasPrefix(firstLine, "+FULLRESYNC") {
		return fmt.Errorf("unexpected response: %s", firstLine)
	}

	// Log the FULLRESYNC details
	parts := strings.Split(firstLine, " ")
	if len(parts) < 3 {
		return fmt.Errorf("invalid FULLRESYNC response: %s", firstLine)
	}
	replID := parts[1]
	offset := parts[2]
	log.Printf("FULLRESYNC received: replID=%s, offset=%s", replID, offset)

	return readFullRDB(conn, lines[1], rdbPath)
}

func readFullRDB(conn net.Conn, leftover []byte, rdbPath string) error {
	// Read the length header
	var lengthStr string
	var lengthStrComplete bool
	// Process leftover if any
	if len(leftover) > 0 {
		// Look for complete length header in leftover
		if idx := bytes.Index(leftover, []byte("\r\n")); idx >= 0 {
			lengthStr = string(leftover[:idx])
			// Any remaining data after \r\n is start of RDB
			leftover = leftover[idx+2:]
			lengthStrComplete = true
		} else {
			// Incomplete header in leftover
			lengthStr = string(leftover)
			leftover = nil
			// Continue reading from conn
		}
	}

	// If we don't have complete length header, read rest from conn
	if !lengthStrComplete {
		buf := make([]byte, 1)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				return fmt.Errorf("failed to read length header: %v", err)
			}
			if buf[0] == '\n' && len(lengthStr) > 0 && lengthStr[len(lengthStr)-1] == '\r' {
				lengthStr = lengthStr[:len(lengthStr)-1]
				break
			}
			lengthStr += string(buf[0])
		}
	}
	// Parse the length
	if !strings.HasPrefix(lengthStr, "$") {
		return fmt.Errorf("invalid RDB length prefix: %s", lengthStr)
	}
	length, err := strconv.ParseInt(lengthStr[1:], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse RDB length: %v", err)
	}

	log.Printf("Expecting RDB file of length: %d bytes", length)

	// Create a file to save the RDB data
	outputFile, err := os.Create(rdbPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Write leftover data if any
	if len(leftover) > 0 {
		if int64(len(leftover)) > length {
			return fmt.Errorf("leftover data exceeds expected RDB length")
		}
		if _, err := outputFile.Write(leftover); err != nil {
			return fmt.Errorf("failed to write leftover data: %v", err)
		}
		length -= int64(len(leftover))
	}

	// Read the file content
	remaining := length
	buf := make([]byte, 8192)
	for remaining > 0 {
		toRead := min(remaining, int64(len(buf)))
		n, err := conn.Read(buf[:toRead])
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("EOF before reading complete RDB file: %d bytes remaining", remaining)
			}
			return fmt.Errorf("failed to read RDB content: %v", err)
		}

		// Write to the output file
		if _, err := outputFile.Write(buf[:n]); err != nil {
			return fmt.Errorf("failed to write to output file: %v", err)
		}

		remaining -= int64(n)
		log.Printf("Read %d/%d bytes", length-remaining, length)
	}
	log.Println("RDB file received successfully")
	return nil
}

func (s *Server) handleMaster(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 1024)

	for !s.ready {
		time.Sleep(10 * time.Millisecond)
	}
	log.Println("Listening to master")
outerLoop:
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err == io.EOF {
				log.Println("Master disconnected:", conn.RemoteAddr())
			} else {
				log.Println("Error reading from master:", err)
			}
			return
		}
		buf = append(buf, tmp[:n]...)
		for len(buf) > 0 {
			req, remainder, err := parser.ParseCommand(buf)
			if err != nil {
				if err == parser.ErrIncomplete {
					// Command is incomplete, wait for more data
					break
				}
				log.Printf("Error parsing command: %v", err)
				conn.Write(parser.AppendError(nil, err.Error()))
				return
			}
			processedBytes := len(buf) - len(remainder)
			buf = remainder
			if len(req) == 0 {
				log.Println("Empty request received")
				continue outerLoop
			}
			log.Printf("Request received: %s", req)
			switch strings.ToLower(string(req[0])) {
			case "set":
				s.handleSet(req)
			case "replconf":
				response := s.handleREPLConf(req)
				log.Printf("Sending REPLCONF response: %q", response)
				_, err := conn.Write(response)
				if err != nil {
					log.Printf("Error writing REPLCONF response: %v", err)
				}
			}
			s.info.masterReplOffset.Add(int64(processedBytes))
		}
	}
}
