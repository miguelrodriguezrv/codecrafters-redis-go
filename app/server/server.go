package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

const (
	MasterRole = "master"
	SlaveRole  = "slave"
)

type Store interface {
	Keys(pattern string) ([]string, error)
	Load(entries []persistence.Entry)
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, expiry int64) error
	Export() []persistence.Entry
}

type Server struct {
	config Config
	info   Info
	stores []Store
}

func NewServer(config Config, stores []Store) *Server {
	var role string
	if config.ReplicaOf == "" {
		role = "master"
	} else {
		role = "slave"
	}

	srv := &Server{
		config: config,
		stores: stores,
		info: Info{
			role:             role,
			masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			masterReplOffset: 0,
		},
	}

	if config.ReplicaOf != "" {
		err := srv.PingServer(config.ReplicaOf)
		if err != nil {
			log.Fatal(err)
		}
	}

	return srv
}

func (s *Server) Listen(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("Failed to bind to " + address)
	}
	log.Println("Listening to " + address)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.handleClient(conn)
	}
}

func (s *Server) PingServer(address string) error {
	log.Printf("Pinging %s\n", address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	conn.Write(parser.AppendBulkString(parser.AppendArray(nil, 1), "PING"))
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
