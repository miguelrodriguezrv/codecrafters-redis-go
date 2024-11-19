package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"

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
	role   string
	stores []Store
}

func NewServer(config Config, stores []Store) *Server {
	return &Server{
		config: config,
		role:   "master",
		stores: stores,
	}
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
