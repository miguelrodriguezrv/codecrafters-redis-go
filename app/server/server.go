package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

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
		err := srv.SetupReplica(config.ReplicaOf)
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

func (s *Server) SetupReplica(replica string) error {
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
	err = s.PSync(conn)
	if err != nil {
		return err
	}
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
	_, err := conn.Write(parser.StringArrayCommand([]string{
		"REPLCONF",
		"listening-port",
		strconv.Itoa(int(s.config.Port)),
	}))
	if err != nil {
		return err
	}
	err = readOK(conn)
	if err != nil {
		return fmt.Errorf("received invalid REPLCONF response: %s", err)
	}

	_, err = conn.Write(parser.StringArrayCommand([]string{
		"REPLCONF",
		"capa",
		"psync2",
	}))
	if err != nil {
		return err
	}

	err = readOK(conn)
	if err != nil {
		return fmt.Errorf("received invalid REPLCONF response: %s", err)
	}
	return nil
}

func (s *Server) PSync(conn net.Conn) error {
	_, err := conn.Write(parser.StringArrayCommand([]string{
		"PSYNC",
		"?",
		"-1",
	}))
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
	log.Println(response)
	return nil
}

func readOK(conn net.Conn) error {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return err
	}
	response := string(buf[:n])
	if response != string(parser.AppendOK(nil)) {
		return fmt.Errorf("%s", response)
	}
	return nil
}
