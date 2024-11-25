package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/store"
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
	config     Config
	info       Info
	ready      bool
	slaveMutex sync.Mutex
	slaves     []net.Conn
	stores     []Store
}

func NewServer(config Config, rdbPath string) *Server {
	var role string
	if config.ReplicaOf == "" {
		role = "master"
	} else {
		role = "slave"
	}

	srv := &Server{
		config: config,
		stores: createStores(rdbPath),
		info: Info{
			role:             role,
			masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			masterReplOffset: 0,
		},
	}

	if config.ReplicaOf != "" {
		err := srv.SetupReplica(config.ReplicaOf, path.Join(config.Dir, config.DBFilename))
		if err != nil {
			log.Fatal(err)
		}
		srv.stores = createStores(rdbPath)
	}

	srv.ready = true

	return srv
}

func createStores(rdbPath string) []Store {
	stores := []Store{store.NewInMemoryStore()}
	if file, err := os.Open(rdbPath); err == nil {
		databases, err := persistence.LoadRDB(file)
		if err != nil {
			log.Fatalf("Error loading RDB file: %v", err)
		}
		file.Seek(0, 0)
		if err := persistence.VerifyChecksum(file); err != nil {
			log.Fatalf("Error veryfing RDB file: %v", err)
		}
		if len(databases) > 0 {
			stores = make([]Store, len(databases))
			for _, db := range databases {
				store := store.NewInMemoryStore()
				store.Load(db.Entries)
				stores[db.Index] = store
			}
		}
		log.Println("Successfully loaded", rdbPath)
	}
	return stores
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
