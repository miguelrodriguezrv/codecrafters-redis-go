package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func main() {
	dir := flag.String("dir", "/tmp/redis-data", "the path to the directory where the RDB file is stored")
	dbFilename := flag.String("dbfilename", "rdbfile", "the name of the RDB file")
	flag.Parse()

	config := Config{
		Dir:        *dir,
		DBFilename: *dbFilename,
	}

	stores := createStores(path.Join(config.Dir, config.DBFilename))

	server := NewServer(config, stores)
	if err := server.Listen("0.0.0.0:6379"); err != nil {
		log.Fatal(err)
	}
}

func createStores(rdbPath string) []store.Store {
	stores := []store.Store{store.NewInMemoryStore()}
	if file, err := os.Open(rdbPath); err == nil {
		databases, err := persistence.LoadRDB(file)
		if err != nil {
			log.Fatalf("Error loading RDB file: %v", err)
		}
		file.Seek(0, 0)
		if err := persistence.VerifyChecksum(file); err != nil {
			log.Fatalf("Error veryfing RDB file: %v", err)
		}
		stores = make([]store.Store, len(databases))
		for _, db := range databases {
			store := store.NewInMemoryStore()
			store.Load(db.Entries)
			stores[db.Index] = store
		}
		log.Println("Successfully loaded", rdbPath)
	}
	return stores
}

type Server struct {
	config Config
	stores []store.Store
}

func NewServer(config Config, stores []store.Store) *Server {
	return &Server{
		config: config,
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

func (s *Server) handleClient(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}
		resp, err := parser.ParseCommand(buf[:n])
		if err != nil {
			conn.Write(parser.AppendError(nil, err.Error()))
			return
		}
		if len(resp) == 0 {
			continue
		}
		switch strings.ToLower(string(resp[0])) {
		case "ping":
			conn.Write(parser.AppendString(nil, "PONG"))
		case "echo":
			conn.Write(parser.AppendString(nil, string(resp[1])))
		case "config":
			if len(resp) < 3 {
				conn.Write(parser.AppendError(nil, "1"))
				return
			}
			switch strings.ToLower(string(resp[1])) {
			case "get":
				response := parser.AppendArray(nil, len(resp[2:])*2)
				for _, arg := range resp[2:] {
					switch strings.ToLower(string(arg)) {
					case "dir":
						response = parser.AppendBulkString(response, "dir")
						response = parser.AppendBulkString(response, s.config.Dir)
					case "dbfilename":
						response = parser.AppendBulkString(response, "dbfilename")
						response = parser.AppendBulkString(response, s.config.DBFilename)
					}
				}
				conn.Write(response)
			}

		case "get":
			value, ok := s.stores[0].Get(string(resp[1]))
			if !ok {
				conn.Write(parser.NullBulkString())
				return
			}
			conn.Write(parser.AppendBulk(nil, value))
		case "set":
			var expiry int64
			if len(resp) == 5 && strings.ToLower(string(resp[3])) == "px" {
				expiry, err = strconv.ParseInt(string(resp[4]), 10, 64)
				if err != nil {
					conn.Write(parser.AppendError(nil, "1"))
					return
				}
			}
			err = s.stores[0].Set(string(resp[1]), resp[2], expiry)
			if err != nil {
				conn.Write(parser.AppendError(nil, "1"))
				return
			}
			conn.Write(parser.AppendString(nil, "OK"))
		case "keys":
			if len(resp) < 2 {
				log.Println("Not enough arguments for KEYS")
				conn.Write(parser.AppendError(nil, "-1"))
				return
			}
			keys, err := s.stores[0].Keys(string(resp[1]))
			if err != nil {
				log.Println(err)
				conn.Write(parser.AppendError(nil, "-1"))
				return
			}
			conn.Write(parser.AppendArray(nil, len(keys)))
			for _, k := range keys {
				conn.Write(parser.AppendBulkString(nil, k))
			}
		case "save":
			databases := make([]*persistence.Database, 0, len(s.stores))
			for i, store := range s.stores {
				databases = append(databases, &persistence.Database{
					Index:   i,
					Entries: store.Export(),
				})
			}
			if err := persistence.SaveRDB(s.config.Dir, s.config.DBFilename, databases); err != nil {
				log.Println(err)
				conn.Write(parser.AppendError(nil, "-1"))
				return
			}
			log.Printf("Successfully saved RDB to %s\n", path.Join(s.config.Dir, s.config.DBFilename))
			conn.Write(parser.AppendString(nil, "OK"))
		}
	}
}
