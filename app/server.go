package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func main() {
	dir := flag.String("dir", "/tmp/redis-data", "the path to the directory where the RDB file is stored")
	dbFilename := flag.String("dbfilename", "rdbfile", "the name of the RDB file")
	port := flag.Uint("port", 6379, "the port for the server to listen on")
	replicaOf := flag.String("replicaof", "", "the host and port of the master server to replicate from")
	flag.Parse()
	if *port > 65535 {
		log.Fatalf("Invalid port %d", *port)
	}
	var replica string
	if *replicaOf != "" {
		v := strings.Split(*replicaOf, " ")
		replica = fmt.Sprintf("%s:%s", v[0], v[1])
	}

	config := server.Config{
		Dir:        *dir,
		DBFilename: *dbFilename,
		Port:       uint16(*port),
		ReplicaOf:  replica,
	}

	stores := createStores(path.Join(config.Dir, config.DBFilename))

	srv := server.NewServer(config, stores)
	if err := srv.Listen(fmt.Sprintf("0.0.0.0:%d", config.Port)); err != nil {
		log.Fatal(err)
	}
}

func createStores(rdbPath string) []server.Store {
	stores := []server.Store{store.NewInMemoryStore()}
	if file, err := os.Open(rdbPath); err == nil {
		databases, err := persistence.LoadRDB(file)
		if err != nil {
			log.Fatalf("Error loading RDB file: %v", err)
		}
		file.Seek(0, 0)
		if err := persistence.VerifyChecksum(file); err != nil {
			log.Fatalf("Error veryfing RDB file: %v", err)
		}
		stores = make([]server.Store, len(databases))
		for _, db := range databases {
			store := store.NewInMemoryStore()
			store.Load(db.Entries)
			stores[db.Index] = store
		}
		log.Println("Successfully loaded", rdbPath)
	}
	return stores
}
