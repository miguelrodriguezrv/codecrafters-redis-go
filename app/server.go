package main

import (
	"flag"
	"log"
	"os"
	"path"

	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func main() {
	dir := flag.String("dir", "/tmp/redis-data", "the path to the directory where the RDB file is stored")
	dbFilename := flag.String("dbfilename", "rdbfile", "the name of the RDB file")
	flag.Parse()

	config := server.Config{
		Dir:        *dir,
		DBFilename: *dbFilename,
	}

	stores := createStores(path.Join(config.Dir, config.DBFilename))

	srv := server.NewServer(config, stores)
	if err := srv.Listen("0.0.0.0:6379"); err != nil {
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
