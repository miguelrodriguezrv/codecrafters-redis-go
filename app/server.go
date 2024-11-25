package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/server"
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
	err := os.MkdirAll(config.Dir, 0750)
	if err != nil {
		log.Fatal(err)
	}

	srv := server.NewServer(config, path.Join(config.Dir, config.DBFilename))

	if err := srv.Listen(fmt.Sprintf("0.0.0.0:%d", config.Port)); err != nil {
		log.Fatal(err)
	}
}
