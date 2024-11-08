package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	// Uncomment this block to pass the first stage
	//
	store := NewStore()
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleClient(conn, store)
	}
}

func handleClient(conn net.Conn, store *Store) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}
		resp, err := ParseCommand(buf[:n])
		if len(resp) == 0 {
			continue
		}
		switch strings.ToLower(string((resp[0]))) {
		case "ping":
			conn.Write(AppendString(nil, "PONG"))
		case "echo":
			conn.Write(AppendString(nil, string(resp[1])))
		case "get":
			value, ok := store.Get(string(resp[1]))
			if !ok {
				conn.Write(NullBulkString())
				return
			}
			conn.Write(AppendBulk(nil, value))
		case "set":
			var expiry int64
			if len(resp) == 5 && strings.ToLower(string(resp[3])) == "px" {
				expiry, err = strconv.ParseInt(string(resp[4]), 10, 64)
				if err != nil {
					conn.Write(AppendError(nil, "1"))
					return
				}
			}
			err = store.Set(string(resp[1]), resp[2], expiry)
			if err != nil {
				conn.Write(AppendError(nil, "1"))
				return
			}
			conn.Write(AppendString(nil, "OK"))
		}
	}
}
