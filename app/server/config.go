package server

type Config struct {
	Dir        string
	DBFilename string
	Port       uint16
	ReplicaOf  string
}
