package src

type GbConfig struct {
	SeedServers []SeedServer
}

type SeedServer struct {
	ServerName string
	ServerIP   string
	ServerPort int
}
