package src

type GbConfig struct {
	SeedServers []Seeds
}

type Seeds struct {
	SeedIP   string
	SeedPort string
}
