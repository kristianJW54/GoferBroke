package src

type GbConfig struct {
	SeedServers []Seeds `gb:"seed"`
}

type Seeds struct {
	SeedIP   string
	SeedPort string
}
