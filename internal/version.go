package internal

import "runtime"

var (
	Version   = "dev"
	GitCommit = "none"
	BuildTime = "unknown"
)

func Info() map[string]string {
	return map[string]string{
		"version":   Version,
		"commit":    GitCommit,
		"buildTime": BuildTime,
		"goVersion": runtime.Version(),
	}
}

// To use with go build and -idFlags
