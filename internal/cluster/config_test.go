package cluster

import (
	"log"
	"testing"
)

func TestBasicConfigSchema(t *testing.T) {

	cfg := InitDefaultClusterConfig()

	cfg.Name = "test-cluster"
	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: "8081"})

	schema := BuildConfigSchema(cfg)

	newName := "big_boy_cluster"

	if field, ok := schema["Name"]; ok {
		log.Printf("old name = %s", cfg.Name)
		err := field.Setter(newName)
		if err != nil {
			t.Errorf("Error setting new name: %v", err)
		}
	} else {
		t.Errorf("Name not found in schema")
	}

	if field, ok := schema["Name"]; ok {
		name := field.Getter()
		log.Printf("new name = %s", name)
		if name != newName {
			t.Errorf("New name is %s, wanted %s", name, newName)
		}
	} else {
		t.Errorf("Name not found in schema")
	}

}
