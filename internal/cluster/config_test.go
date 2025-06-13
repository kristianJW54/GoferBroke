package cluster

import (
	"log"
	"testing"
)

func TestBasicConfigSchema(t *testing.T) {

	cfg := InitDefaultClusterConfig()

	cfg.Name = "test-cluster"
	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: "8081"})
	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: "8082"})

	schema := BuildConfigSchema(cfg)

	newName := "big_boy_cluster"

	if field, ok := schema["SeedServers.0"]; ok {
		log.Printf("Name = %s", cfg.Name)
		log.Printf("path - %s | field type and kind -> %s - %s", field.Path, field.Type, field.Kind)
		log.Printf("getter is nil? %v", field.Getter == nil)
	}

	if field, ok := schema["Name"]; ok {
		log.Printf("old name = %s", cfg.Name)
		log.Printf("path - %s | field type and kind -> %s - %s", field.Path, field.Type, field.Kind)
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

func TestSliceConfigSchema(t *testing.T) {

	cfg := InitDefaultClusterConfig()

	oldPort := "8082"

	cfg.Name = "test-cluster"
	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: "8081"})
	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: oldPort})

	schema := BuildConfigSchema(cfg)

	log.Printf("Old port = %s", cfg.SeedServers[1].Port)

	newPort := "5000"

	err := SetByPath(schema, cfg, "SeedServers.1.Port", newPort)
	if err != nil {
		t.Errorf("Error setting new name: %v", err)
	}

	if cfg.SeedServers[1].Port != newPort {
		t.Errorf("New port is %s, wanted %s", cfg.SeedServers[1].Port, newPort)
	}
	log.Printf("New port = %s", cfg.SeedServers[1].Port)

}
