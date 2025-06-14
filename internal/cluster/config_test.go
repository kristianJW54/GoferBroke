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

func TestComplexConfigSchema(t *testing.T) {

	cfg := InitDefaultClusterConfig()

	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: "8081"})

	test1 := make(map[string]int)
	test1["mapKey1"] = 2
	test1["mapKey2"] = 3

	test2 := make(map[string]int)
	test2["mapKey3"] = 4
	test2["mapKey4"] = 5

	cfg.Cluster.TestNest2["alpha"] = []int{1, 2, 3}
	cfg.Cluster.TestNest2["beta"] = []int{4, 5, 6}

	cfg.Cluster.TestNest = append(cfg.Cluster.TestNest, test1)
	cfg.Cluster.TestNest = append(cfg.Cluster.TestNest, test2)

	schema := BuildConfigSchema(cfg)

	if sch, ok := schema["Cluster.TestNest2.<key>"]; ok {
		log.Printf("sch = %v", sch)
	}

	err := SetByPath(schema, cfg, "Cluster.TestNest.1.mapKey4", 9)
	if err != nil {
		t.Errorf("Error setting new name: %v", err)
	}

	log.Printf("new mapKey4 value = %v", cfg.Cluster.TestNest[1]["mapKey4"])

	//err := SetByPath(schema, cfg, "Cluster.TestNest2.alpha.1", 2)
	//if err != nil {
	//	t.Errorf("Error setting new name: %v", err)
	//}
	//
	//log.Printf("new alpha value at index 1 = %v", cfg.Cluster.TestNest2["alpha"][1])

}

func TestComplexConfigSchema2(t *testing.T) {
	cfg := InitDefaultClusterConfig()

	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: "8081"})

	test1 := map[string]int{"mapKey1": 2, "mapKey2": 3}
	test2 := map[string]int{"mapKey3": 4, "mapKey4": 5}

	cfg.Cluster.TestNest = append(cfg.Cluster.TestNest, test1, test2)
	cfg.Cluster.TestNest2["alpha"] = []int{1, 2, 3}
	cfg.Cluster.TestNest2["beta"] = []int{4, 5, 6}

	schema := BuildConfigSchema(cfg)

	// Make sure the schema was built properly for the map
	if _, ok := schema["Cluster.TestNest2.<key>"]; !ok {
		t.Fatalf("expected schema to contain key 'Cluster.TestNest2.<key>'")
	}

	t.Run("Set nested map inside slice", func(t *testing.T) {
		oldVal := cfg.Cluster.TestNest[1]["mapKey4"]
		t.Logf("Old value of mapKey4: %d", oldVal)

		err := SetByPath(schema, cfg, "Cluster.TestNest.1.mapKey4", 9)
		if err != nil {
			t.Errorf("Error setting value: %v", err)
		}

		newVal := cfg.Cluster.TestNest[1]["mapKey4"]
		t.Logf("New value of mapKey4: %d", newVal)

		if newVal != 9 {
			t.Errorf("Expected mapKey4 to be 9, got %d", newVal)
		}
	})

	t.Run("Set slice inside map value", func(t *testing.T) {
		oldVal := cfg.Cluster.TestNest2["alpha"][1]
		t.Logf("Old value of alpha[1]: %d", oldVal)

		err := SetByPath(schema, cfg, "Cluster.TestNest2.alpha.1", 42)
		if err != nil {
			t.Errorf("Error setting value: %v", err)
		}

		newVal := cfg.Cluster.TestNest2["alpha"][1]
		t.Logf("New value of alpha[1]: %d", newVal)

		if newVal != 42 {
			t.Errorf("Expected alpha[1] to be 42, got %d", newVal)
		}
	})
}
