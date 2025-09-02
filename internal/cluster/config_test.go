package cluster

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func printConfig(cfg interface{}) {
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		fmt.Printf("error marshalling config: %v\n", err)
		return
	}
	fmt.Println(string(out))
}

func TestKeyPathFlatten(t *testing.T) {

	filePath := "../../Configs/cluster/default_cluster_config.conf"
	cfg := InitDefaultClusterConfig()

	sch, err := BuildConfigFromFile(filePath, cfg)
	if err != nil {
		t.Errorf("Error building complex config file: %v", err)
	}

	var paths []string
	CollectPaths(reflect.ValueOf(cfg), "", &paths)

	if paths[1] != "SeedServers.0.Host" {
		t.Errorf("paths[1] does not contain \"SeedServers.0.Host\"")
	}

	_, _, err = GetByPath(sch, cfg, paths[1])
	if err != nil {
		t.Errorf("Error getting by path: %v", err)
	}

}

func TestGenerateConfigDeltas(t *testing.T) {

	filePath := "../../Configs/cluster/default_cluster_config.conf"
	cfg := InitDefaultClusterConfig()

	sch, err := BuildConfigFromFile(filePath, cfg)
	if err != nil {
		t.Errorf("Error building complex config file: %v", err)
	}

	part := &Participant{
		keyValues: make(map[string]*Delta, 4),
	}

	err = GenerateConfigDeltas(sch, cfg, part)
	if err != nil {
		t.Errorf("Error generating deltas: %v", err)
	}

	if _, exists := part.keyValues[MakeDeltaKey(CONFIG_DKG, "SeedServers.0.Host")]; !exists {
		t.Errorf("expected to get delta")
	}

}

func TestSliceConfigSchema(t *testing.T) {

	cfg := InitDefaultClusterConfig()

	oldPort := "8082"

	cfg.Name = "test-cluster"
	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: "8081"})
	cfg.SeedServers = append(cfg.SeedServers, &Seeds{Host: "localhost", Port: oldPort})

	schema := BuildConfigSchema(cfg)

	newPort := "5000"

	err := SetByPath(schema, cfg, "SeedServers.1.Port", newPort)
	if err != nil {
		t.Errorf("Error setting new name: %v", err)
	}

	if cfg.SeedServers[1].Port != newPort {
		t.Errorf("New port is %s, wanted %s", cfg.SeedServers[1].Port, newPort)
	}

}

func TestSetComplexConfigSchema(t *testing.T) {

	type complexConfig struct {
		Name   string
		Nested struct {
			Numbers []int
			Flags   map[string]bool
		}
		PointerField *struct {
			Value int
		}
		ComplexNest struct {
			MapTest      map[string][]int
			ArrayMapTest []map[string]int
		}
	}

	cfg := complexConfig{
		Name: "test-cluster",
		Nested: struct {
			Numbers []int
			Flags   map[string]bool
		}{
			Numbers: []int{1, 2, 3},
			Flags:   map[string]bool{"foo": true},
		},
		PointerField: &struct{ Value int }{Value: 1},
		ComplexNest: struct {
			MapTest      map[string][]int
			ArrayMapTest []map[string]int
		}{
			MapTest: map[string][]int{
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			ArrayMapTest: []map[string]int{
				{"one": 1},
				{"two": 2},
			},
		},
	}

	schema := BuildConfigSchema(cfg)

	// Make sure the schema was built properly for the map
	if _, ok := schema["ComplexNest.ArrayMapTest.0"]; !ok {
		t.Fatalf("expected schema to contain key 'Cluster.TestNest2.<key>'")
	}

	t.Run("Set nested map inside slice", func(t *testing.T) {

		err := SetByPath(schema, &cfg, "ComplexNest.ArrayMapTest.1.two", 9)
		if err != nil {
			t.Errorf("Error setting value: %v", err)
		}

		newVal := cfg.ComplexNest.ArrayMapTest[1]["two"]

		if newVal != 9 {
			t.Errorf("Expected two to be 9, got %d", newVal)
		}
	})

	t.Run("Set slice inside map value", func(t *testing.T) {

		err := SetByPath(schema, &cfg, "ComplexNest.MapTest.bar.1", 42)
		if err != nil {
			t.Errorf("Error setting value: %v", err)
		}

		newVal := cfg.ComplexNest.MapTest["bar"][1]

		if newVal != 42 {
			t.Errorf("Expected bar[1] to be 42, got %d", newVal)
		}
	})
}

func TestGetComplexConfigSchema(t *testing.T) {

	type complexConfig struct {
		Name   string
		Nested struct {
			Numbers []int
			Flags   map[string]bool
		}
		PointerField *struct {
			Value int
		}
		ComplexNest struct {
			MapTest      map[string][]int
			ArrayMapTest []map[string]int
		}
	}

	cfg := complexConfig{
		Name: "test-cluster",
		Nested: struct {
			Numbers []int
			Flags   map[string]bool
		}{
			Numbers: []int{1, 2, 3},
			Flags:   map[string]bool{"foo": true},
		},
		PointerField: &struct{ Value int }{Value: 1},
		ComplexNest: struct {
			MapTest      map[string][]int
			ArrayMapTest []map[string]int
		}{
			MapTest: map[string][]int{
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			ArrayMapTest: []map[string]int{
				{"one": 1},
				{"two": 2},
			},
		},
	}

	schema := BuildConfigSchema(cfg)

	// Make sure the schema was built properly for the map
	if _, ok := schema["ComplexNest.ArrayMapTest.0"]; !ok {
		t.Fatalf("expected schema to contain key 'Cluster.TestNest2.<key>'")
	}

	t.Run("Get nested map inside slice", func(t *testing.T) {
		valCheck := cfg.ComplexNest.ArrayMapTest[1]["two"]

		val, _, err := GetByPath(schema, &cfg, "ComplexNest.ArrayMapTest.1.two")
		if err != nil {
			t.Errorf("Error getting value: %v", err)
		}

		newVal, ok := val.(int)
		if !ok {
			t.Errorf("Expected value to be an int, got %T", val)
		}

		if newVal != valCheck {
			t.Errorf("Expected two to be %d, got %d", valCheck, newVal)
		}
	})

	t.Run("Get slice inside map value", func(t *testing.T) {
		valCheck := cfg.ComplexNest.MapTest["bar"][0]

		val, _, err := GetByPath(schema, &cfg, "ComplexNest.MapTest.bar.0")
		if err != nil {
			t.Errorf("Error getting value: %v", err)
		}

		newVal, ok := val.(int)
		if !ok {
			t.Errorf("Expected value to be an int, got %T", val)
		}

		if newVal != valCheck {
			t.Errorf("Expected bar[0] to be %d, got %d", valCheck, newVal)
		}
	})
}

func TestBuildComplexConfig(t *testing.T) {

	filePath := "../../Configs/cluster/complex_test_config.conf"

	type complexConfig struct {
		Name   string
		Nested struct {
			Numbers []int
			Flags   map[string]bool
		}
		PointerField *struct {
			Value int
		}
		ComplexNest struct {
			MapTest      map[string][]int
			ArrayMapTest []map[string]int
		}
	}

	cfg := &complexConfig{
		Nested: struct {
			Numbers []int
			Flags   map[string]bool
		}{
			Flags: make(map[string]bool),
		},
		PointerField: &struct{ Value int }{},
		ComplexNest: struct {
			MapTest      map[string][]int
			ArrayMapTest []map[string]int
		}{
			MapTest: make(map[string][]int),
		},
	}

	_, err := BuildConfigFromFile(filePath, cfg)
	if err != nil {
		t.Errorf("Error building complex config file: %v", err)
	}

}

func TestNodeConfigFile(t *testing.T) {

	filePath := "../../Configs/node/basic_seed_config.conf"

	nodeCfg := InitDefaultNodeConfig()

	_, err := BuildConfigFromFile(filePath, nodeCfg)
	if err != nil {
		t.Errorf("Error building complex config file: %v", err)
	}

}

func TestConfigCheckSum(t *testing.T) {

	filePath := "../../Configs/node/basic_seed_config.conf"

	nodeCfg := InitDefaultNodeConfig()
	nodeCfg2 := InitDefaultNodeConfig()

	_, err := BuildConfigFromFile(filePath, nodeCfg)
	if err != nil {
		t.Errorf("Error building complex config file: %v", err)
	}

	cs, err := configChecksum(nodeCfg)
	if err != nil {
		t.Errorf("Error getting config checksum: %v", err)
	}

	cs2, err := configChecksum(nodeCfg2)
	if err != nil {
		t.Errorf("Error getting config checksum: %v", err)
	}

	if cs == cs2 {
		t.Errorf("Config checksums should be different")
	}

}
