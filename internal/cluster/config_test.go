package cluster

import (
	"log"
	"reflect"
	"testing"
	"time"
)

func TestGetConfigFieldNames(t *testing.T) {

	testConfig := &GbClusterConfig{
		Name: "test-name",
		Cluster: &ClusterOptions{
			MaxDeltaSize:     20,
			MaxDiscoverySize: 1024,
		},
	}

	getConfigFields(reflect.ValueOf(testConfig), "")

}

func TestConfigSetters(t *testing.T) {

	testConfig := &GbClusterConfig{
		Name: "test-name",
		Cluster: &ClusterOptions{
			MaxDeltaSize:     20,
			MaxDiscoverySize: 1024,
		},
	}

	names := getConfigFields(reflect.ValueOf(testConfig), "")

	setters := buildConfigSetters(testConfig, names)

	if _, ok := setters["Name"]; !ok {
		t.Error("Name not found in setters")
	}

	err := setters["Name"]("hello-there")
	if err != nil {
		t.Fatalf("failed to set Name: %v", err)
	}
	//if testConfig.Name != "updated-node" {
	//	t.Errorf("expected Name to be updated-node, got %s", testConfig.Name)
	//}

	log.Printf("testConfig name -> %s", testConfig.Name)

}

func TestConfigSettersWithDelta(t *testing.T) {

	testConfig := &GbClusterConfig{
		Name: "test-name",
		Cluster: &ClusterOptions{
			MaxDeltaSize:     20,
			MaxDiscoverySize: 1024,
			LoggingURL:       "www.example.com",
		},
	}

	names := getConfigFields(reflect.ValueOf(testConfig), "")

	setters := buildConfigSetters(testConfig, names)

	if _, ok := setters["Name"]; !ok {
		t.Error("Name not found in setters")
	}

	delta := &Delta{
		KeyGroup:  CONFIG_DKG,
		Key:       "Cluster.LoggingURL",
		Version:   time.Now().Unix(),
		ValueType: STRING_V,
		Value:     []byte("www.opensourceftw.com"),
	}

	kv := make(map[string]*Delta, 1)
	kv["Cluster.LoggingURL"] = delta

	setter, ok := setters[delta.Key]
	if !ok {
		t.Fatal("Delta not found in setters")
	}

	err := setter(string(delta.Value))
	if err != nil {
		t.Fatalf("failed to set Delta: %v", err)
	}

	log.Printf("testConfig loggingURL -> %s", testConfig.Cluster.LoggingURL)

}

func TestConfigSettersAndGetters(t *testing.T) {

	testConfig := &GbClusterConfig{
		Name: "test-name",
		Cluster: &ClusterOptions{
			MaxDeltaSize:     20,
			MaxDiscoverySize: 1024,
			LoggingURL:       "www.example.com",
		},
	}

	names := getConfigFields(reflect.ValueOf(testConfig), "")

	setters := buildConfigSetters(testConfig, names)
	getters := buildConfigGetters(testConfig, names)

	if _, ok := setters["Name"]; !ok {
		t.Error("Name not found in setters")
	}

	delta := &Delta{
		KeyGroup:  CONFIG_DKG,
		Key:       "Cluster.LoggingURL",
		Version:   time.Now().Unix(),
		ValueType: STRING_V,
		Value:     []byte("www.opensourceftw.com"),
	}

	kv := make(map[string]*Delta, 1)
	kv["Cluster.LoggingURL"] = delta

	setter, ok := setters[delta.Key]
	if !ok {
		t.Fatal("Delta not found in setters")
	}

	err := setter(string(delta.Value))
	if err != nil {
		t.Fatalf("failed to set Delta: %v", err)
	}

	log.Printf("testConfig loggingURL -> %s", testConfig.Cluster.LoggingURL)

	// Now to test getter

	if _, ok := getters["Name"]; !ok {
		t.Error("Name not found in getters")
	}

	getter, ok := getters[delta.Key]
	if !ok {
		t.Fatal("Delta not found in getters")
	}

	typ, val, err := getter(delta.Key)
	if err != nil {
		t.Fatalf("failed to get delta value: %v", err)
	}

	log.Printf("testConfig getter -> (%v-%s)", typ, val)

}
