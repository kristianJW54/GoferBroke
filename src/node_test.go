package src

import (
	"log"
	"slices"
	"testing"
)

func TestBuildAddrMap(t *testing.T) {

	gbs := GenerateDefaultTestServer(multipleAddressTestingKVs, 5)

	testStruct := []struct {
		name         string
		known        []string
		config       GbConfig
		addrMapCheck map[string][]string
	}{
		{
			name: "no-config tcp only",
			known: []string{
				gbs.clusterMap.participantArray[1],
				gbs.clusterMap.participantArray[2],
			},
			config: GbConfig{
				SeedServers: make(map[string]Seeds),
				Internal: &InternalOptions{
					addressKeys: nil,
				},
				Cluster: &ClusterOptions{},
			},
			addrMapCheck: map[string][]string{
				gbs.clusterMap.participantArray[0]: {
					_ADDRESS_,
				},
				gbs.clusterMap.participantArray[3]: {
					_ADDRESS_,
				},
				gbs.clusterMap.participantArray[4]: {
					_ADDRESS_,
				},
			},
		},
		{
			name: "extra address in config",
			known: []string{
				gbs.clusterMap.participantArray[1],
				gbs.clusterMap.participantArray[3],
			},
			config: GbConfig{
				SeedServers: make(map[string]Seeds),
				Internal: &InternalOptions{
					addressKeys: []string{
						"CLOUD",
					},
				},
				Cluster: &ClusterOptions{},
			},
			addrMapCheck: map[string][]string{
				gbs.clusterMap.participantArray[0]: {
					_ADDRESS_,
					"CLOUD",
				},
				gbs.clusterMap.participantArray[2]: {
					_ADDRESS_,
					"CLOUD",
				},
				gbs.clusterMap.participantArray[4]: {
					_ADDRESS_,
					"CLOUD",
				},
			},
		},
	}

	for _, tt := range testStruct {
		t.Run(tt.name, func(t *testing.T) {

			gbs.gbConfig = &tt.config

			am, err := gbs.buildAddrGroupMap(tt.known)
			if err != nil {
				t.Error(err)
			}

			for name, value := range am {
				log.Printf("name: %s", name)
				if _, exists := tt.addrMapCheck[name]; !exists {
					t.Errorf("%s not found in addr map", name)
				}
				for _, addr := range value {
					if !slices.Contains(tt.addrMapCheck[name], addr) {
						t.Errorf("%s not found in addr map", name)
					}
					log.Println("addr:", addr)
				}
			}

		})
	}
}

func TestPercDiff(t *testing.T) {

	connCount := 3
	knownCount := 1

	res := percMakeup(knownCount, connCount)

	log.Println(res)

}
