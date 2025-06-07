package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Network"
	"log"
	"reflect"
	"strings"
)

// TODO
// Need two top level different configs - cluster config shared by all nodes and gossiped changes
// Node config specific to the local node

//=============================================================
// Server Options + Config
//=============================================================

/*

Config will have two types [CLUSTER] + [INTERNAL]

Cluster will be gossiped and synced across the cluster for uniformity
- Cluster config will make other nodes also change their state

Internal will be local to the node - such as node name or address etc.
- Internal will only affect other nodes in so much as what is reflected in their map

*/

const (
	DEFAULT_MAX_DELTA_SIZE            = DEFAULT_MAX_GSA - 400 // TODO If config not set then we default to this
	DEFAULT_MAX_DISCOVERY_SIZE        = 1024
	DEFAULT_DISCOVERY_PERCENTAGE      = 50
	DEFAULT_MAX_GSA                   = 1400
	DEFAULT_MAX_DELTA_PER_PARTICIPANT = 5
	DEFAULT_PA_WINDOW_SIZE            = 100
	DEFAULT_PA_THRESHOLD              = 8
)

// TODO May want a config mutex lock?? -- Especially if gossip messages will mean our server makes changes to it's config

type GbClusterConfig struct {
	Name        string
	SeedServers []Seeds
	Cluster     *ClusterOptions
}

type Seeds struct {
	Host string
	Port string
}

type ClusterNetworkType int

const (
	C_UNDEFINED ClusterNetworkType = iota
	C_PRIVATE
	C_PUBLIC
	C_DYNAMIC
	C_LOCAL
)

type ClusterOptions struct {
	MaxGossipSize                 uint16
	MaxDeltaSize                  uint16
	MaxDiscoverySize              uint16
	DiscoveryPercentageProportion int
	MaxNumberOfNodes              int
	DefaultSubsetDigestNodes      int
	MaxSequenceIDPool             int
	NodeSelectionPerGossipRound   int
	MaxParticipantHeapSize        int
	PreferredAddrGroup            string
	DiscoveryPercentage           uint8 // from 0 to 100 how much of a percentage a new node should gather address information in discovery mode for based on total number of participants in the cluster
	PaWindowSize                  int
	PaThreshold                   int
	ClusterNetworkType            ClusterNetworkType
	DynamicGossipScaling          bool // Adjusts node selection, delta size, discovery size, etc based on cluster metrics and size
	LoggingURL                    string
	MetricsURL                    string
	ErrorsURL                     string
	RequestIDPool                 uint16

	NodeMTLSRequired   bool
	ClientMTLSRequired bool
}

type NodeNetworkType int

const (
	UNDEFINED NodeNetworkType = iota
	PRIVATE
	PUBLIC
	LOCAL
)

type GbNodeConfig struct {
	Name        string
	ID          uint32
	Host        string
	Port        string
	NetworkType NodeNetworkType
	ClientPort  string
	Internal    *InternalOptions
}

type InternalOptions struct {
	//
	IsTestMode                            bool
	DisableInitialiseSelf                 bool
	DisableGossip                         bool
	GoRoutineTracking                     bool
	DebugMode                             bool
	DisableInternalGossipSystemUpdate     bool
	DisableUpdateServerTimeStampOnStartup bool
	NodeSelection                         uint8

	// TLS
	CACertFilePath string
	CertFilePath   string
	KeyFilePath    string

	// Need logging config also
}

//=====================================================================
// Extract field names to use as delta keys
//=====================================================================

// The reason why we map these functions up-front is that reflect is quite expensive and to keep calling it at
// Runtime will slow us down - therefore we do it once at compile time and store the functions
// With this we (mostly) avoid reflect

type clusterConfigSetterMapFunc func(any) error
type clusterConfigGetterMapFunc func(string) (uint8, any, error)

func getConfigFields(v reflect.Value, prefix string) []string {

	//v := reflect.ValueOf(gb)

	var names []string

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	//names := make([]string, t.NumField())
	for i := range v.NumField() {

		field := v.Field(i)
		structField := v.Type().Field(i)

		fieldName := structField.Name
		fullPath := fieldName

		if prefix != "" {
			fullPath = fmt.Sprintf("%s.%s", prefix, fieldName)
		}

		if field.Kind() == reflect.Ptr {
			field = field.Elem()
		}

		if field.Kind() == reflect.Struct {
			subFields := getConfigFields(field, fullPath)
			names = append(names, subFields...)
			continue
		}

		names = append(names, fullPath)

	}

	return names

}

func buildConfigSetters(cfg *GbClusterConfig, paths []string) map[string]clusterConfigSetterMapFunc {
	setters := make(map[string]clusterConfigSetterMapFunc, len(paths))

	for _, path := range paths {
		keys := strings.Split(path, ".")

		// Traverse ONCE during setup
		v := reflect.ValueOf(cfg).Elem()
		for i := 0; i < len(keys)-1; i++ {
			v = v.FieldByName(keys[i])
			if v.Kind() == reflect.Ptr {
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				v = v.Elem()
			}
		}

		lastKey := keys[len(keys)-1]
		field := v.FieldByName(lastKey)

		if !field.IsValid() {
			continue
		}

		f := field // Re-capture to avoid variable loop closures

		switch f.Kind() {
		case reflect.String:
			setters[path] = func(val any) error {
				strVal, ok := val.(string)
				if !ok {
					return fmt.Errorf("expected string")
				}
				f.SetString(strVal)
				return nil
			}
		case reflect.Int, reflect.Int64:
			setters[path] = func(val any) error {
				intVal, ok := val.(int)
				if !ok {
					return fmt.Errorf("expected int")
				}
				field.SetInt(int64(intVal))
				return nil
			}

		case reflect.Uint8:
			setters[path] = func(val any) error {
				intVal, ok := val.(uint8)
				if !ok {
					return fmt.Errorf("expected uint8")
				}
				f.SetUint(uint64(intVal))
				return nil
			}

		case reflect.Uint16:
			setters[path] = func(val any) error {
				intVal, ok := val.(uint16)
				if !ok {
					return fmt.Errorf("expected uint8")
				}
				f.SetUint(uint64(intVal))
				return nil
			}

		case reflect.Bool:
			setters[path] = func(val any) error {
				boolVal, ok := val.(bool)
				if !ok {
					return fmt.Errorf("expected bool")
				}
				f.SetBool(boolVal)
				return nil
			}

		}
	}

	return setters
}

func buildConfigGetters(cfg *GbClusterConfig, paths []string) map[string]clusterConfigGetterMapFunc {

	//getters := make(map[string]clusterConfigGetterMapFunc, len(paths))
	getters := make(map[string]clusterConfigGetterMapFunc, len(paths))

	for _, path := range paths {
		keys := strings.Split(path, ".")

		// Traverse ONCE during setup
		v := reflect.ValueOf(cfg).Elem()
		for i := 0; i < len(keys)-1; i++ {
			v = v.FieldByName(keys[i])
			if v.Kind() == reflect.Ptr {
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				v = v.Elem()
			}
		}

		lastKey := keys[len(keys)-1]
		field := v.FieldByName(lastKey)

		if !field.IsValid() {
			continue
		}

		f := field

		switch f.Kind() {

		case reflect.String:
			getters[path] = func(s string) (uint8, any, error) {
				return D_STRING_TYPE, f.Interface(), nil
			}

		case reflect.Int:
			getters[path] = func(s string) (uint8, any, error) {
				return D_INT_TYPE, f.Interface(), nil
			}

		case reflect.Int8:
			getters[path] = func(s string) (uint8, any, error) {
				return D_INT8_TYPE, f.Interface(), nil
			}

		case reflect.Int16:
			getters[path] = func(s string) (uint8, any, error) {
				return D_INT16_TYPE, f.Interface(), nil
			}

		case reflect.Int32:
			getters[path] = func(s string) (uint8, any, error) {
				return D_INT32_TYPE, f.Interface(), nil
			}

		case reflect.Int64:
			getters[path] = func(s string) (uint8, any, error) {
				return D_INT64_TYPE, f.Interface(), nil
			}

		case reflect.Uint8:
			getters[path] = func(s string) (uint8, any, error) {
				return D_UINT8_TYPE, f.Interface(), nil
			}

		case reflect.Uint16:
			getters[path] = func(s string) (uint8, any, error) {
				return D_UINT16_TYPE, f.Interface(), nil
			}

		case reflect.Uint32:
			getters[path] = func(s string) (uint8, any, error) {
				return D_UINT32_TYPE, f.Interface(), nil
			}

		case reflect.Uint64:
			getters[path] = func(s string) (uint8, any, error) {
				return D_UINT64_TYPE, f.Interface(), nil
			}

		case reflect.Bool:
			getters[path] = func(s string) (uint8, any, error) {
				return D_BOOL_TYPE, f.Interface(), nil
			}

		default:
			getters[path] = func(s string) (uint8, any, error) {
				return D_INT8_TYPE, f.Interface(), nil
			}

		}

	}

	return getters
}

// TODO Need to modify to handle array types and return index and value
func encodeGetterValue(val any) ([]byte, error) {

	switch v := val.(type) {
	case string:
		return []byte(v), nil
	case int8:
		buf := make([]byte, 1)
		buf[0] = byte(v)
		return buf, nil
	case int16:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(v))
		return buf, nil
	case int32:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))
		return buf, nil
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case uint8:
		buf := make([]byte, 1)
		buf[0] = byte(v)
		return buf, nil
	case uint16:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(v))
		return buf, nil
	case uint32:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))
		return buf, nil
	case uint64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case bool:
		buf := make([]byte, 1)
		if v {
			buf[0] = 0x01
		} else {
			buf[0] = 0x00
		}
		return buf, nil
	case int:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(v))
		return buf, nil
	case ClusterNetworkType:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(v))
		return buf, nil
	case []Seeds:
		log.Println("GOT EM")
		return nil, nil
	}

	return nil, fmt.Errorf("unknown type %T", val)

}

//TODO Now need to handle tracking our config state in deltas - (once server is live we do not reflect on config)
// - when we change a config state, we update our delta which we then gossip
// - if we detect a newer version delta config we change our state (need to carefully think about this)

//=====================================================================

func ParseNodeNetworkType(s string) (NodeNetworkType, error) {

	st := strings.TrimSpace(s)
	st = strings.ToUpper(st)

	switch st {
	case "UNDEFINED":
		return UNDEFINED, nil
	case "PRIVATE":
		return PRIVATE, nil
	case "PUBLIC":
		return PUBLIC, nil
	case "LOCAL":
		return LOCAL, nil
	}

	return UNDEFINED, fmt.Errorf("invalid node network type: %s", st)

}

func ParseClusterNetworkType(s string) (ClusterNetworkType, error) {

	st := strings.TrimSpace(s)
	st = strings.ToUpper(st)

	switch st {
	case "UNDEFINED":
		return C_UNDEFINED, nil
	case "PRIVATE":
		return C_PRIVATE, nil
	case "PUBLIC":
		return C_PUBLIC, nil
	case "LOCAL":
		return C_LOCAL, nil
	}

	return C_UNDEFINED, fmt.Errorf("invalid cluster network type: %s", st)

}

func ParseClusterConfigNetworkType(netType string) (ClusterNetworkType, error) {

	nt := strings.Trim(netType, " ")
	nt = strings.ToUpper(nt)

	switch nt {
	case "UNDEFINED":
		return C_UNDEFINED, nil
	case "PRIVATE":
		return C_PRIVATE, nil
	case "PUBLIC":
		return C_PUBLIC, nil
	case "DYNAMIC":
		return C_DYNAMIC, nil
	case "LOCAL":
		return C_LOCAL, nil
	}

	return 0, fmt.Errorf("invalid network type: %s", nt)

}

//TODO Need config initializer here to set values and any defaults needed

// TODO need update functions and methods for when server runs background processes to update config based on gossip

func ConfigInitialNetworkCheck(cluster *GbClusterConfig, node *GbNodeConfig, reach Network.NodeNetworkReachability) error {

	c := cluster.Cluster.ClusterNetworkType
	n := node.NetworkType

	switch c {
	case C_UNDEFINED:
		return fmt.Errorf("cluster network type cannot be undefined")
	case C_PUBLIC:
		if n == PRIVATE {
			return fmt.Errorf("private node trying to join on a public only cluster")
		} else {
			switch reach {
			case Network.LoopbackOnly:
				return fmt.Errorf("loopback node trying to join on a public cluster")
			case Network.PrivateOnly:
				return fmt.Errorf("private node trying to join on a public cluster")
			case Network.NATMapped:
				return fmt.Errorf("NATMapped node trying to join on a public cluster")
			case Network.RelayRequired:
				return fmt.Errorf("relay node trying to join on a public cluster")
			case Network.Unreachable:
				return nil
			case Network.PublicUnverified:
				return nil
			case Network.PublicReachable:
				return nil
			case Network.PublicOpen:
				return nil
			}
		}

	case C_PRIVATE:
		if n == PUBLIC {
			return fmt.Errorf("public node trying to join on a private only cluster")
		} else {
			switch reach {
			case Network.LoopbackOnly:
				return fmt.Errorf("loopback node trying to join on a private cluster")
			case Network.PrivateOnly:
				return nil
			case Network.NATMapped:
				return nil
			case Network.RelayRequired:
				return fmt.Errorf("relay node trying to join on a private cluster")
			case Network.Unreachable:
				return nil
			case Network.PublicReachable:
				return fmt.Errorf("public node trying to join on a private cluster")
			case Network.PublicOpen:
				return fmt.Errorf("public node trying to join on a private cluster")
			case Network.PublicUnverified:
				return fmt.Errorf("public node trying to join on a private cluster")
			}
		}
	case C_DYNAMIC:
		switch reach {
		case Network.LoopbackOnly:
			return fmt.Errorf("loopback node trying to join on a non-local cluster")
		default:
			return nil
		}
	case C_LOCAL:
		switch reach {
		case Network.LoopbackOnly:
			return nil
		default:
			return fmt.Errorf("non-loopback node trying to join on a local cluster")
		}
	}

	return fmt.Errorf("unknown cluster network type - %d", c)

}

// Need to do a further config check for LOCAL and loopback
// TODO Encapsulate as much as we can into a validateConfig() check so we can pass a cluster config in and return on any errors
