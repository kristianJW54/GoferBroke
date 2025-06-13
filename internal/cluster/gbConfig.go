package cluster

import (
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Network"
	"log"
	"reflect"
	"strconv"
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

//=============================================================
// Cluster
//=============================================================

const (
	DEFAULT_MAX_DELTA_SIZE            = DEFAULT_MAX_GSA - 400
	DEFAULT_MAX_DISCOVERY_SIZE        = uint16(1024)
	DEFAULT_DISCOVERY_PERCENTAGE      = uint8(50)
	DEFAULT_MAX_GSA                   = uint16(1400)
	DEFAULT_MAX_DELTA_PER_PARTICIPANT = uint8(5)
	DEFAULT_PA_WINDOW_SIZE            = uint16(100)
	DEFAULT_NODE_SELECTION_PER_ROUND  = uint8(1)
	DEFAULT_MAX_GOSSIP_SIZE           = uint16(2048)
	DEFAULT_MAX_NUMBER_OF_NODES       = uint32(1000)
	DEFAULT_SEQUENCE_ID_POOL          = uint32(100)
)

// TODO May want a config mutex lock?? -- Especially if gossip messages will mean our server makes changes to it's config

type GbClusterConfig struct {
	Name        string
	SeedServers []*Seeds
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
	NodeSelectionPerGossipRound    uint8
	DiscoveryPercentage            uint8 // from 0 to 100 how much of a percentage a new node should gather address information in discovery mode for based on total number of participants in the cluster
	MaxDeltaGossipedPerParticipant uint8
	MaxGossipSize                  uint16
	MaxDeltaSize                   uint16
	MaxDiscoverySize               uint16
	PaWindowSize                   uint16
	MaxGossSynAck                  uint16
	MaxNumberOfNodes               uint32
	MaxSequenceIDPool              uint32
	ClusterNetworkType             ClusterNetworkType
	DynamicGossipScaling           bool // Adjusts node selection, delta size, discovery size, etc based on cluster metrics and size
	LoggingURL                     string
	MetricsURL                     string
	ErrorsURL                      string
	TestNest                       []map[string]int
	TestNest2                      map[string][]int

	NodeMTLSRequired   bool `default:"false"`
	ClientMTLSRequired bool `default:"false"`
}

func InitDefaultClusterConfig() *GbClusterConfig {

	return &GbClusterConfig{
		Name:        "",
		SeedServers: make([]*Seeds, 0, 4),
		Cluster: &ClusterOptions{
			NodeSelectionPerGossipRound:    DEFAULT_NODE_SELECTION_PER_ROUND,
			DiscoveryPercentage:            DEFAULT_DISCOVERY_PERCENTAGE,
			MaxDeltaGossipedPerParticipant: DEFAULT_MAX_DELTA_PER_PARTICIPANT,
			MaxGossipSize:                  DEFAULT_MAX_GOSSIP_SIZE,
			MaxDeltaSize:                   DEFAULT_MAX_DELTA_SIZE,
			MaxDiscoverySize:               DEFAULT_MAX_DISCOVERY_SIZE,
			PaWindowSize:                   DEFAULT_PA_WINDOW_SIZE,
			MaxGossSynAck:                  DEFAULT_MAX_GSA,
			MaxNumberOfNodes:               DEFAULT_MAX_NUMBER_OF_NODES,
			MaxSequenceIDPool:              DEFAULT_SEQUENCE_ID_POOL,
			ClusterNetworkType:             C_UNDEFINED,
			DynamicGossipScaling:           false,
			LoggingURL:                     "",
			MetricsURL:                     "",
			ErrorsURL:                      "",
			NodeMTLSRequired:               false,
			ClientMTLSRequired:             false,
		},
	}

}

//=============================================================

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

	// TLS
	CACertFilePath string
	CertFilePath   string
	KeyFilePath    string

	// Need logging config also
}

//=====================================================================
// Config Schema
//=====================================================================

// The reason why we map these functions up-front is that reflect is quite expensive and to keep calling it at
// Runtime will slow us down - therefore we do it once at compile time and store the functions
// With this we (mostly) avoid reflect

type clusterConfigSetterMapFunc func(any) error
type clusterConfigGetterMapFunc func() any

type ConfigSchema struct {
	Path      string
	Type      reflect.Type
	Kind      reflect.Kind
	isIndexed bool
	isSlice   bool
	isMap     bool
	isPtr     bool
	elemType  *ConfigSchema
	Getter    clusterConfigGetterMapFunc
	Setter    clusterConfigSetterMapFunc
	//Custom Encode + Decode to be included?
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

func isIndexedPath(path string) bool {
	parts := strings.Split(path, ".")
	for _, p := range parts {
		if _, err := strconv.Atoi(p); err == nil {
			return true // it's an index (e.g. "0", "1")
		}
	}
	return false
}

func BuildConfigSchema(cfg *GbClusterConfig) map[string]*ConfigSchema {

	out := make(map[string]*ConfigSchema)
	buildSchemaRecursive(reflect.ValueOf(&cfg).Elem(), "", out)
	return out

}

func buildSchemaRecursive(v reflect.Value, parent string, out map[string]*ConfigSchema) {

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	t := v.Type()

	// Loop through each field of the struct
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)
		path := joinPath(parent, field.Name)
		kind := field.Type.Kind()

		switch kind {

		case reflect.Struct:

			buildSchemaRecursive(fieldValue, path, out)
		case reflect.Ptr:
			if field.Type.Elem().Kind() == reflect.Struct {
				if fieldValue.IsNil() {
					fieldValue.Set(reflect.New(field.Type.Elem()))
				}

				buildSchemaRecursive(fieldValue, path, out)
			} else {
				out[path] = &ConfigSchema{
					Path:      path,
					Type:      field.Type.Elem(),
					Kind:      field.Type.Elem().Kind(),
					isIndexed: isIndexedPath(path),
					isPtr:     true,
					Setter:    makeDirectSetter(fieldValue),
					Getter:    makeDirectGetter(fieldValue),
				}
			}

		case reflect.Slice:

			elemType := fieldValue.Type().Elem() // Example -> *Seeds
			elemKind := elemType.Kind()          // Example -> reflect.Ptr

			// Capture the container

			out[path] = &ConfigSchema{
				Path:      path,
				Type:      field.Type,
				Kind:      field.Type.Kind(),
				isIndexed: false,
				isSlice:   true,
				isPtr:     elemKind == reflect.Ptr,
				Setter:    nil,
				Getter:    nil,
			}

			indexPath := path + ".0" // Example -> "SeedServers.0"

			if elemKind == reflect.Ptr && elemType.Elem().Kind() == reflect.Struct {
				//Ensure we have one element, so we can walk into it
				if fieldValue.Len() == 0 {
					newElem := reflect.New(field.Type.Elem())
					fieldValue.Set(reflect.Append(fieldValue, newElem))
				}

				firstElem := fieldValue.Index(0)
				if firstElem.Kind() == reflect.Ptr && firstElem.IsNil() {
					firstElem.Set(reflect.New(field.Type.Elem()))
				}

				out[indexPath] = &ConfigSchema{
					Path:      indexPath,
					Type:      elemType.Elem(),
					Kind:      elemType.Elem().Kind(),
					isIndexed: false,
					isSlice:   false,
					isPtr:     elemKind == reflect.Ptr,
					Setter:    nil,
					Getter:    nil,
				}

				buildSchemaRecursive(firstElem, indexPath, out)
			} else {

				out[indexPath] = &ConfigSchema{
					Path:      indexPath,
					Type:      field.Type.Elem(),
					Kind:      field.Type.Elem().Kind(),
					isIndexed: true,
					isSlice:   true,
					Setter:    nil,
					Getter:    nil,
				}

			}

		case reflect.Map:

			out[path] = &ConfigSchema{
				Path:      path,
				Type:      field.Type,
				Kind:      field.Type.Elem().Kind(),
				isIndexed: false,
				isMap:     true,
				Setter:    nil,
				Getter:    nil,
			}

			if field.Type.Key().Kind() == reflect.String {
				mapPath := path + ".<key>"
				out[mapPath] = &ConfigSchema{
					Path:      mapPath,
					Type:      field.Type.Key(),
					Kind:      field.Type.Key().Kind(),
					isIndexed: true,
					isMap:     true,
					Getter:    nil,
					Setter:    nil,
				}
			}

		default:
			isIndexed := isIndexedPath(path)
			if !isIndexed {
				out[path] = &ConfigSchema{
					Path:      path,
					Type:      field.Type,
					Kind:      kind,
					isIndexed: isIndexed,
					Setter:    makeDirectSetter(fieldValue),
					Getter:    makeDirectGetter(fieldValue),
				}

			} else {
				out[path] = &ConfigSchema{
					Path:      path,
					Type:      field.Type,
					Kind:      kind,
					isIndexed: isIndexed,
					Setter:    nil,
					Getter:    nil,
				}

			}

		}

	}

}

func deref(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}
	if v.Kind() == reflect.Ptr {
		return v.Elem()
	}
	return v
}

// Say we are trying to set "Seedservers.0.Host"
// We have validated the schema and know it exist and what type/kind it is

func SetByPath(sch map[string]*ConfigSchema, config *GbClusterConfig, path string, value any) error {

	parts := strings.Split(path, ".") // e.g. SeedServers.1.Host -> ["SeedServers", "1", "Host"]

	var prefix string // Buildup a path as we go to access the schema at each point

	index := ".0"
	key := ".<key>"

	current := reflect.ValueOf(config)
	if current.Kind() == reflect.Ptr {
		current = current.Elem()
	}

	for i := 0; i < len(parts); i++ {

		log.Printf("processing part = %s", parts[i])

		if prefix == "" {
			prefix = parts[i]
		}

		schema, ok := sch[prefix]
		if !ok {
			return fmt.Errorf(`prefix "%s" not exist`, prefix)
		}

		// If we are at the last part...
		if i == len(parts)-1 {
			schemaField := parts[i]

			field := current.FieldByName(schemaField)
			if !field.IsValid() {
				return fmt.Errorf("field '%s' not found in struct '%s'", schemaField, prefix)
			}

			field = deref(field)

			val := reflect.ValueOf(value)
			if !val.Type().AssignableTo(field.Type()) {
				return fmt.Errorf("cannot assign %T to %s", value, field.Type())
			}

			field.Set(val)
			return nil
		}

		switch schema.Kind {
		case reflect.Slice:
			prefix += index
			// Do work

			if len(parts) <= i+1 {
				return fmt.Errorf("unexpected end of path after '%s' at position %d — expected index or key", parts[i], i)
			}

			field := parts[i]

			i++ // Consume the index in the loop, so we don't process it again

			idxPart := parts[i]
			idx, err := strconv.Atoi(idxPart)
			if err != nil {
				return err
			}

			// Get the slice
			slice := current.FieldByName(field)
			slice = deref(slice)
			if slice.Kind() != reflect.Slice {
				return fmt.Errorf("field '%s' is not a slice", field)
			}

			for slice.Len() <= idx {
				elem := reflect.New(slice.Type().Elem())
				slice = reflect.Append(slice, elem)
			}
			current.FieldByName(field).Set(slice)
			current = slice.Index(idx)
			current = deref(current)

		case reflect.Map:

			prefix += key

			if len(parts) <= i+1 {
				return fmt.Errorf("unexpected end of path after '%s' at position %d — expected index or key", parts[i], i)
			}
			part := parts[i]

			i++ // Consume the index in the loop, so we don't process the map key again

			mapKey := reflect.ValueOf(parts[i]) // Example "Foo" --- prefix should be "TestNest.0.<key>"

			// Get the map
			m := current.FieldByName(part)
			m = deref(m)
			if m.Kind() != reflect.Map {
				return fmt.Errorf("field '%s' is not a map", part)
			}

			val := m.MapIndex(mapKey)
			if !val.IsValid() {
				// Key doesn't exist — create new element
				val = reflect.New(m.Type().Elem()).Elem()
				m.SetMapIndex(mapKey, val)
			}

			//Move current into the map value
			current = val
			current = deref(current)

		case reflect.Struct:

			prefix += "." + parts[i]

			field := current.FieldByName(parts[i])

			if !field.IsValid() {
				return fmt.Errorf("field '%s' not found in struct", parts[i])
			}

			current = deref(field)

		default:
			return fmt.Errorf("unsupported kind %s at '%s'", schema.Kind, prefix)

		}

	}

	return nil

}

// For getters and setters - we only cache functions on primitives which are simple and predictable
// more complex types like slices/maps etc. we will use a runtime reflection which will be able to extract indexes
// walk to the value and set
// To do this, we will access the schema for the respective field we are trying to get/set and check the type
// if it is a complex type we know it will not have cached function, and we can call a runtime function to handle

func makeDirectSetter(fv reflect.Value) clusterConfigSetterMapFunc {
	if fv.Kind() == reflect.Ptr {
		if fv.IsNil() {
			fv.Set(reflect.New(fv.Type().Elem()))
		}
		fv = fv.Elem()
	}

	return func(value any) error {
		val := reflect.ValueOf(value)
		if !val.Type().AssignableTo(fv.Type()) {
			return fmt.Errorf("cannot assign %T to %s", value, fv.Type())
		}
		fv.Set(val)
		return nil
	}
}

func makeDirectGetter(fv reflect.Value) clusterConfigGetterMapFunc {
	if fv.Kind() == reflect.Ptr {
		if fv.IsNil() {
			fv.Set(reflect.New(fv.Type().Elem()))
		}
		fv = fv.Elem()
	}

	return func() any {
		return fv.Interface()
	}
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
