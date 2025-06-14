package cluster

import (
	"errors"
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
			TestNest:                       make([]map[string]int, 0, 2),
			TestNest2:                      make(map[string][]int),
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

			fv := deref(fieldValue)

			log.Printf("path = %s", path)

			// Register the container node (e.g., "Cluster")
			out[path] = &ConfigSchema{
				Path:      path,
				Type:      fv.Type(),
				Kind:      fv.Kind(),
				isIndexed: isIndexedPath(path),
				isPtr:     fieldValue.Kind() == reflect.Ptr,
				Setter:    makeDirectSetter(fieldValue),
				Getter:    makeDirectGetter(fieldValue),
			}

			buildSchemaRecursive(fieldValue, path, out)
		case reflect.Ptr:

			elemKind := field.Type.Elem().Kind()

			// Ensure it's initialized
			if fieldValue.IsNil() {
				fieldValue.Set(reflect.New(field.Type.Elem()))
			}

			out[path] = &ConfigSchema{
				Path:      path,
				Type:      field.Type.Elem(),
				Kind:      elemKind,
				isIndexed: isIndexedPath(path),
				isPtr:     true,
				Setter:    makeDirectSetter(fieldValue),
				Getter:    makeDirectGetter(fieldValue),
			}

			if elemKind == reflect.Struct {
				buildSchemaRecursive(fieldValue.Elem(), path, out)
			}

		case reflect.Slice:

			elemType := field.Type.Elem()
			elemKind := elemType.Kind()
			indexPath := path + ".0" // Example -> "SeedServers.0"

			// Register the slice itself
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

			// Ensure at least one element exists for introspection
			if fieldValue.Len() == 0 {
				var newElem reflect.Value
				if elemKind == reflect.Ptr {
					newElem = reflect.New(elemType.Elem())
				} else {
					newElem = reflect.New(elemType).Elem()
				}
				fieldValue.Set(reflect.Append(fieldValue, newElem))
			}

			// Get the first element for schema traversal
			firstElem := fieldValue.Index(0)
			firstElem = deref(firstElem)

			// Register the .0 path
			out[indexPath] = &ConfigSchema{
				Path:      indexPath,
				Type:      firstElem.Type(),
				Kind:      firstElem.Kind(),
				isIndexed: true,
				isSlice:   true,
				isPtr:     elemKind == reflect.Ptr,
				Setter:    nil,
				Getter:    nil,
			}

			// Recurse if element is a struct or map
			switch firstElem.Kind() {
			case reflect.Struct:
				buildSchemaRecursive(firstElem, indexPath, out)

			case reflect.Map:
				// Add <key> mapping
				mapPath := indexPath + ".<key>"
				out[mapPath] = &ConfigSchema{
					Path:      mapPath,
					Type:      firstElem.Type().Elem(),
					Kind:      firstElem.Type().Elem().Kind(),
					isMap:     true,
					isIndexed: true,
					Setter:    nil,
					Getter:    nil,
				}
			}

		case reflect.Map:
			// Capture the container
			out[path] = &ConfigSchema{
				Path:      path,
				Type:      field.Type,
				Kind:      field.Type.Kind(),
				isIndexed: false,
				isMap:     true,
				Setter:    nil,
				Getter:    nil,
			}

			keyKind := field.Type.Key().Kind()
			valType := field.Type.Elem()
			valKind := valType.Kind()

			if keyKind == reflect.String {
				mapPath := path + ".<key>"

				// Add schema entry for accessing values via key
				out[mapPath] = &ConfigSchema{
					Path:      mapPath,
					Type:      valType,
					Kind:      valKind,
					isIndexed: true,
					isMap:     true,
					Setter:    nil,
					Getter:    nil,
				}

				// If value is pointer to struct or struct, recurse
				switch valKind {
				case reflect.Ptr:
					if valType.Elem().Kind() == reflect.Struct {
						elem := reflect.New(valType.Elem()).Elem()
						buildSchemaRecursive(elem, mapPath, out)
					}

				case reflect.Struct:
					elem := reflect.New(valType).Elem()
					buildSchemaRecursive(elem, mapPath, out)
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
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			if v.CanSet() {
				v.Set(reflect.New(v.Type().Elem()))
			} else {
				// Return zero value so downstream doesn't panic
				return reflect.Zero(v.Type().Elem())
			}
		}
		return v.Elem()
	}
	return v
}

type configState struct {
	current reflect.Value
	prefix  string
	index   int
	parts   []string
	schema  map[string]*ConfigSchema
	value   any
}

type configStateFunc func(*configState) (configStateFunc, error)

// Say we are trying to set "Seedservers.0.Host"
// We have validated the schema and know it exist and what type/kind it is

func SetByPath(sch map[string]*ConfigSchema, config any, path string, value any) error {

	parts := strings.Split(path, ".")
	val := reflect.ValueOf(config)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return errors.New("config must be a non-nil pointer")
	}

	val = val.Elem()

	s := &configState{
		current: val,
		prefix:  "",
		index:   0,
		parts:   parts,
		schema:  sch,
		value:   value,
	}

	fn := handleStruct
	var err error

	for fn != nil {
		fn, err = fn(s)
		if err != nil {
			return err
		}
	}

	return nil

}

func handleStruct(s *configState) (configStateFunc, error) {
	if s.index >= len(s.parts) {
		return nil, nil
	}

	fieldName := s.parts[s.index]
	field := s.current.FieldByName(fieldName)
	if !field.IsValid() {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}

	// Move prefix update here
	if s.prefix != "" {
		s.prefix += "." + fieldName
	} else {
		s.prefix = fieldName
	}

	sch, ok := s.schema[s.prefix]
	if !ok {
		return nil, fmt.Errorf("schema missing for '%s'", s.prefix)
	}

	// Then proceed
	s.current = deref(field)
	s.index++

	if s.index == len(s.parts) {
		val := reflect.ValueOf(s.value)
		if !val.Type().AssignableTo(s.current.Type()) {
			return nil, fmt.Errorf("value of %s is not assignable to %s", val.Type(), s.current.Type())
		}
		s.current.Set(val)
		return nil, nil
	}

	switch sch.Kind {
	case reflect.Struct:
		log.Printf("calling struct with prefix %s", s.prefix)
		return handleStruct, nil
	case reflect.Slice:
		log.Printf("calling slice with prefix %s", s.prefix)
		return handleSlice, nil
	case reflect.Map:
		log.Printf("struct calling map with prefix %s", s.prefix)
		return handleMap, nil
	default:
		return nil, fmt.Errorf("unsupported kind: %v", sch.Kind)
	}
}

func handleSlice(s *configState) (configStateFunc, error) {
	// s.current is already the slice value
	slice := deref(s.current)

	if slice.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected slice but got %s", slice.Kind())
	}

	// Index is the next part
	idxStr := s.parts[s.index]
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return nil, fmt.Errorf("invalid index '%s'", idxStr)
	}

	// Grow slice if needed
	for slice.Len() <= idx {
		elem := reflect.New(slice.Type().Elem()).Elem()
		slice = reflect.Append(slice, elem)
	}

	// Set back if s.current was addressable
	if s.current.CanSet() {
		s.current.Set(slice)
	}

	// Move to the indexed element
	s.current = deref(slice.Index(idx))

	s.prefix += ".0"
	s.index++

	if s.index == len(s.parts) {
		val := reflect.ValueOf(s.value)
		if !val.Type().AssignableTo(s.current.Type()) {
			return nil, fmt.Errorf("cannot assign %T to %s", s.value, s.current.Type())
		}
		s.current.Set(val)
		return nil, nil
	}

	// Now check schema at this path
	log.Printf("prefix in slice = %s", s.prefix)
	sch, ok := s.schema[s.prefix]
	if !ok {
		return nil, fmt.Errorf("schema missing for '%s'", s.prefix)
	}

	switch sch.Kind {
	case reflect.Struct:
		return handleStruct, nil
	case reflect.Map:
		log.Printf("calling map with prefix %s", s.prefix)
		return handleMap, nil
	default:
		return nil, fmt.Errorf("unsupported slice element kind: %v", sch.Kind)
	}
}

func handleMap(s *configState) (configStateFunc, error) {
	mapKeyStr := s.parts[s.index]
	log.Printf("mapKey = %s", mapKeyStr)

	m := s.current
	m = deref(m)

	if m.Kind() != reflect.Map {
		return nil, fmt.Errorf("current value is not a map at prefix %s", s.prefix)
	}

	mapKey := reflect.ValueOf(mapKeyStr)
	val := m.MapIndex(mapKey)
	log.Printf("val = %v", val)

	if !val.IsValid() {
		// Create a new zero value for the map element
		val = reflect.New(m.Type().Elem()).Elem()
		m.SetMapIndex(mapKey, val)
	}

	s.index++
	s.prefix += ".<key>"

	// Final path part: assign directly into map
	if s.index == len(s.parts) {
		newVal := reflect.ValueOf(s.value)
		if !newVal.Type().AssignableTo(val.Type()) {
			return nil, fmt.Errorf("cannot assign %T to %s", s.value, val.Type())
		}

		m.SetMapIndex(mapKey, newVal)
		return nil, nil
	}

	// Not at end yet: transition into the correct handler
	val = deref(val)
	s.current = val

	sch, ok := s.schema[s.prefix]
	if !ok {
		return nil, fmt.Errorf("schema missing for '%s'", s.prefix)
	}

	switch sch.Kind {
	case reflect.Struct:
		log.Printf("called struct with prefix %s", s.prefix)
		return handleStruct, nil
	case reflect.Slice:
		log.Printf("called slice with prefix %s", s.prefix)
		return handleSlice, nil
	case reflect.Map:
		log.Printf("called map with prefix %s", s.prefix)
		return handleMap, nil
	default:
		return nil, fmt.Errorf("unsupported map value kind: %v", sch.Kind)
	}
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
