package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kristianJW54/GoferBroke/internal/Errors"
	"github.com/kristianJW54/GoferBroke/internal/Network"
	cfg "github.com/kristianJW54/GoferBroke/internal/config"
	"math"
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
	DEFAULT_GOSSIP_ROUND_TIMEOUT      = uint16(1000)
	DEFAULT_FAILURE_PROBE             = uint8(1)
	DEFAULT_FAILURE_TIMEOUT           = uint16(300)
	DEFAULT_FAULTY_FLAG               = uint16(10000)
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

type ClusterNetworkType uint8

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
	GossipRoundTimeout             uint16 // ms
	MaxSequenceIDPool              uint32
	ClusterNetworkType             ClusterNetworkType
	// TODO Think if we need a URL map that users can specify for their own endpoints
	EndpointsURLMap map[string]string

	NodeMTLSRequired   bool `default:"false"`
	ClientMTLSRequired bool `default:"false"`

	// Failure
	FailureKNodesToProbe uint8
	FailureProbeTimeout  uint16 // ms

	//Background tasks
	NodeFaultyAfter uint16 // ms
}

func InitDefaultClusterConfig() *GbClusterConfig {

	ep := make(map[string]string)

	ep["test"] = "hello"

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
			GossipRoundTimeout:             DEFAULT_GOSSIP_ROUND_TIMEOUT,
			MaxSequenceIDPool:              DEFAULT_SEQUENCE_ID_POOL,
			ClusterNetworkType:             C_UNDEFINED,
			EndpointsURLMap:                ep,
			NodeMTLSRequired:               false,
			ClientMTLSRequired:             false,
			FailureKNodesToProbe:           DEFAULT_FAILURE_PROBE,
			FailureProbeTimeout:            DEFAULT_FAILURE_TIMEOUT,
			NodeFaultyAfter:                DEFAULT_FAULTY_FLAG,
		},
	}

}

//=============================================================

type NodeNetworkType uint8

const (
	UNDEFINED NodeNetworkType = iota
	PRIVATE
	PUBLIC
	LOCAL
)

type GbNodeConfig struct {
	Name        string
	Host        string
	Port        string
	NetworkType NodeNetworkType
	IsSeed      bool
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

	// Logging
	DefaultLoggerEnabled bool
	LogOutput            string
	LogToBuffer          bool
	LogBufferSize        int
	LogChannelSize       int

	// TLS
	CACertFilePath string
	CertFilePath   string
	KeyFilePath    string

	//Startup
	DisableStartupMessage bool

	// Need logging config also
}

func InitDefaultNodeConfig() *GbNodeConfig {
	return &GbNodeConfig{
		Name:        "node",
		Host:        "",
		Port:        "",
		NetworkType: UNDEFINED,
		IsSeed:      false,
		ClientPort:  "",
		Internal: &InternalOptions{
			IsTestMode:                            false,
			DisableInitialiseSelf:                 false,
			DisableGossip:                         false,
			GoRoutineTracking:                     true,
			DebugMode:                             false,
			DisableUpdateServerTimeStampOnStartup: false,
			DisableInternalGossipSystemUpdate:     false,

			DefaultLoggerEnabled: true,
			LogOutput:            "stdout",
			LogToBuffer:          true,
			LogBufferSize:        100,
			LogChannelSize:       200,

			CACertFilePath: "",
			CertFilePath:   "",
			KeyFilePath:    "",

			DisableStartupMessage: false,
		},
	}
}

//=====================================================================
// Config Checksum
//=====================================================================

func configChecksum(cfg any) (string, error) {

	data, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum), nil

}

//=====================================================================
// Config Schema
//=====================================================================

// The reason why we map these functions up-front is that reflect is quite expensive and to keep calling it at
// Runtime will slow us down - therefore we do it once at compile time and store the functions
// With this we (mostly) avoid reflect

type ConfigSchema struct {
	Path      string
	Type      reflect.Type
	Kind      reflect.Kind
	isIndexed bool
	isSlice   bool
	isMap     bool
	isPtr     bool
	elemType  *ConfigSchema
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

func BuildConfigSchema(cfg any) map[string]*ConfigSchema {

	val := reflect.ValueOf(cfg)
	val = deref(val) // handles interface{} and pointer

	out := make(map[string]*ConfigSchema)
	buildSchemaRecursive(val, "", out)
	return out

}

func buildSchemaRecursive(v reflect.Value, parent string, out map[string]*ConfigSchema) {

	if !v.IsValid() {
		return
	}

	v = deref(v)

	if v.Kind() != reflect.Struct {
		return
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

			// Register the container node (e.g., "Cluster")
			out[path] = &ConfigSchema{
				Path:      path,
				Type:      fv.Type(),
				Kind:      fv.Kind(),
				isIndexed: isIndexedPath(path),
				isPtr:     fieldValue.Kind() == reflect.Ptr,
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

				case reflect.Slice:
					indexPath := mapPath + ".0"
					out[indexPath] = &ConfigSchema{
						Path:      indexPath,
						Type:      valType.Elem(),        // Type of the slice element
						Kind:      valType.Elem().Kind(), // Kind of the slice element
						isIndexed: true,
						isSlice:   true,
					}
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
				}

			} else {
				out[path] = &ConfigSchema{
					Path:      path,
					Type:      field.Type,
					Kind:      kind,
					isIndexed: isIndexed,
				}

			}

		}

	}

}

// TODO From here onwards - need proper GBError handling and returning

//=====================================================================
// Config Type Coercion
//=====================================================================

func coerceValue(input any, targetType reflect.Type) (reflect.Value, error) {
	inVal := reflect.ValueOf(input)

	// Direct assignable
	if inVal.Type().AssignableTo(targetType) {
		return inVal, nil
	}

	// Handle numeric coercion
	switch targetType.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		asInt, ok := input.(int)
		if !ok {
			return reflect.Value{}, fmt.Errorf("cannot convert %T to %s", input, targetType)
		}
		if asInt < 0 {
			return reflect.Value{}, fmt.Errorf("negative value cannot be converted to %s", targetType)
		}
		return reflect.ValueOf(uint64(asInt)).Convert(targetType), nil

	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		asInt, ok := input.(int)
		if !ok {
			return reflect.Value{}, fmt.Errorf("cannot convert %T to %s", input, targetType)
		}
		return reflect.ValueOf(asInt).Convert(targetType), nil

	case reflect.Float32, reflect.Float64:
		asFloat, ok := input.(float64)
		if !ok {
			asInt, ok := input.(int)
			if !ok {
				return reflect.Value{}, fmt.Errorf("cannot convert %T to float", input)
			}
			asFloat = float64(asInt)
		}
		return reflect.ValueOf(asFloat).Convert(targetType), nil
	}

	// Add enum fallback here if needed

	return reflect.Value{}, fmt.Errorf("cannot coerce %T to %s", input, targetType)
}

//=====================================================================
// Config Deref For Reflection
//=====================================================================

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

//=====================================================================
// Config Setting and Getting
//=====================================================================

type accessMode uint8

const (
	modeSet accessMode = iota
	modeGet
)

type configState struct {
	current     reflect.Value
	prefix      string
	index       int
	parts       []string
	schema      map[string]*ConfigSchema
	value       any
	mode        accessMode
	result      any
	parentIsMap bool
	mapRef      reflect.Value
	mapKey      string
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
		mode:    modeSet,
		result:  nil,
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

func GetByPath(sch map[string]*ConfigSchema, config any, path string) (any, *ConfigSchema, error) {
	state := &configState{
		schema:  sch,
		parts:   strings.Split(path, "."),
		current: deref(reflect.ValueOf(config)),
		index:   0,
		prefix:  "", // will be built up
		mode:    modeGet,
	}

	handler := handleStruct
	var err error
	for handler != nil {
		handler, err = handler(state)
		if err != nil {
			return nil, nil, err
		}
	}

	return state.result, state.schema[state.prefix], nil
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
		if s.mode == modeSet {
			if !val.Type().AssignableTo(s.current.Type()) {
				switch s.current.Interface().(type) {
				case ClusterNetworkType:
					if _, ok := val.Interface().(string); !ok {
						return nil, fmt.Errorf("cluster network type must be a string")
					}
					parsed, err := ParseClusterNetworkTypeFromString(val.String())
					if err != nil {
						return nil, err
					}
					val = reflect.ValueOf(parsed)
					s.current.Set(val)
					return nil, nil
				case NodeNetworkType:
					if _, ok := val.Interface().(string); !ok {
						return nil, fmt.Errorf("node network type must be a string")
					}
					parsed, err := ParseNodeNetworkTypeFromString(val.String())
					if err != nil {
						return nil, err
					}
					val = reflect.ValueOf(parsed)
					s.current.Set(val)

					return nil, nil

				}

				// Fall back to generic coercion
				coerced, err := coerceValue(s.value, s.current.Type())
				if err != nil {
					return nil, fmt.Errorf("invalid type for '%v': %v", val.Type(), err)
				}
				s.current.Set(coerced)
				return nil, nil
			}
			s.current.Set(val)
		} else if s.mode == modeGet {
			if !s.current.IsValid() {
				return nil, fmt.Errorf("value is not valid at path %s", s.prefix)
			}
			s.result = s.current.Interface()
		}
		return nil, nil
	}

	switch sch.Kind {
	case reflect.Struct:
		return handleStruct, nil
	case reflect.Slice:
		return handleSlice, nil
	case reflect.Map:
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
	} else if s.parentIsMap && s.mapRef.IsValid() {
		s.mapRef.SetMapIndex(reflect.ValueOf(s.mapKey), slice)
	}

	// Move to the indexed element
	s.current = slice.Index(idx)
	s.current = deref(s.current)

	s.prefix += ".0"
	s.index++

	if s.index == len(s.parts) {
		val := reflect.ValueOf(s.value)
		if s.mode == modeSet {
			if !val.Type().AssignableTo(s.current.Type()) {
				return nil, fmt.Errorf("cannot assign %T to %s", s.value, s.current.Type())
			}
			s.current.Set(val)
		} else if s.mode == modeGet {
			if !s.current.IsValid() {
				return nil, fmt.Errorf("value is not valid at path %s", s.prefix)
			}
			s.result = s.current.Interface()
		}

		return nil, nil
	}

	// Now check schema at this path
	sch, ok := s.schema[s.prefix]
	if !ok {
		return nil, fmt.Errorf("schema missing for '%s'", s.prefix)
	}

	switch sch.Kind {
	case reflect.Struct:
		return handleStruct, nil
	case reflect.Map:
		return handleMap, nil
	default:
		return nil, fmt.Errorf("unsupported slice element kind: %v", sch.Kind)
	}
}

func handleMap(s *configState) (configStateFunc, error) {
	mapKeyStr := s.parts[s.index]

	m := deref(s.current)

	if m.IsNil() {
		if s.current.CanSet() {
			newMap := reflect.MakeMap(m.Type())
			s.current.Set(newMap)
			m = newMap
		} else {
			return nil, fmt.Errorf("cannot set map at prefix %s", s.prefix)
		}
	}

	if m.Kind() != reflect.Map {
		return nil, fmt.Errorf("current value is not a map at prefix %s", s.prefix)
	}

	mapKey := reflect.ValueOf(mapKeyStr)
	val := m.MapIndex(mapKey)

	s.index++
	s.prefix += ".<key>"

	if !val.IsValid() {
		nextPart := ""
		if s.index < len(s.parts) {
			nextPart = s.parts[s.index]
		}

		elemType := m.Type().Elem()

		// If we're about to index into a slice (e.g. "0"), initialize as slice
		if _, err := strconv.Atoi(nextPart); err == nil && elemType.Kind() == reflect.Slice {
			val = reflect.MakeSlice(elemType, 0, 1)
			m.SetMapIndex(mapKey, val)

			val = m.MapIndex(mapKey)
		} else {
			// Default to struct/map/etc
			val = reflect.New(elemType).Elem()
			m.SetMapIndex(mapKey, val)
		}
	}

	// Final path part: assign directly into map
	if s.index == len(s.parts) {
		newVal := reflect.ValueOf(s.value)

		if s.mode == modeSet {
			// use Elem type of the map to validate type, not val.Type() (which could be invalid)
			if !newVal.Type().AssignableTo(m.Type().Elem()) {
				fmt.Printf("[SET ERROR] path=%s cannot assign value of type %s to map with elem type %s\n",
					s.prefix, newVal.Type(), m.Type().Elem())
				return nil, fmt.Errorf("cannot assign %T to %s", s.value, m.Type().Elem())
			}

			m.SetMapIndex(mapKey, newVal)
			return nil, nil

		} else if s.mode == modeGet {
			if !val.IsValid() {
				return nil, fmt.Errorf("value for key %v does not exist", mapKey)
			}
			s.result = val.Interface()
			return nil, nil
		}
	}

	// If the value doesn't exist, create a zero struct (for traversal)
	if !val.IsValid() {
		val = reflect.New(m.Type().Elem()).Elem()
		m.SetMapIndex(mapKey, val)
	}

	val = deref(val)
	s.current = val

	sch, ok := s.schema[s.prefix]
	if !ok {
		return nil, fmt.Errorf("schema missing for '%s'", s.prefix)
	}

	switch sch.Kind {
	case reflect.Struct:
		return handleStruct, nil
	case reflect.Slice:
		s.parentIsMap = true
		s.mapRef = m
		s.mapKey = mapKey.String()
		return handleSlice, nil
	case reflect.Map:
		return handleMap, nil
	default:
		return nil, fmt.Errorf("unsupported map value kind: %v", sch.Kind)
	}
}

func SetConfigValue(schema map[string]*ConfigSchema, cfg any, path string, value any) error {

	err := SetByPath(schema, cfg, path, value)
	if err != nil {
		return err
	}

	return nil

}

//=====================================================================
// Config Build
//=====================================================================

func BuildConfigFromFile(filePath string, config any) (map[string]*ConfigSchema, error) {

	// Parse the config from file into an ast tree
	root, err := cfg.ParseConfigFromFile(filePath)
	if err != nil {
		return nil, err
	}

	// Build schema from the config
	schema := BuildConfigSchema(config)
	//for _, s := range schema {
	//	log.Printf("schema path = %s", s.Path)
	//}

	// Use the ast tree to populate a list of path values to populate the config with
	pathValues, err := cfg.StreamAST(root)
	if err != nil {
		return nil, err
	}

	// Use the path values along with the schema to build the config
	for _, av := range pathValues {

		if err := SetConfigValue(schema, config, av.Path, av.Value); err != nil {
			return nil, err
		}

	}

	return schema, nil

}

func BuildConfigFromString(data string, config any) (map[string]*ConfigSchema, error) {

	// Parse the config from file into an ast tree
	root, err := cfg.ParseConfig(data)
	if err != nil {
		return nil, err
	}

	// Build schema from the config
	schema := BuildConfigSchema(config)
	//for _, s := range schema {
	//	log.Printf("schema path = %s", s.Path)
	//}

	// Use the ast tree to populate a list of path values to populate the config with
	pathValues, err := cfg.StreamAST(root)
	if err != nil {
		return nil, err
	}

	// Use the path values along with the schema to build the config
	for _, av := range pathValues {

		if err := SetConfigValue(schema, config, av.Path, av.Value); err != nil {
			return nil, err
		}

	}

	return schema, nil

}

//=====================================================================
// Config Generate Delta
//=====================================================================

func CollectPaths(v reflect.Value, prefix string, out *[]string) {
	v = deref(v)

	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" { // unexported
				continue
			}
			CollectPaths(v.Field(i), joinPath(prefix, field.Name), out)
		}

	case reflect.Ptr:
		if !v.IsNil() {
			CollectPaths(v.Elem(), prefix, out)
		}

	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			idxPath := fmt.Sprintf("%s.%d", prefix, i)
			CollectPaths(v.Index(i), idxPath, out)
		}

	case reflect.Map:
		for _, key := range v.MapKeys() {
			keyStr := fmt.Sprintf("%v", key.Interface())
			mapPath := fmt.Sprintf("%s.%s", prefix, keyStr)
			CollectPaths(v.MapIndex(key), mapPath, out)
		}

	default:
		*out = append(*out, prefix)
	}
}

func convertReflectTypeToDeltaType(t reflect.Kind) (byte, error) {

	switch t {
	case reflect.Bool:
		return D_BOOL_TYPE, nil
	case reflect.String:
		return D_STRING_TYPE, nil
	case reflect.Int:
		return D_INT_TYPE, nil
	case reflect.Int8:
		return D_INT8_TYPE, nil
	case reflect.Int16:
		return D_INT16_TYPE, nil
	case reflect.Int32:
		return D_INT32_TYPE, nil
	case reflect.Int64:
		return D_INT64_TYPE, nil
	case reflect.Uint8:
		return D_UINT8_TYPE, nil
	case reflect.Uint16:
		return D_UINT16_TYPE, nil
	case reflect.Uint32:
		return D_UINT32_TYPE, nil
	case reflect.Uint64:
		return D_UINT64_TYPE, nil
	case reflect.Float64:
		return D_FLOAT64_TYPE, nil
	default:
		return D_BYTE_TYPE, fmt.Errorf("unsupported type: %v", t)
	}
}

func encodeToBytes(value any) ([]byte, error) {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return nil, errors.New("cannot encode nil value")
	}
	return encodeReflectValue(v)
}

func encodeReflectValue(v reflect.Value) ([]byte, error) {
	v = deref(v)

	switch v.Kind() {
	case reflect.String:
		return []byte(v.String()), nil

	case reflect.Bool:
		if v.Bool() {
			return []byte{1}, nil
		}
		return []byte{0}, nil

	case reflect.Int8:
		return []byte{byte(v.Int())}, nil

	case reflect.Uint8:
		return []byte{byte(v.Uint())}, nil

	case reflect.Int16:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(v.Int()))
		return buf, nil

	case reflect.Uint16:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(v.Uint()))
		return buf, nil

	case reflect.Int32:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v.Int()))
		return buf, nil

	case reflect.Uint32:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v.Uint()))
		return buf, nil

	case reflect.Int64, reflect.Int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v.Int()))
		return buf, nil

	case reflect.Uint64, reflect.Uint:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, v.Uint())
		return buf, nil

	case reflect.Float32:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, math.Float32bits(float32(v.Float())))
		return buf, nil

	case reflect.Float64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(v.Float()))
		return buf, nil

	default:
		return nil, fmt.Errorf("unsupported kind: %s", v.Kind())
	}
}

func decodeDeltaValue(d *Delta) (any, error) {
	typ, ok := deltaTypeToReflect[d.ValueType]
	if !ok {
		return nil, Errors.ChainGBErrorf(Errors.DecodeDeltaErr, nil, "unknown delta type - %d", d.ValueType)
	}
	return decodeFromBytes(d.Value, typ)
}

func decodeFromBytes(data []byte, typ reflect.Type) (any, error) {
	switch typ.Kind() {
	case reflect.String:
		return string(data), nil

	case reflect.Bool:
		if len(data) != 1 {
			return nil, Errors.ChainGBErrorf(Errors.DecodeDeltaErr, nil, "invalid bool encoding - got %d, expected 1", len(data))
		}
		return data[0] == 1, nil

	case reflect.Int8:
		return int8(data[0]), nil
	case reflect.Uint8:
		return uint8(data[0]), nil

	case reflect.Int16:
		return int16(binary.BigEndian.Uint16(data)), nil
	case reflect.Uint16:
		return binary.BigEndian.Uint16(data), nil

	case reflect.Int32:
		return int32(binary.BigEndian.Uint32(data)), nil
	case reflect.Uint32:
		return binary.BigEndian.Uint32(data), nil

	case reflect.Int64, reflect.Int:
		return int64(binary.BigEndian.Uint64(data)), nil
	case reflect.Uint64, reflect.Uint:
		return binary.BigEndian.Uint64(data), nil

	case reflect.Float32:
		return float32(math.Float32frombits(binary.BigEndian.Uint32(data))), nil
	case reflect.Float64:
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil

	default:
		return nil, Errors.ChainGBErrorf(Errors.DecodeDeltaErr, nil, "unknown delta kind - %s", typ.Kind())
	}
}

func GenerateConfigDeltas(schema map[string]*ConfigSchema, cfg *GbClusterConfig, part *Participant) error {

	var paths []string
	CollectPaths(reflect.ValueOf(cfg), "", &paths)

	for _, path := range paths {

		val, sch, err := GetByPath(schema, cfg, path)
		if err != nil {
			return err
		}

		result, err := encodeToBytes(val)
		if err != nil {
			return err
		}

		vType, err := convertReflectTypeToDeltaType(sch.Kind)
		if err != nil {
			// We return a byte value here still so may want to check it before committing to the error
			return err
		}

		delta := &Delta{
			KeyGroup:  CONFIG_DKG,
			Key:       path,
			Version:   0, // We initialise zero here because if we must reconcile state we won't seem to have newer versions than in flight config state from others
			ValueType: vType,
			Value:     result,
		}

		part.keyValues[MakeDeltaKey(delta.KeyGroup, delta.Key)] = delta

	}

	return nil
}

//=====================================================================
// Config Internal API
//=====================================================================

func (cfg *GbClusterConfig) getNodeSelection() uint8 {

	if cfg.Cluster.NodeSelectionPerGossipRound == 0 {
		return 1
	}

	return cfg.Cluster.NodeSelectionPerGossipRound
}

//=====================================================================

func ParseNodeNetworkTypeFromString(s string) (NodeNetworkType, error) {

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

func ParseNodeNetworkType(nn NodeNetworkType) (string, error) {

	switch nn {
	case UNDEFINED:
		return "UNDEFINED", nil
	case PRIVATE:
		return "PRIVATE", nil
	case PUBLIC:
		return "PUBLIC", nil
	case LOCAL:
		return "LOCAL", nil
	default:
		return "", fmt.Errorf("invalid node network type: %d", nn)
	}

}

func ParseClusterNetworkTypeFromString(s string) (ClusterNetworkType, error) {

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

func ParseClusterNetworkType(cn ClusterNetworkType) (string, error) {

	switch cn {
	case C_UNDEFINED:
		return "UNDEFINED", nil
	case C_PRIVATE:
		return "PRIVATE", nil
	case C_PUBLIC:
		return "PUBLIC", nil
	case C_DYNAMIC:
		return "DYNAMIC", nil
	case C_LOCAL:
		return "LOCAL", nil
	default:
		return "", fmt.Errorf("invalid cluster network type: %d", cn)
	}

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
