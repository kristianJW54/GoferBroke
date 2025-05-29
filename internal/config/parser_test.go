package config

import (
	"log"
	"testing"
)

func TestParserToken(t *testing.T) {

	d := `key1: [{"value2"]`

	token, err := parseConfig(d)
	if err != nil {
		t.Error(err)
	}

	//log.Printf("%T - %s", token.Nodes[0], token.Nodes[0])

	for _, node := range token.Nodes {
		switch n := node.(type) {
		case *KeyValueNode:
			log.Printf("KeyValueNode → %s = %v", n.Key, n.Value)
		case *ListNode:
			log.Printf("ListNode → has %d items = first item --> %s", len(n.Items), n.Items[0])
		case *ObjectNode:
			log.Printf("ObjectNode → has %d fields", len(n.Children))
		default:
			log.Printf("Unknown node: %T → %+v", node, node)
		}
	}

}
