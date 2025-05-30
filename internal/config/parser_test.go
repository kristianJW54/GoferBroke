package config

import (
	"log"
	"testing"
)

func TestParserToken(t *testing.T) {

	d := `key1: [{mapKey: "value2", mapKey2: "value3"}]
key2: 1`

	token, err := parseConfig(d)
	if err != nil {
		t.Error(err)
	}

	//log.Printf("%T - %s", token.Nodes[0], token.Nodes[0])

	log.Printf("%v number of nodes in tree", len(token.Nodes))

	for _, node := range token.Nodes {
		walkNode(node, "")
	}

}

func walkNode(n Node, indent string) {
	switch node := n.(type) {
	case *KeyValueNode:
		log.Printf("%sKeyValueNode → %s", indent, node.Key)
		walkNode(node.Value, indent+"  ")

	case *StringNode:
		log.Printf("%sString → %s", indent, node.Value)

	case *DigitNode:
		log.Printf("%sDigit → %d", indent, node.Value)

	case *ListNode:
		log.Printf("%sList → %d item(s)", indent, len(node.Items))
		for i, item := range node.Items {
			log.Printf("%s  [Item %d]", indent, i)
			walkNode(item, indent+"    ")
		}

	case *ObjectNode:
		log.Printf("%sObject → %d field(s)", indent, len(node.Children))
		for _, kv := range node.Children {
			walkNode(kv, indent+"  ")
		}

	default:
		log.Printf("%sUnknown node type: %T", indent, node)
	}
}
