package config

import (
	"log"
	"testing"
)

func TestParserToken(t *testing.T) {

	//	d := `key1: [{mapKey: "value2", mapKey2: "value3"}]
	//key2: 1`

	d2 := `Cluster: {
		
			Name: "hello :)",

		}`

	token, err := ParseConfig(d2)
	if err != nil {
		t.Error(err)
	}

	//log.Printf("%T - %s", token.Nodes[0], token.Nodes[0])

	log.Printf("%v number of nodes in tree", len(token.Nodes))

	for _, node := range token.Nodes {
		walkNode(node, "")
	}

}

func TestParserWithTestFile(t *testing.T) {

	filePath := "../../Configs/cluster/complex_test_config.conf"

	cfg, err := ParseConfigFromFile(filePath)
	if err != nil {
		t.Error(err)
	}

	log.Printf("%v number of nodes in tree", len(cfg.Nodes))

	for _, node := range cfg.Nodes {
		walkNode(node, "")
	}

}

func TestParserWithBasicFile(t *testing.T) {

	filePath := "../../Configs/cluster/default_cluster_config.conf"

	cfg, err := ParseConfigFromFile(filePath)
	if err != nil {
		t.Error(err)
	}

	log.Printf("%v number of nodes in tree", len(cfg.Nodes))

	for _, node := range cfg.Nodes {
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

	case *BoolNode:
		log.Printf("%sBool → %v", indent, node.Value)

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

func TestASTPathValues(t *testing.T) {

	filePath := "../../Configs/cluster/complex_test_config.conf"

	root, err := ParseConfigFromFile(filePath)
	if err != nil {
		t.Error(err)
	}

	av, err := StreamAST(root)
	if err != nil {
		t.Error(err)
	}

	for i, paths := range av {
		log.Printf("%d -> path = %s", i, paths.Path)
	}

}
