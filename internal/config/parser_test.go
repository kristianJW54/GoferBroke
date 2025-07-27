package config

import (
	"fmt"
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

	fmt.Printf("%v number of nodes in tree\n", len(token.Nodes))

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

	fmt.Printf("%v number of nodes in tree\n", len(cfg.Nodes))

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

	fmt.Printf("%v number of nodes in tree\n", len(cfg.Nodes))

	for _, node := range cfg.Nodes {
		walkNode(node, "")
	}

}

func walkNode(n Node, indent string) {
	switch node := n.(type) {
	case *KeyValueNode:
		fmt.Printf("%sKeyValueNode → %s\n", indent, node.Key)
		walkNode(node.Value, indent+"  ")

	case *StringNode:
		fmt.Printf("%sString → %s\n", indent, node.Value)

	case *BoolNode:
		fmt.Printf("%sBool → %v\n", indent, node.Value)

	case *DigitNode:
		fmt.Printf("%sDigit → %d\n", indent, node.Value)

	case *ListNode:
		fmt.Printf("%sList → %d item(s)\n", indent, len(node.Items))
		for i, item := range node.Items {
			fmt.Printf("%s  [Item %d]\n", indent, i)
			walkNode(item, indent+"    ")
		}

	case *ObjectNode:
		fmt.Printf("%sObject → %d field(s)\n", indent, len(node.Children))
		for _, kv := range node.Children {
			walkNode(kv, indent+"  ")
		}

	default:
		fmt.Printf("%sUnknown node type: %T\n", indent, node)
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
		fmt.Printf("%d -> path = %s\n", i, paths.Path)
	}

}
