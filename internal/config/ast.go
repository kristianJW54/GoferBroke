package config

import (
	"fmt"
	"strconv"
)

type Node any

type RootConfig struct {
	Nodes []Node
}

type KeyValueNode struct {
	Key   string
	Value Node
}

type StringNode struct {
	Value string
}

type DigitNode struct {
	Value int
}

type BoolNode struct {
	Value bool
}

type ListNode struct {
	Items []Node
}

type ObjectNode struct {
	Children []*KeyValueNode
}

type ASTPathValue struct {
	Path  string
	Value any
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

func StreamAST(root *RootConfig) ([]ASTPathValue, error) {

	out := make([]ASTPathValue, 0, 8) // Initialise with some capacity to avoid re-allocating for small configs
	err := walkAst(root, "", &out)
	return out, err
}

func walkAst(n Node, prefix string, out *[]ASTPathValue) error {

	if n == nil {
		return fmt.Errorf("root config tree can not be nil")
	}

	switch node := n.(type) {

	case *RootConfig:
		for _, child := range node.Nodes {

			if err := walkAst(child, prefix, out); err != nil {
				return err
			}
		}

	case *KeyValueNode:

		newPrefix := joinPath(prefix, node.Key)

		return walkAst(node.Value, newPrefix, out)

	case *ListNode:

		for i, item := range node.Items {
			idxPrefix := joinPath(prefix, strconv.Itoa(i))
			if err := walkAst(item, idxPrefix, out); err != nil {
				return err
			}
		}

	case *ObjectNode:

		for _, child := range node.Children {
			if err := walkAst(child, prefix, out); err != nil {
				return err
			}
		}

	case *StringNode:
		*out = append(*out, ASTPathValue{prefix, node.Value})

	case *DigitNode:
		*out = append(*out, ASTPathValue{prefix, node.Value})

	case *BoolNode:
		*out = append(*out, ASTPathValue{prefix, node.Value})

	default:
		return fmt.Errorf("unknown node type: %T", node)

	}

	return nil

}
