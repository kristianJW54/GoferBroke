package config

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

type ListNode struct {
	Items []Node
}

type ObjectNode struct {
	Children []*KeyValueNode
}
