package art

import "sort"

type NodeType int

const (
	Node4 NodeType = iota
	Node16
	Node48
	Node256
	NodeLeaf

	Node4Max   = 4
	Node16Max  = 16
	Node48Max  = 48
	Node256Max = 256
)

type Node struct {
	nodeType    NodeType
	prefix      []byte
	keys        []byte
	indexMap    [256]int8
	children    []*Node
	numChildren int
	isLeaf      bool
	value       interface{}
}

func createNode(nodeType NodeType, prefix []byte, value interface{}) *Node {
	switch nodeType {
	case NodeLeaf:
		return &Node{
			nodeType: NodeLeaf,
			prefix:   prefix,
			value:    value,
			isLeaf:   true,
		}

	case Node4:
		return &Node{
			nodeType: Node4,
			prefix:   prefix,
			keys:     make([]byte, 0, Node4Max),
			children: make([]*Node, 0, Node4Max),
		}

	case Node16:
		return &Node{
			nodeType: Node16,
			prefix:   prefix,
			keys:     make([]byte, 0, Node16Max),
			children: make([]*Node, 0, Node16Max),
		}

	case Node48:
		return &Node{
			nodeType: Node48,
			prefix:   prefix,
			indexMap: [256]int8{-1},
			children: make([]*Node, 0, 48),
		}

	case Node256:
		return &Node{
			nodeType: Node256,
			prefix:   prefix,
			children: make([]*Node, 256),
		}

	default:
		panic("Unknown node type")
	}
}

func (node *Node) addChild(key byte, child *Node) {
	switch node.nodeType {
	case Node4:
		if len(node.children) < 4 {
			node.keys = append(node.keys, key)
			node.children = append(node.children, child)
			return
		}
		node.resize(Node16)
		node.addChild(key, child)

	case Node16:
		if len(node.children) < 16 {
			idx := findInsertPosition(node.keys, key)
			node.keys = insertAt(node.keys, idx, key)
			node.children = insertAt(node.children, idx, child)
			return
		}
		node.resize(Node48)
		node.addChild(key, child)

	case Node48:
		if node.indexMap[key] == -1 {
			node.indexMap[key] = int8(len(node.children))
			node.children = append(node.children, child)
			return
		}
		node.resize(Node256)
		node.addChild(key, child)

	case Node256:
		node.children[key] = child
	default:
		panic("Invalid node type for addChild")
	}
}

func (node *Node) resize(newType NodeType) {
	switch newType {
	case Node16:
		newNode := &Node{
			nodeType: Node16,
			prefix:   node.prefix,
			keys:     make([]byte, 0, Node16Max),
			children: make([]*Node, 0, Node16Max),
		}
		for i, key := range node.keys {
			newNode.addChild(key, node.children[i])
		}
		*node = *newNode
	case Node48:
		newNode := &Node{
			nodeType: Node48,
			prefix:   node.prefix,
			indexMap: [256]int8{-1},
		}
		for i, key := range node.keys {
			newNode.addChild(key, node.children[i])
		}
		*node = *newNode
	case Node256:
		newNode := &Node{
			nodeType: Node256,
			prefix:   node.prefix,
			children: make([]*Node, 256),
		}
		for i, key := range node.keys {
			newNode.children[key] = node.children[i]
		}
		*node = *newNode
	default:
		panic("Invalid node type for resize")
	}
}

func findInsertPosition(keys []byte, newKey byte) int {
	return sort.Search(len(keys), func(i int) bool {
		return keys[i] >= newKey
	})
}

func insertAt[T any](slice []T, index int, value T) []T {
	if index < 0 || index > len(slice) {
		panic("Index out of bounds")
	}
	slice = append(slice[:index+1], slice[index:]...)
	slice[index] = value
	return slice
}
