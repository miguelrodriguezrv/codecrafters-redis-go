package art

import (
	"bytes"
)

// Adaptive Radix Trie implementation
func NewART() *ART {
	return &ART{}
}

type ART struct {
	root *Node
}

func (t *ART) Select(key []byte) (interface{}, bool) {
	node := t.root

	for node != nil {
		keyIndex := 0
		if node.isLeaf {
			if matchesPrefix(key, node.prefix) {
				return node.value, true
			}
			return nil, false
		}

		if !matchesPrefix(key[:len(node.prefix)], node.prefix) {
			return nil, false
		}

		keyIndex += len(node.prefix)

		var nextKey byte
		if keyIndex >= len(key) {
			nextKey = byte(0)
		} else {
			nextKey = key[keyIndex]
		}
		nextNode := findNextNode(node, nextKey)

		if nextNode == nil {
			return nil, false
		}

		node = nextNode
		keyIndex++
	}
	return nil, false
}
func (t *ART) Insert(key []byte, value interface{}) {
	if t.root == nil {
		t.root = createNode(NodeLeaf, key, value)
		return
	}
	node := t.root

	for {
		// Compare prefix
		keyIdx := findMismatchIndex(key, node.prefix)
		if keyIdx != len(node.prefix) {
			t.splitNode(node, keyIdx, key, value)
			return
		}

		// Handle leaf
		if node.isLeaf {
			if bytes.Equal(key, node.prefix) {
				node.value = value
				return
			} else {
				t.splitNode(node, len(node.prefix), key, value)
				return
			}
		}

		// Find or create next child
		var nextKey byte
		if keyIdx >= len(key) {
			nextKey = byte(0)
		} else {
			nextKey = key[keyIdx]
		}
		nextNode := findNextNode(node, nextKey)
		if nextNode == nil {
			nextNode = createNode(NodeLeaf, key, value)
			node.addChild(nextKey, nextNode)
			return
		}

		// Go to the next node
		node = nextNode
	}
}

func findNextNode(node *Node, nextKey byte) *Node {
	var nextNode *Node
	switch node.nodeType {
	case Node4:
		for i, k := range node.keys {
			if k == nextKey {
				nextNode = node.children[i]
				break
			}
		}
	case Node16:
		idx := binarySearch(node.keys, nextKey)
		if idx != -1 {
			nextNode = node.children[idx]
		}
	case Node48:
		idx := node.indexMap[nextKey]
		if idx != -1 {
			nextNode = node.children[idx]
		}
	case Node256:
		nextNode = node.children[nextKey]
	}
	return nextNode
}

func (t *ART) splitNode(oldNode *Node, splitIndex int, key []byte, value interface{}) {
	newParent := createNode(Node4, oldNode.prefix[:splitIndex], nil)

	newLeaf := createNode(NodeLeaf, key, value)

	// Add oldNode to new parent
	if oldNode.isLeaf {
		if splitIndex >= len(oldNode.prefix) {
			newParent.addChild(byte(0), oldNode)
		} else {
			newParent.addChild(oldNode.prefix[splitIndex], oldNode)
		}
	} else {
		newParent.addChild(oldNode.prefix[splitIndex], oldNode)
	}
	if splitIndex >= len(key) {
		newParent.addChild(byte(0), newLeaf)
	} else {
		newParent.addChild(key[splitIndex], newLeaf)
	}

	replaceNodeInTree(t, oldNode, newParent)
}

func replaceNodeInTree(tree *ART, oldNode, newNode *Node) {
	if oldNode == tree.root {
		tree.root = newNode
		return
	}

	parent := findParent(tree.root, oldNode)
	if parent == nil {
		panic("Parent not found!")
	}

	for i, child := range parent.children {
		if child == oldNode {
			parent.children[i] = newNode
			return
		}
	}
}

func findParent(node, target *Node) *Node {
	if node == nil || node.isLeaf {
		return nil
	}

	for _, child := range node.children {
		if child == target {
			return node
		}
		if parent := findParent(child, target); parent != nil {
			return parent
		}
	}
	return nil
}
