package art

import (
	"bytes"
	"fmt"
	"strings"
)

func matchesPrefix(keyPart, prefix []byte) bool {
	if len(keyPart) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if keyPart[i] != prefix[i] {
			return false
		}
	}
	return true
}

func binarySearch(keys []byte, key byte) int {
	low, high := 0, len(keys)-1
	for low <= high {
		mid := (low + high) / 2
		if keys[mid] == key {
			return mid
		} else if keys[mid] < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return -1 // Key not found
}

func findMismatchIndex(keyPart, prefix []byte) int {
	minLength := min(len(keyPart), len(prefix))
	for i := 0; i < minLength; i++ {
		if keyPart[i] != prefix[i] {
			return i
		}
	}
	return minLength
}

// inRange checks if key is between start and end (inclusive)
func inRange(key, start, end []byte) bool {
    return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) <= 0
}

// isPrefixInRange checks if any key with this prefix could be in range
func isPrefixInRange(prefix, start, end []byte) bool {
    // If prefix is shorter than start, check if it could lead to keys >= start
    if len(prefix) < len(start) {
        return bytes.Compare(prefix, start[:len(prefix)]) >= 0
    }

    // If prefix is shorter than end, check if it could lead to keys <= end
    if len(prefix) < len(end) {
        return bytes.Compare(prefix, end[:len(prefix)]) <= 0
    }

    // If prefix is longer or equal to both bounds, check if it's in range
    return bytes.Compare(prefix, start) >= 0 && bytes.Compare(prefix, end) <= 0
}

// asciiPrint generates the ASCII representation with leaf highlighting and colors.
func asciiPrint(node *Node, depth int, prefix string) string {
	if node == nil {
		return ""
	}

	var builder strings.Builder
	indent := ""
	if depth > 0 {
		indent = strings.Repeat("  ", depth-1) + prefix
	}

	// Node information with colors and leaf highlighting
	builder.WriteString(indent)
	if depth > 0 {
		builder.WriteString(fmt.Sprintf("%s── ", prefix))
	}

	// Add colors
	nodeTypeColor := "\033[33m" // Yellow
	prefixColor := "\033[32m"   // Green
	valueColor := "\033[34m"    // Blue
	resetColor := "\033[0m"

	builder.WriteString(fmt.Sprintf("%s[Type: %d%s%s, Prefix: %s%s%s]",
		nodeTypeColor, node.nodeType, resetColor,
		resetColor, prefixColor, string(node.prefix), resetColor))

	if node.isLeaf {
		builder.WriteString(fmt.Sprintf(" (*) -> %sValue: %v%s",
			valueColor, node.value, resetColor))
	}
	builder.WriteString("\n")

	// Recursively print children
	if !node.isLeaf {
		for i, child := range node.children {
			if child != nil {
				newPrefix := "│"
				if i == len(node.children)-1 || node.children[i+1] == nil {
					newPrefix = " "
				}
				builder.WriteString(asciiPrint(child, depth+1, newPrefix))
			}
		}
	}

	return builder.String()
}

// PrintARTAscii prints the ART with leaf highlighting and colors.
func (tree *ART) PrintARTAscii() {
	fmt.Print(asciiPrint(tree.root, 0, ""))
}
