package garden

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/OrdalieTech/big-garden/internal/head"
)

// Preferred returns the rendezvous-preferred NodeID for key: the node whose
// SHA-256(u32be(len(key)) || key || u32be(len(node)) || node) is the
// lexicographically largest, ties broken by the larger NodeID. The result
// does not depend on the order of nodeIDs. It is placement advice for new or
// unowned keys, never ownership.
func Preferred(key string, nodeIDs []string) (string, error) {
	if err := head.ValidKey(key); err != nil {
		return "", invalid(err.Error())
	}
	if len(nodeIDs) == 0 {
		return "", invalid("no node IDs")
	}
	seen := make(map[string]bool, len(nodeIDs))
	var best string
	var bestSum [32]byte
	for _, n := range nodeIDs {
		if err := head.ValidNode(n); err != nil {
			return "", invalid(err.Error())
		}
		if seen[n] {
			return "", invalid("duplicate node ID")
		}
		seen[n] = true
		b := binary.BigEndian.AppendUint32(nil, uint32(len(key)))
		b = binary.BigEndian.AppendUint32(append(b, key...), uint32(len(n)))
		sum := sha256.Sum256(append(b, n...))
		if c := bytes.Compare(sum[:], bestSum[:]); best == "" || c > 0 || (c == 0 && n > best) {
			best, bestSum = n, sum
		}
	}
	return best, nil
}
