package api

import "encoding/json"

// countRequestNodes walks a DSL v2 tree (opaque JSON) and counts nodes
// where `type == "request"`. Anything not understood is ignored — we
// treat the tree as data, not a contract we enforce at the API layer.
// Manteion's job is to store and serve definitions; zeus parses them.
//
// The workflows list page renders cards from the list-item DTO, and
// "N request nodes" is a useful subtitle that costs less than shipping
// the full DSL tree per row.
func countRequestNodes(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var node map[string]json.RawMessage
	if err := json.Unmarshal(raw, &node); err != nil {
		return 0
	}
	var typ string
	if t, ok := node["type"]; ok {
		_ = json.Unmarshal(t, &typ)
	}
	switch typ {
	case "request":
		return 1
	case "sequence", "parallel":
		var kids []json.RawMessage
		if c, ok := node["children"]; ok {
			_ = json.Unmarshal(c, &kids)
		}
		n := 0
		for _, k := range kids {
			n += countRequestNodes(k)
		}
		return n
	case "optional", "delay":
		// optional/delay nodes wrap a single child via `child`.
		if c, ok := node["child"]; ok {
			return countRequestNodes(c)
		}
		return 0
	default:
		// Unknown shape — descend into children if present, else zero.
		if c, ok := node["children"]; ok {
			var kids []json.RawMessage
			_ = json.Unmarshal(c, &kids)
			n := 0
			for _, k := range kids {
				n += countRequestNodes(k)
			}
			return n
		}
		return 0
	}
}
