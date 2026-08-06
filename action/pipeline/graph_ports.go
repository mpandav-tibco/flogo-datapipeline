package pipeline

// PortSchema describes a stage type's input/output port names for graph
// authoring — the ENGINE's ground truth for what Studio's canvas should draw
// as wireable ports on a node of this kind/type. Mirrors the file format's own
// GraphNodeDef.Ports (see definition.go), which already lets a NODE declare
// its ports; this is the per-TYPE default a Studio catalog can consult before
// a node has been customized.
//
// Dynamic is true when the port names depend on that node's OWN config rather
// than being fixed by type (e.g. router's one-port-per-route, broadcast's
// one-port-per-sink) — a static catalog cannot enumerate them; Studio must
// derive the actual names from the node's config at authoring time.
type PortSchema struct {
	In      []string `json:"in,omitempty"`
	Out     []string `json:"out,omitempty"`
	Dynamic bool     `json:"dynamic,omitempty"`
}

// fixedTransformPorts overrides the 1-in/1-out transform default for stage
// types whose port arity is fixed by TYPE, not by that node's own config.
var fixedTransformPorts = map[string]PortSchema{
	// join accepts a graph-wired "right" input (set "right":"graph" in its
	// config instead of an inline source) since a node implementing
	// graphexec.MultiInputOp is present with a "right"-port inbound edge — see
	// transform/join's LoadRight/CloseRight/ApplyLeft. A join with NO
	// "right"-port edge (inline "right" config instead) runs on the ordinary
	// single-input path unaffected.
	"join": {In: []string{"left", "right"}, Out: []string{"out"}},
	// validate is the one transform that implements graphexec.Splitter: rows
	// failing a rule route to "reject" instead of aborting the batch.
	"validate": {In: []string{"in"}, Out: []string{"pass", "reject"}},
}

// dynamicTransformPorts lists transform types whose port names depend on that
// node's own config — a human-readable note on what to derive them from.
var dynamicTransformPorts = map[string]string{
	"router":    `one output port per routes[].name, plus "default"`,
	"split":     `one output port per routes[].name, plus "default"`, // alias of router
	"broadcast": `one output port per sinks[].name, plus the main "out"`,
}

// PortsFor returns the port schema for a stage of the given kind ("source",
// "transform", "sink") and type. Source/sink port shape never varies by type
// (always one output "rows" / one input "in" respectively); transform ports
// default to 1-in/1-out unless overridden by fixedTransformPorts or flagged
// Dynamic per dynamicTransformPorts.
func PortsFor(kind, typ string) PortSchema {
	switch kind {
	case "source":
		return PortSchema{Out: []string{"rows"}}
	case "sink":
		return PortSchema{In: []string{"in"}}
	case "transform":
		if p, ok := fixedTransformPorts[typ]; ok {
			return p
		}
		if _, ok := dynamicTransformPorts[typ]; ok {
			return PortSchema{In: []string{"in"}, Dynamic: true}
		}
		return PortSchema{In: []string{"in"}, Out: []string{"out"}}
	default:
		return PortSchema{}
	}
}

// DynamicPortNote returns the human-readable derivation note for a dynamic
// transform type (empty string if the type is not dynamic).
func DynamicPortNote(typ string) string {
	return dynamicTransformPorts[typ]
}
