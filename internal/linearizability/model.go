// Package linearizability holds the Porcupine model of the KV API, used to
// check client histories recorded by the simulation and cluster tests.
package linearizability

import (
	"fmt"
	"maps"
	"slices"

	"github.com/anishathalye/porcupine"
)

type Input struct {
	Op    string // "get", "put" or "delete"
	Key   string
	Value string
}

type Output struct {
	Value string
	Found bool
}

// Model treats each key as an independent register, so histories are
// checked one key at a time.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(Input).Key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, k := range slices.Sorted(maps.Keys(byKey)) {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return Output{} },
	Step: func(state, input, output any) (bool, any) {
		s, in := state.(Output), input.(Input)
		switch in.Op {
		case "get":
			return output.(Output) == s, s
		case "put":
			return true, Output{Value: in.Value, Found: true}
		default:
			return true, Output{}
		}
	},
	DescribeOperation: func(input, output any) string {
		in := input.(Input)
		if in.Op == "get" {
			out := output.(Output)
			if !out.Found {
				return fmt.Sprintf("get(%s) -> missing", in.Key)
			}
			return fmt.Sprintf("get(%s) -> %q", in.Key, out.Value)
		}
		if in.Op == "put" {
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		}
		return fmt.Sprintf("delete(%s)", in.Key)
	},
}
