package harness

import (
	"fmt"
	"sort"
)

// CheckLinearizability verifies whether the recorded history of operations is linearizable
// against a sequential Key-Value store model using the Wing & Gong algorithm.
func CheckLinearizability(ops []ClientOp) (bool, string) {
	// In distributed systems testing (e.g. Porcupine / Jepsen):
	// A client operation that timed out or lost connection is indeterminate.
	// If a subsequent Get observed the value written by an unacknowledged Put,
	// that Put actually committed and took effect in the linearization order.
	readValues := make(map[string]map[string]bool)
	for _, op := range ops {
		if op.Type == "Get" && op.Success && op.Value != "" {
			if readValues[op.Key] == nil {
				readValues[op.Key] = make(map[string]bool)
			}
			readValues[op.Key][op.Value] = true
		}
	}

	validOps := make([]ClientOp, 0, len(ops))
	for _, op := range ops {
		if op.Success {
			validOps = append(validOps, op)
		} else if op.Type == "Put" && readValues[op.Key] != nil && readValues[op.Key][op.Value] {
			// This unacknowledged Put actually took effect
			op.Success = true
			validOps = append(validOps, op)
		}
	}

	// Partition history by key (operations on distinct keys are independent)
	byKey := make(map[string][]ClientOp)
	for _, op := range validOps {
		byKey[op.Key] = append(byKey[op.Key], op)
	}

	for key, keyOps := range byKey {
		ok, errDesc := checkSingleKey(keyOps)
		if !ok {
			return false, fmt.Sprintf("Linearizability violation on key '%s': %s", key, errDesc)
		}
	}

	return true, ""
}

type kvState struct {
	hasVal bool
	val    string
}

func checkSingleKey(ops []ClientOp) (bool, string) {
	if len(ops) <= 1 {
		return true, ""
	}

	// Sort operations primarily by start time
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].Start.Before(ops[j].Start)
	})

	n := len(ops)
	used := make([]bool, n)
	initialState := kvState{hasVal: false, val: ""}

	if dfsLinearize(ops, used, 0, initialState) {
		return true, ""
	}

	trace := ""
	for i, op := range ops {
		trace += fmt.Sprintf("\n [%d] id=%d type=%s val=%q start=%s end=%s",
			i, op.ID, op.Type, op.Value, op.Start.Format("15:04:05.000"), op.End.Format("15:04:05.000"))
	}

	return false, fmt.Sprintf("no sequential history matches real-time intervals for %d operations:%s", n, trace)
}

func dfsLinearize(ops []ClientOp, used []bool, matchedCount int, state kvState) bool {
	if matchedCount == len(ops) {
		return true
	}

	// Find the earliest End time among remaining unused operations.
	// Any candidate next operation MUST have Start time <= earliestEnd.
	// Otherwise, the operation that ended earliest would be violated in precedence.
	var earliestEndIdx = -1
	for i := 0; i < len(ops); i++ {
		if !used[i] {
			if earliestEndIdx == -1 || ops[i].End.Before(ops[earliestEndIdx].End) {
				earliestEndIdx = i
			}
		}
	}

	earliestEnd := ops[earliestEndIdx].End

	// Try all unused operations that started before or at earliestEnd
	for i := 0; i < len(ops); i++ {
		if used[i] {
			continue
		}
		if ops[i].Start.After(earliestEnd) {
			// Real-time precedence: cannot linearize ops[i] after earliestEnd without violating precedence
			continue
		}

		// Check if op matches current model state
		op := ops[i]
		nextState, ok := applyModel(state, op)
		if !ok {
			continue
		}

		used[i] = true
		if dfsLinearize(ops, used, matchedCount+1, nextState) {
			return true
		}
		used[i] = false
	}

	return false
}

func applyModel(st kvState, op ClientOp) (kvState, bool) {
	switch op.Type {
	case "Put":
		return kvState{hasVal: true, val: op.Value}, true
	case "Delete":
		return kvState{hasVal: false, val: ""}, true
	case "Get":
		if !st.hasVal {
			// Key not present: valid if returned empty
			if op.Value == "" {
				return st, true
			}
			return st, false
		}
		// Key present: must match returned value
		if st.val == op.Value {
			return st, true
		}
		return st, false
	default:
		return st, false
	}
}
