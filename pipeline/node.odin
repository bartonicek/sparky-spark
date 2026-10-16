package pipeline

import "../batch"
import "../types"
import "core:fmt"
import "core:slice"

// User functions should use context.temp_allocator; it is freed when the task ends.
Map_Fn    :: proc(inputs: []batch.Column) -> batch.Column
Filter_Fn :: proc(inputs: []batch.Column) -> []bool // true = keep the row
Aggregate_Fn :: proc(acc: batch.Value, inputs: []batch.Column) -> batch.Value
Unit_Fn :: proc() -> batch.Value

// For Map and Aggregate, output = "" means the same as output = inputs[0].
Source :: struct { data: batch.Batch }
Map    :: struct { fn: Map_Fn, inputs: []string, output: string }
Filter :: struct { fn: Filter_Fn, inputs: []string }
Aggregate :: struct { fn: Aggregate_Fn, unit: Unit_Fn, inputs: []string, output: string }

// Folds the rows of each distinct `key` with `fn`; the output has the columns
// `key` and `output`. Map tasks fold rows and reduce tasks fold the partial
// results, so `fn` must also work on its own output (sum, min, max: yes;
// count: no, map to 1s and sum instead).
Reduce_By_Key :: struct { fn: Aggregate_Fn, unit: Unit_Fn, key: string, inputs: []string, output: string }

Op :: union { Source, Map, Filter, Aggregate, Reduce_By_Key }

NodeOptions :: struct {
	num_partitions: int,  // 0 = inherit from the parent (narrow ops)
	cache:          bool,
}

Node :: struct {
	id:             types.Node_Id, // unique; identifies the node's partitions in storage
	op:             Op,
	parent:         ^Node, // nil for a Source
	options: 		NodeOptions,
}

DEFAULT_OPTIONS :: NodeOptions {num_partitions = 1}

@(private)
next_node_id: types.Node_Id

@(private)
new_node :: proc(node: Node) -> ^Node {
	n := new_clone(node)
	n.id = next_node_id
	next_node_id += 1
	return n
}

parallelize :: proc(data: batch.Batch, options := DEFAULT_OPTIONS) -> ^Node {
	fmt.assertf(options.num_partitions >= 1, "num_partitions must be >= 1, got %d", options.num_partitions)
	batch.assert_valid(data)
	return new_node(Node{op = clone_slices(Source{data}), options = options})
}

// Only the partitioning is inherited from the parent, not caching.
append_node :: proc(parent: ^Node, op: Op, options := NodeOptions{}) -> ^Node {
	assert(parent != nil, "append_node: parent is nil (use parallelize for sources)")
	opts := options
	if opts.num_partitions == 0 do opts.num_partitions = parent.options.num_partitions
	fmt.assertf(opts.num_partitions >= 1, "num_partitions must be >= 1, got %d", opts.num_partitions)

	switch o in op {
	case Source:    panic("append_node: a Source cannot have a parent (use parallelize)")
	case Map:       assert(len(o.inputs) >= 1, "Map needs at least 1 input column")
	case Filter:    assert(len(o.inputs) >= 1, "Filter needs at least 1 input column")
	case Aggregate: assert(len(o.inputs) >= 1, "Aggregate needs at least 1 input column")
	case Reduce_By_Key:
		assert(len(o.inputs) == 1, "Reduce_By_Key needs exactly 1 input column")
		assert(o.key != "", "Reduce_By_Key needs a key column")
	}

	return new_node(Node{parent = parent, op = clone_slices(op), options = opts})
}

// Slice literals like `inputs = {"x"}` live in the caller's stack frame, so the
// node keeps its own copies. Column data is not copied.
@(private)
clone_slices :: proc(op: Op) -> Op {
	op := op
	switch &o in op {
	case Source:
		o.data.names = slice.clone(o.data.names)
		o.data.columns = slice.clone(o.data.columns)
	case Map:       o.inputs = slice.clone(o.inputs)
	case Filter:    o.inputs = slice.clone(o.inputs)
	case Aggregate: o.inputs = slice.clone(o.inputs)
	case Reduce_By_Key: o.inputs = slice.clone(o.inputs)
	}
	return op
}

cache :: proc(node: ^Node) -> ^Node {
	node.options.cache = true
	return node
}

Compute_Options :: struct {
	max_attempts: int, // per task, and per stage for lost input; 0 = DEFAULT_MAX_ATTEMPTS
}

// Free the result with batch.destroy.
compute :: proc(ctx: ^Compute_Context, node: ^Node, options := Compute_Options{}) -> batch.Batch {
	job := Job{ctx = ctx, options = options}
	defer delete_job(&job)

	submit_stage(&job, node)

	parts := make([]batch.Batch, node.options.num_partitions)
	defer delete(parts)
	for &b, p in parts {
		ok: bool
		b, ok = get_stored_output(&job, node.id, p)
		fmt.assertf(ok, "compute: partition %d of node %d was not stored", p, node.id)
	}
	return batch.concat(parts)
}

count :: proc(sc: ^Compute_Context, node: ^Node, options := Compute_Options{}) -> int {
	result := compute(sc, node, options)
	defer batch.destroy(result)
	return batch.num_rows(result)
}
