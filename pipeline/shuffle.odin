#+private
package pipeline

import "../batch"
import "../storage"
import "../types"

AccumulatorMap :: map[batch.Value]batch.Value

output_name :: proc(op: Reduce_By_Key) -> string {
	return op.output if op.output != "" else op.inputs[0]
}

// Map side: folds the partition by key, then splits
// the accumulators into one bucket per reduce partition by hash(key).
shuffle_fold_and_split :: proc(it: ^Iter, op: Reduce_By_Key, n_buckets: int) -> []batch.Batch {
	accs := make(AccumulatorMap, allocator = context.temp_allocator)
	for b in next(it) do fold_by_key(&accs, b, op.key, op.inputs, op.fn, op.unit)

	keys := make([][dynamic]batch.Value, n_buckets, context.temp_allocator)
	values := make([][dynamic]batch.Value, n_buckets, context.temp_allocator)
	for i in 0..<n_buckets {
		keys[i] = make([dynamic]batch.Value, context.temp_allocator)
		values[i] = make([dynamic]batch.Value, context.temp_allocator)
	}
	for k, v in accs {
		bucket := int(batch.value_hash(k) % u64(n_buckets))
		append(&keys[bucket], k)
		append(&values[bucket], v)
	}

	out := make([]batch.Batch, n_buckets)
	for i in 0..<n_buckets {
		b := create_key_value_batch(op.key, output_name(op), keys[i][:], values[i][:], context.temp_allocator)
		out[i] = batch.concat({b}) // copy out of temp memory
	}
	return out
}

// Reduce side: merges bucket `partition` of every map task's output.
shuffle_merge_and_fold :: proc(job: ^Job, node: ^Node, partition: int) -> batch.Batch {
	op := node.op.(Reduce_By_Key)
	output := output_name(op)

	accs := make(AccumulatorMap, allocator = context.temp_allocator)
	for i in 0..<node.parent.options.num_partitions {
		key := storage.Shuffle_Key{node.id, types.Partition_Index(i), types.Bucket_Index(partition)}
		b, ok := storage.get(&job.node_outputs, key)
		assert(ok, "shuffle_merge_and_fold: missing map output")
		fold_by_key(&accs, b, op.key, {output}, op.fn, op.unit)
	}

	keys := make([dynamic]batch.Value, 0, len(accs), context.temp_allocator)
	values := make([dynamic]batch.Value, 0, len(accs), context.temp_allocator)
	for k, v in accs {
		append(&keys, k)
		append(&values, v)
	}
	return create_key_value_batch(op.key, output, keys[:], values[:], context.temp_allocator)
}

// Folds the rows of `b` into `accs`, one accumulator per distinct key. Each
// group of rows is passed to `fn` as its own columns.
fold_by_key :: proc(accs: ^AccumulatorMap, b: batch.Batch, key: string, inputs: []string, fn: Aggregate_Fn, unit: Unit_Fn) {
	keys := batch.get_column(b, key)
	values := batch.get_columns(b, inputs, context.temp_allocator)

	group_indices := make(map[batch.Value][dynamic]int, allocator = context.temp_allocator)
	for i in 0..<batch.num_rows(b) {
		k := batch.column_value(keys, i)
		// Read, append, write back: `append(&group_indices[k], i)` would append to a copy.
		rows := group_indices[k]
		if cap(rows) == 0 do rows = make([dynamic]int, 0, 4, context.temp_allocator)
		append(&rows, i)
		group_indices[k] = rows
	}

	for k, rows in group_indices {
		group := make([]batch.Column, len(values), context.temp_allocator)
		for col, j in values do group[j] = batch.column_take(col, rows[:], context.temp_allocator)
		acc, seen := accs[k]
		if !seen do acc = unit()
		accs[k] = fn(acc, group)
	}
}

create_key_value_batch :: proc(key, value: string, keys, values: []batch.Value, allocator := context.allocator) -> batch.Batch {
	names := make([]string, 2, allocator)
	names[0], names[1] = key, value
	columns := make([]batch.Column, 2, allocator)
	columns[0] = batch.column_from_values(keys, allocator)
	columns[1] = batch.column_from_values(values, allocator)
	return batch.Batch{names, columns}
}
