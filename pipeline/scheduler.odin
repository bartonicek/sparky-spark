#+private
package pipeline

// Splits a job into stages at wide and cached nodes, runs them in
// dependency order, and tracks which outputs are stored.

import "../batch"
import "../storage"
import "../types"
import "core:fmt"
import "core:reflect"
import "core:slice"
import "core:strings"

Job :: struct {
	ctx: ^Compute_Context,
	options: Compute_Options,
	num_stages: int,

	// Outputs of finished stages. Only written between stages, so tasks can
	// read it without locks.
	node_outputs:    storage.Store,
}

delete_job :: proc(job: ^Job) {
	storage.destroy(&job.node_outputs)
}

get_stored_output :: proc(job: ^Job, node_id: types.Node_Id, partition: int) -> (b: batch.Batch, ok: bool) {
	key := storage.Partition_Key{node_id, types.Partition_Index(partition)}
	if b, ok := storage.get(&job.ctx.cached_output, key); ok do return b, true
	return storage.get(&job.node_outputs, key)
}

// Every partition is stored, in the cache or in the job's store.
all_outputs_stored :: proc(job: ^Job, node: ^Node) -> bool {
	missing := find_missing_partitions(job, node)
	defer delete(missing)
	return len(missing) == 0
}

is_cached :: proc(job: ^Job, node: ^Node) -> bool {
	for p in 0..<node.options.num_partitions {
		if !storage.contains(&job.ctx.cached_output, storage.Partition_Key{node.id, types.Partition_Index(p)}) do return false
	}
	return true
}

find_missing_partitions :: proc(job: ^Job, node: ^Node) -> [dynamic]int {
	missing := make([dynamic]int)
	for p in 0..<node.options.num_partitions {
		if _, ok := get_stored_output(job, node.id, p); !ok do append(&missing, p)
	}
	return missing
}

// True if the task for `partition` of `node` would read stored data that is
// gone. Narrow nodes keep the partition index, so the walk can follow it.
is_missing_input :: proc(job: ^Job, node: ^Node, partition: int) -> bool {
	for n := node; n.parent != nil; n = n.parent {
		if _, ok := get_stored_output(job, n.id, partition); ok do return false
		if !begins_stage(n) do continue

		if _, shuffles := n.op.(Reduce_By_Key); shuffles {
			for m in 0..<n.parent.options.num_partitions {
				key := storage.Shuffle_Key{n.id, types.Partition_Index(m), types.Bucket_Index(partition)}
				if !storage.contains(&job.node_outputs, key) do return true
			}
		} else {
			start, end := chunk(partition, n.options.num_partitions, n.parent.options.num_partitions)
			for p in start..<end {
				if _, ok := get_stored_output(job, n.parent.id, p); !ok do return true
			}
		}
		return false
	}
	return false
}

// Each partition of a wide node reads several parent partitions.
is_wide :: proc(node: ^Node) -> bool {
	if node.parent == nil do return false
	_, shuffles := node.op.(Reduce_By_Key)
	return shuffles || node.options.num_partitions != node.parent.options.num_partitions
}

// The parent's output gets stored, so a new stage starts at `node`.
begins_stage :: proc(node: ^Node) -> bool {
	return node.parent != nil && (is_wide(node) || node.parent.options.cache)
}

// Runs the stages `node` depends on, then its own for the partitions that
// aren't stored yet, and stores the output.
submit_stage :: proc(job: ^Job, node: ^Node) {
	if all_outputs_stored(job, node) {
		if is_cached(job, node) {
			fmt.printfln("Skipping stage: %v output is cached", reflect.union_variant_typeid(node.op))
		}
		return
	}

	submit_dependencies(job, node)

	stage_id := job.num_stages
	job.num_stages += 1
	notify_stage(.Before, job, stage_id, node, false)

	missing := find_missing_partitions(job, node)
	defer delete(missing)
	description := describe_stage(job, node)
	defer delete(description)
	fmt.printfln("Stage %d: %s (%d tasks)", stage_id, description, len(missing))

	results := run_stage(job, node, missing[:], stage_id)
	defer delete(results)
	target := &job.ctx.cached_output if node.options.cache else &job.node_outputs
	for p in missing do storage.put(target, storage.Partition_Key{node.id, types.Partition_Index(p)}, results[p].part)
	notify_stage(.After, job, stage_id, node, false)

	// Recomputing lost input along the way may have lost some of this output.
	if !all_outputs_stored(job, node) do submit_stage(job, node)
}

// Runs the earlier stages that the stage ending at `node` reads from.
submit_dependencies :: proc(job: ^Job, node: ^Node) {
	// Walk up to where this stage starts.
	for current := node; current.parent != nil; current = current.parent {
		if !begins_stage(current) do continue

		// The parent ends an earlier stage.
		if _, ok := current.op.(Reduce_By_Key); ok {
			submit_shuffle_map_stage(job, current)
		} else {
			submit_stage(job, current.parent)
		}
		return
	}
}

// Runs the stage ending at `shuffle.parent`, splitting each task's output
// into one bucket per partition of `shuffle`.
submit_shuffle_map_stage :: proc(job: ^Job, shuffle: ^Node) {
	if shuffle_is_stored(job, shuffle) do return

	parent := shuffle.parent
	if parent.options.cache {
		submit_stage(job, parent) // fill the cache, then read from it
	} else {
		submit_dependencies(job, parent)
	}

	stage_id := job.num_stages
	job.num_stages += 1
	notify_stage(.Before, job, stage_id, parent, true)

	missing := find_incomplete_map_partitions(job, shuffle)
	defer delete(missing)
	description := describe_stage(job, parent)
	defer delete(description)
	fmt.printfln("Stage %d: %s → shuffle write (%d tasks)", stage_id, description, len(missing))

	results := run_stage(job, parent, missing[:], stage_id, shuffle)
	defer delete(results)
	for m in missing {
		for b, bucket in results[m].buckets {
			key := storage.Shuffle_Key{shuffle.id, types.Partition_Index(m), types.Bucket_Index(bucket)}
			storage.put(&job.node_outputs, key, b)
		}
		delete(results[m].buckets)
	}
	notify_stage(.After, job, stage_id, parent, true)

	if !shuffle_is_stored(job, shuffle) do submit_shuffle_map_stage(job, shuffle)
}

// Map partitions with at least one bucket missing.
find_incomplete_map_partitions :: proc(job: ^Job, shuffle: ^Node) -> [dynamic]int {
	missing := make([dynamic]int)
	for m in 0..<shuffle.parent.options.num_partitions {
		for bucket in 0..<shuffle.options.num_partitions {
			key := storage.Shuffle_Key{shuffle.id, types.Partition_Index(m), types.Bucket_Index(bucket)}
			if !storage.contains(&job.node_outputs, key) {
				append(&missing, m)
				break
			}
		}
	}
	return missing
}

shuffle_is_stored :: proc(job: ^Job, shuffle: ^Node) -> bool {
	incomplete := find_incomplete_map_partitions(job, shuffle)
	defer delete(incomplete)
	return len(incomplete) == 0
}

// E.g. "Source → Filter → Map → Aggregate", "stored → Aggregate" or
// "shuffle read → Reduce_By_Key".
describe_stage :: proc(job: ^Job, node: ^Node) -> string {
	if all_outputs_stored(job, node) {
		return strings.clone(is_cached(job, node) ? "cached" : "stored")
	}

	names: [dynamic]string
	defer delete(names)

	for n := node; ; n = n.parent {
		append(&names, fmt.aprint(reflect.union_variant_typeid(n.op)))
		if n.parent == nil do break
		if begins_stage(n) {
			_, shuffles := n.op.(Reduce_By_Key)
			label := "shuffle read" if shuffles else ("cached" if n.parent.options.cache else "stored")
			append(&names, strings.clone(label))
			break
		}
	}
	slice.reverse(names[:])
	defer for name in names do delete(name)

	return strings.join(names[:], " → ")
}
