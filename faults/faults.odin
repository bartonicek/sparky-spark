// Random crashes and data loss for testing the pipeline's recovery.
package faults

import "../pipeline"
import "../storage"
import "core:fmt"
import "core:math/rand"
import "core:slice"

// Install with `pipeline.add_hooks(hooks(&chaos))`. Rates are probabilities
// in [0, 1]; the same seed gives the same faults, because the pipeline calls
// the hooks in a fixed order from a single thread.
Chaos :: struct {
	task_failure_rate: f64,
	data_loss_rate:    f64,
	rng:               rand.Default_Random_State,
}

make_chaos :: proc(task_failure_rate, data_loss_rate: f64, seed: u64) -> Chaos {
	return {task_failure_rate, data_loss_rate, rand.create(seed)}
}

hooks :: proc(chaos: ^Chaos) -> pipeline.Hooks {
	return {data = chaos, before_stage = lose_data, after_task = crash_task}
}

@(private)
roll :: proc(chaos: ^Chaos) -> f64 {
	return rand.float64(rand.default_random_generator(&chaos.rng))
}

@(private)
crash_task :: proc(data: rawptr, event: ^pipeline.Task_Event) {
	chaos := (^Chaos)(data)
	if event.status != .Ok || chaos.task_failure_rate <= 0 do return
	event.discard = roll(chaos) < chaos.task_failure_rate
}

@(private)
lose_data :: proc(data: rawptr, event: ^pipeline.Stage_Event) {
	chaos := (^Chaos)(data)
	if chaos.data_loss_rate <= 0 do return
	stored, cached := event.stored, event.cached

	lose_partitions(chaos, stored, "stored")
	lose_partitions(chaos, cached, "cached")

	// Maps iterate in no fixed order, so sort the keys before rolling.
	keys := make([dynamic]storage.Shuffle_Key, 0, len(stored.shuffle))
	defer delete(keys)
	for key in stored.shuffle do append(&keys, key)
	slice.sort_by(keys[:], proc(a, b: storage.Shuffle_Key) -> bool {
		if a.shuffle_id != b.shuffle_id do return a.shuffle_id < b.shuffle_id
		if a.parent_partition_index != b.parent_partition_index do return a.parent_partition_index < b.parent_partition_index
		return a.bucket_index < b.bucket_index
	})
	for key in keys {
		if roll(chaos) >= chaos.data_loss_rate do continue
		fmt.printfln("  Lost shuffle bucket %d of map partition %d (shuffle %d)",
			key.bucket_index, key.parent_partition_index, key.shuffle_id)
		storage.remove(stored, key)
	}
}

@(private)
lose_partitions :: proc(chaos: ^Chaos, store: ^storage.Store, label: string) {
	keys := make([dynamic]storage.Partition_Key, 0, len(store.partitions))
	defer delete(keys)
	for key in store.partitions do append(&keys, key)
	slice.sort_by(keys[:], proc(a, b: storage.Partition_Key) -> bool {
		if a.node_id != b.node_id do return a.node_id < b.node_id
		return a.partition_index < b.partition_index
	})
	for key in keys {
		if roll(chaos) >= chaos.data_loss_rate do continue
		fmt.printfln("  Lost %s partition %d of node %d", label, key.partition_index, key.node_id)
		storage.remove(store, key)
	}
}
