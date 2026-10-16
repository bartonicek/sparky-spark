package pipeline

import "../batch"
import "core:thread"

Task_Status :: enum {
	Ok,
	Crashed,       // result was lost; it is retried
	Missing_Input, // upstream input is missing; recomputed, then retried
}

@(private)
Task_Result :: struct {
	status:  Task_Status,
	part:    batch.Batch,   // regular stages
	buckets: []batch.Batch, // shuffle map stages: one per partition of `shuffle`
}

@(private)
run_task :: proc(task: thread.Task) {
	ctx := (^Stage_Context)(task.data)
	partition := task.user_index
	defer free_all(context.temp_allocator)

	result := &ctx.results[partition]
	if is_missing_input(ctx.job, ctx.node, partition) {
		result.status = .Missing_Input
		return
	}

	it := new_iterator(ctx.job, ctx.node, partition)
	defer free_iterator(it)

	if ctx.shuffle != nil {
		op := ctx.shuffle.op.(Reduce_By_Key)
		result.buckets = shuffle_fold_and_split(it, op, ctx.shuffle.options.num_partitions)
	} else {
		batches := make([dynamic]batch.Batch, context.temp_allocator)
		for b in next(it) do append(&batches, b)
		// Copy out of temp memory so the result outlives the task.
		result.part = batch.concat(batches[:])
	}
	result.status = .Ok
}

// As if the task never reported back.
@(private)
discard_result :: proc(result: ^Task_Result) {
	batch.destroy(result.part)
	for b in result.buckets do batch.destroy(b)
	delete(result.buckets)
	result^ = {status = .Crashed}
}
