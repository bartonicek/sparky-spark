#+private
package pipeline

// Runs one stage's tasks on a thread pool and retries the ones that fail.

import "core:fmt"
import "core:slice"
import "core:thread"

// Shared by all tasks of a stage; each task gets its partition as
// task.user_index and writes only results[partition], so no locks are needed.
Stage_Context :: struct {
	stage_id: int,
	job:      ^Job,
	node:     ^Node,
	results:  []Task_Result,
	shuffle:  ^Node, // shuffle map stages only: the Reduce_By_Key node being fed
}

// Returns one result per partition of `node`; only `partitions` are filled in,
// all with status .Ok. With `shuffle`, the tasks write shuffle buckets for it.
run_stage :: proc(job: ^Job, node: ^Node, partitions: []int, stage_id: int, shuffle: ^Node = nil) -> []Task_Result {
	// On the stack, which is fine: run_with_retries waits for every task.
	ctx := Stage_Context{job = job, node = node, shuffle = shuffle, stage_id = stage_id}
	ctx.results = make([]Task_Result, node.options.num_partitions)
	run_with_retries(&ctx, partitions)
	return ctx.results
}

// Reruns only the tasks that failed. A crashed task is simply run again, and
// counts against that task's attempts. A task whose stored input was lost
// needs that input recomputed first; that counts against the stage instead.
run_with_retries :: proc(ctx: ^Stage_Context, partitions: []int) {
	crashes := make([]int, ctx.node.options.num_partitions)
	defer delete(crashes)
	input_losses := 0

	max_attempts := ctx.job.options.max_attempts
	if max_attempts == 0 do max_attempts = DEFAULT_MAX_ATTEMPTS

	pending := slice.clone_to_dynamic(partitions)
	defer delete(pending)

	for len(pending) > 0 {
		run_tasks(ctx, pending[:])

		for p in pending {
			event := Task_Event{stage_id = ctx.stage_id, partition = p, status = ctx.results[p].status}
			notify_task(&event)
			if event.discard && event.status == .Ok do discard_result(&ctx.results[p])
		}

		input_lost := false
		failed := make([dynamic]int, 0, len(pending))
		for p in pending {
			switch ctx.results[p].status {
			case .Ok:
			case .Crashed:
				crashes[p] += 1
				fmt.printfln("  Stage %d, task %d crashed (%d/%d)", ctx.stage_id, p, crashes[p], max_attempts)
				fmt.assertf(crashes[p] < max_attempts, "stage %d: task %d crashed %d times, giving up", ctx.stage_id, p, crashes[p])
				append(&failed, p)
			case .Missing_Input:
				fmt.printfln("  Stage %d, task %d lost its input, recomputing it", ctx.stage_id, p)
				append(&failed, p)
				input_lost = true
			}
		}
		delete(pending)
		pending = failed

		if input_lost {
			input_losses += 1
			fmt.assertf(input_losses < max_attempts, "stage %d: lost its input %d times, giving up", ctx.stage_id, input_losses)
			submit_dependencies(ctx.job, ctx.node)
		}
	}
}

run_tasks :: proc(ctx: ^Stage_Context, partitions: []int) {
	pool: thread.Pool
	thread.pool_init(&pool, context.allocator, len(partitions))
	defer thread.pool_destroy(&pool)

	for p in partitions do thread.pool_add_task(&pool, context.allocator, run_task, ctx, p)

	thread.pool_start(&pool)
	thread.pool_finish(&pool)
}
