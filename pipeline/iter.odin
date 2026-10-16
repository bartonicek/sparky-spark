#+private
package pipeline

import "../batch"
import "core:fmt"

// Batches passed between iterators are read-only. Source batches are views into
// the source data; all others live in context.temp_allocator until the task ends.
Iter :: struct {
        node:   ^Node,
        parent: ^Iter,   // nil for a Source, or once all parent partitions are read
        // Reads rows [start, end) of `data`: the source data or a stored partition.
        data:   batch.Batch,
        start, end, pos:  int,

        // Parent partitions still to read: [parent_partition, parent_end).
        parent_partition: int,
        parent_end:       int,

        job:        ^Job,
        partition:  int,
        from_store: bool,
}

BATCH_SIZE :: 1024

POSITION_DONE :: max(int)

// Chunk `i` of `total` items split into `n` contiguous, near-equal chunks.
chunk :: proc(i, n, total: int) -> (start, end: int) {
	return i * total / n, (i + 1) * total / n
}

new_iterator :: proc(job: ^Job, node: ^Node, partition := 0) -> ^Iter {
	it := new(Iter)
	it.node = node
	it.job = job
	it.partition = partition
	n := node.options.num_partitions
	fmt.assertf(0 <= partition && partition < n, "partition %d out of range [0, %d)", partition, n)

	if stored, ok := get_stored_output(job, node.id, partition); ok {
		it.from_store = true
		it.data = stored
		it.start, it.end = 0, batch.num_rows(it.data)
		it.pos = it.start
		return it
	}

	switch op in node.op {
	case Source:
		it.data = op.data
		it.start, it.end = chunk(partition, n, batch.num_rows(op.data))
		it.pos = it.start
	case Map, Filter, Aggregate:
		it.parent_partition, it.parent_end = chunk(partition, n, node.parent.options.num_partitions)
		if it.parent_partition < it.parent_end {
			it.parent = new_iterator(job, node.parent, it.parent_partition)
		}
	case Reduce_By_Key:
		// Reads the shuffle output of the parent instead of a parent iterator.
	}

	return it
}

free_iterator :: proc(it: ^Iter) {
	if it == nil do return
	free_iterator(it.parent)
	free(it)
}

// Moves on to the next parent partition whenever the current one runs out.
next_from_parent :: proc(it: ^Iter) -> (result: batch.Batch, ok: bool) {
	for it.parent != nil {
		b, ok := next(it.parent)
		if ok do return b, true

		free_iterator(it.parent)
		it.parent = nil
		it.parent_partition += 1
		if it.parent_partition < it.parent_end {
			it.parent = new_iterator(it.job, it.node.parent, it.parent_partition)
		}
	}

	return {}, false
}

next_slice :: proc(it: ^Iter) -> (result: batch.Batch, ok: bool) {
	assert(it.start <= it.pos && it.pos <= it.end, "iterator position outside [start, end]")
	if it.pos >= it.end do return {}, false
	batch_start := it.pos
	it.pos = min(it.pos + BATCH_SIZE, it.end)
	return batch.slice_rows(it.data, batch_start, it.pos, context.temp_allocator), true
}

next :: proc(it: ^Iter) -> (result: batch.Batch, ok: bool) {
	if it.from_store do return next_slice(it)

	switch op in it.node.op {
	case Source:
		return next_slice(it)

	case Map:
		b := next_from_parent(it) or_return
		inputs := batch.get_columns(b, op.inputs, context.temp_allocator)
		output := op.output if op.output != "" else op.inputs[0]
		return batch.with_column(b, output, op.fn(inputs), context.temp_allocator), true

	case Filter:
		b := next_from_parent(it) or_return
		inputs := batch.get_columns(b, op.inputs, context.temp_allocator)
		return batch.filter_rows(b, op.fn(inputs), context.temp_allocator), true

	case Aggregate:
		if it.pos == POSITION_DONE do return {}, false
		it.pos = POSITION_DONE
		acc := op.unit()
		for b in next_from_parent(it) {
			acc = op.fn(acc, batch.get_columns(b, op.inputs, context.temp_allocator))
		}
		output := op.output if op.output != "" else op.inputs[0]
		return batch.from_value(output, acc, context.temp_allocator), true

	case Reduce_By_Key:
		if it.pos == POSITION_DONE do return {}, false
		it.pos = POSITION_DONE
		return shuffle_merge_and_fold(it.job, it.node, it.partition), true
	}

	unreachable()
}
