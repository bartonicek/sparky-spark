package batch

import "core:fmt"
import "core:slice"
import "core:strings"

// All columns have the same length; names[i] is the name of columns[i].
Batch :: struct {
	names:   []string,
	columns: []Column,
}

num_rows :: proc(batch: Batch) -> int {
	if len(batch.columns) == 0 do return 0
	return column_len(batch.columns[0])
}

assert_valid :: proc(batch: Batch, loc := #caller_location) {
	fmt.assertf(len(batch.names) == len(batch.columns),
		"batch has %d names but %d columns", len(batch.names), len(batch.columns), loc = loc)
	n := num_rows(batch)
	for col, i in batch.columns {
		fmt.assertf(column_len(col) == n,
			"column %q has %d rows, expected %d", batch.names[i], column_len(col), n, loc = loc)
	}
}

get_column_index :: proc(batch: Batch, name: string) -> (index: int, ok: bool) {
	for n, i in batch.names do if n == name do return i, true
	return -1, false
}

get_column :: proc(batch: Batch, name: string) -> Column {
	if i, ok := get_column_index(batch, name); ok do return batch.columns[i]
	fmt.panicf("no column %q in batch with columns %v", name, batch.names)
}

get_columns :: proc(batch: Batch, names: []string, allocator := context.allocator) -> []Column {
	columns := make([]Column, len(names), allocator)
	for name, i in names do columns[i] = get_column(batch, name)
	return columns
}

// The columns are views into `batch`, not copies.
slice_rows :: proc(batch: Batch, start, end: int, allocator := context.allocator) -> Batch {
	columns := make([]Column, len(batch.columns), allocator)
	for col, i in batch.columns do columns[i] = column_slice(col, start, end)
	return Batch{batch.names, columns}
}

filter_rows :: proc(batch: Batch, mask: []bool, allocator := context.allocator) -> Batch {
	fmt.assertf(len(mask) == num_rows(batch),
		"filter mask has %d entries, batch has %d rows", len(mask), num_rows(batch))
	columns := make([]Column, len(batch.columns), allocator)
	for col, i in batch.columns do columns[i] = column_filter(col, mask, allocator)
	return Batch{batch.names, columns}
}

// Replaces column `name` if it exists, appends it otherwise.
with_column :: proc(batch: Batch, name: string, col: Column, allocator := context.allocator) -> Batch {
	if i, ok := get_column_index(batch, name); ok {
		return replace_column(batch, i, col, allocator)
	}
	return append_column(batch, name, col, allocator)
}

// The other columns are shared with `batch`, not copied.
replace_column :: proc(batch: Batch, index: int, col: Column, allocator := context.allocator) -> Batch {
	fmt.assertf(column_len(col) == num_rows(batch),
		"new column %q has %d rows, batch has %d", batch.names[index], column_len(col), num_rows(batch))
	columns := make([]Column, len(batch.columns), allocator)
	copy(columns, batch.columns)
	columns[index] = col
	return Batch{batch.names, columns}
}

// The existing columns are shared with `batch`, not copied.
append_column :: proc(batch: Batch, name: string, col: Column, allocator := context.allocator) -> Batch {
	_, exists := get_column_index(batch, name)
	fmt.assertf(!exists, "batch already has a column %q", name)
	if len(batch.columns) > 0 {
		fmt.assertf(column_len(col) == num_rows(batch),
			"new column %q has %d rows, batch has %d", name, column_len(col), num_rows(batch))
	}

	names := make([]string, len(batch.names) + 1, allocator)
	copy(names, batch.names)
	names[len(batch.names)] = name

	columns := make([]Column, len(batch.columns) + 1, allocator)
	copy(columns, batch.columns)
	columns[len(batch.columns)] = col

	return Batch{names, columns}
}

from_value :: proc(name: string, x: Value, allocator := context.allocator) -> Batch {
	names := make([]string, 1, allocator)
	names[0] = name
	columns := make([]Column, 1, allocator)
	columns[0] = value_to_column(x, allocator)
	return Batch{names, columns}
}

// The result owns all its memory, names included (free it with destroy).
// Batches without columns (empty partitions) are skipped.
concat :: proc(batches: []Batch, allocator := context.allocator) -> Batch {
	template: Batch
	for batch in batches do if len(batch.columns) > 0 {
		template = batch
		break
	}
	if len(template.columns) == 0 do return {}

	names := make([]string, len(template.names), allocator)
	for n, i in template.names do names[i] = strings.clone(n, allocator)

	parts := make([dynamic]Column, 0, len(batches), allocator)
	defer delete(parts)

	columns := make([]Column, len(template.columns), allocator)
	for i in 0..<len(columns) {
		clear(&parts)
		for b in batches do if len(b.columns) > 0 {
			fmt.assertf(slice.equal(b.names, template.names),
				"cannot concat batches with columns %v and %v", b.names, template.names)
			append(&parts, b.columns[i])
		}
		columns[i] = column_concat(parts[:], allocator)
	}

	return Batch{names, columns}
}

// Only for batches made by concat.
destroy :: proc(batch: Batch, allocator := context.allocator) {
	for col in batch.columns do column_delete(col, allocator)
	for name in batch.names do delete(name, allocator)
	delete(batch.columns, allocator)
	delete(batch.names, allocator)
}
