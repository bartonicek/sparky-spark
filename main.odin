package main

import "batch"
import "faults"
import "pipeline"
import "core:fmt"
import "core:math"
import "core:slice"
import "core:strings"

is_even :: proc(inputs: []batch.Column) -> []bool {
	assert(len(inputs) == 1, "is_even: expects 1 input column")
	result := make([]bool, batch.column_len(inputs[0]), context.temp_allocator)
	#partial switch v in inputs[0] {
	case []i64:
		for x, i in v do result[i] = x % 2 == 0
	case []f64:
		for x, i in v do result[i] = math.mod(x, 2) == 0 // only whole floats can be even
	case:
		panic("is_even: not a number")
	}
	return result
}

square :: proc(inputs: []batch.Column) -> batch.Column {
	assert(len(inputs) == 1, "square: expects 1 input column")
	#partial switch v in inputs[0] {
	case []i64:
		out := make([]i64, len(v), context.temp_allocator)
		for x, i in v do out[i] = x * x
		return out
	case []f64:
		out := make([]f64, len(v), context.temp_allocator)
		for x, i in v do out[i] = x * x
		return out
	}
	panic("square: not a number")
}

// Both columns must have the same type.
multiply :: proc(inputs: []batch.Column) -> batch.Column {
	assert(len(inputs) == 2, "multiply: expects 2 input columns")
	assert(batch.column_len(inputs[0]) == batch.column_len(inputs[1]), "multiply: columns have different lengths")
	#partial switch x in inputs[0] {
	case []i64:
		y := inputs[1].([]i64)
		out := make([]i64, len(x), context.temp_allocator)
		for i in 0..<len(x) do out[i] = x[i] * y[i]
		return out
	case []f64:
		y := inputs[1].([]f64)
		out := make([]f64, len(x), context.temp_allocator)
		for i in 0..<len(x) do out[i] = x[i] * y[i]
		return out
	}
	panic("multiply: not a number")
}

sum :: proc(acc: batch.Value, inputs: []batch.Column) -> batch.Value {
	assert(len(inputs) == 1, "sum: expects 1 input column")
	#partial switch v in inputs[0] {
	case []i64: return add(acc, math.sum(v))
	case []f64: return add(acc, math.sum(v))
	}
	panic("sum: not a number")
}

// i64 + i64 stays i64; anything involving an f64 becomes f64.
add :: proc(a: batch.Value, b: batch.Value) -> batch.Value {
	x, x_ok := a.(i64)
	y, y_ok := b.(i64)
	if x_ok && y_ok do return x + y
	return to_f64(a) + to_f64(b)
}

to_f64 :: proc(x: batch.Value) -> f64 {
	#partial switch v in x {
	case i64: return f64(v)
	case f64: return v
	}
	panic("to_f64: not a number")
}

zero :: proc() -> batch.Value { return i64(0) }

// 1 for every row, so summing it counts rows.
one :: proc(inputs: []batch.Column) -> batch.Column {
	out := make([]i64, batch.column_len(inputs[0]), context.temp_allocator)
	for &x in out do x = 1
	return out
}

last_digit :: proc(inputs: []batch.Column) -> batch.Column {
	assert(len(inputs) == 1, "last_digit: expects 1 input column")
	xs := inputs[0].([]i64)
	out := make([]i64, len(xs), context.temp_allocator)
	for x, i in xs do out[i] = x % 10
	return out
}

// Reduce_By_Key output comes in no particular order, so sort it for printing.
print_sorted :: proc(label: string, b: batch.Batch, key, value: string) {
	Row :: struct { key: batch.Value, value: batch.Value }
	rows := make([]Row, batch.num_rows(b))
	defer delete(rows)
	keys, values := batch.get_column(b, key), batch.get_column(b, value)
	for &r, i in rows do r = {batch.column_value(keys, i), batch.column_value(values, i)}

	slice.sort_by(rows, proc(a, b: Row) -> bool {
		// Biggest value first, then by key.
		if a.value.(i64) != b.value.(i64) do return a.value.(i64) > b.value.(i64)
		return fmt.tprint(a.key) < fmt.tprint(b.key)
	})
	fmt.print(label)
	for r in rows do fmt.printf(" %v=%v", r.key, r.value)
	fmt.println()
}

seq :: proc(n: int) -> []i64 {
	data := make([]i64, n)
	for i in 0..<n do data[i] = i64(i)
	return data
}

// Filter, Map with one and two inputs, caching, and a two-level Aggregate.
squares_example :: proc(ctx: ^pipeline.Compute_Context) {
	data := seq(20)
	defer delete(data)

	nums  := pipeline.parallelize(batch.Batch{names = {"x"}, columns = {data}}, pipeline.NodeOptions{num_partitions=3})
	evens := pipeline.append_node(nums, pipeline.Filter{is_even, {"x"}})
	squares    := pipeline.cache(pipeline.append_node(evens, pipeline.Map{fn = square, inputs = {"x"}, output = "x_squared"}))
	cubes    := pipeline.append_node(squares, pipeline.Map{fn = multiply, inputs = {"x", "x_squared"}, output = "x_cubed"})
	partials := pipeline.append_node(squares, pipeline.Aggregate{fn = sum, unit = zero, inputs = {"x_squared"}})
	total    := pipeline.append_node(partials, pipeline.Aggregate{fn = sum, unit = zero, inputs = {"x_squared"}}, pipeline.NodeOptions{num_partitions = 1})

	squares_out := pipeline.compute(ctx, squares)
	defer batch.destroy(squares_out)
	fmt.println("Columns:", squares_out.names)
	fmt.println("x:", batch.get_column(squares_out, "x"))
	fmt.println("x_squared:", batch.get_column(squares_out, "x_squared"))

	cubes_out := pipeline.compute(ctx, cubes)
	defer batch.destroy(cubes_out)
	fmt.println("Columns:", cubes_out.names)
	fmt.println("x_cubed:", batch.get_column(cubes_out, "x_cubed"))

	partials_out := pipeline.compute(ctx, partials)
	defer batch.destroy(partials_out)
	fmt.println("Partial sums:", batch.get_column(partials_out, "x_squared"))

	total_out := pipeline.compute(ctx, total)
	defer batch.destroy(total_out)
	fmt.println("Sum:", batch.get_column(total_out, "x_squared"))
}

// Map every word to 1, then sum per word. The shuffle sends every occurrence
// of a word to the same reduce partition.
word_count_example :: proc(ctx: ^pipeline.Compute_Context) {
	text := "the quick brown fox jumps over the lazy dog the dog barks and the fox runs"
	words := strings.fields(text)
	defer delete(words)

	word_rows := pipeline.parallelize(batch.Batch{names = {"word"}, columns = {words}}, pipeline.NodeOptions{num_partitions = 3})
	ones := pipeline.append_node(word_rows, pipeline.Map{fn = one, inputs = {"word"}, output = "count"})
	counts := pipeline.append_node(ones, pipeline.Reduce_By_Key{fn = sum, unit = zero, key = "word", inputs = {"count"}}, pipeline.NodeOptions{num_partitions = 2})

	counts_out := pipeline.compute(ctx, counts)
	defer batch.destroy(counts_out)
	print_sorted("Word counts:", counts_out, "word", "count")
}

// Sum of x per last digit of x, checked against a plain loop.
digit_sums_example :: proc(ctx: ^pipeline.Compute_Context) {
	data := seq(100)
	defer delete(data)

	nums := pipeline.parallelize(batch.Batch{names = {"x"}, columns = {data}}, pipeline.NodeOptions{num_partitions = 3})
	digits := pipeline.append_node(nums, pipeline.Map{fn = last_digit, inputs = {"x"}, output = "digit"})
	by_digit := pipeline.append_node(digits, pipeline.Reduce_By_Key{fn = sum, unit = zero, key = "digit", inputs = {"x"}, output = "total"}, pipeline.NodeOptions{num_partitions = 4})

	by_digit_out := pipeline.compute(ctx, by_digit)
	defer batch.destroy(by_digit_out)
	print_sorted("Sum by last digit:", by_digit_out, "digit", "total")

	expected: [10]i64
	for x in data do expected[x % 10] += x
	fmt.println("Expected:          ", expected)
}

// Word count again, with simulated crashes and lost data. The result must be
// the same as without faults: lost work is recomputed from the lineage.
fault_tolerance_example :: proc(ctx: ^pipeline.Compute_Context) {
	text := "the quick brown fox jumps over the lazy dog the dog barks and the fox runs"
	words := strings.fields(text)
	defer delete(words)

	word_rows := pipeline.parallelize(batch.Batch{names = {"word"}, columns = {words}}, pipeline.NodeOptions{num_partitions = 4})
	ones := pipeline.append_node(word_rows, pipeline.Map{fn = one, inputs = {"word"}, output = "count"})
	counts := pipeline.append_node(ones, pipeline.Reduce_By_Key{fn = sum, unit = zero, key = "word", inputs = {"count"}}, pipeline.NodeOptions{num_partitions = 3})
	total := pipeline.append_node(counts, pipeline.Aggregate{fn = sum, unit = zero, inputs = {"count"}}, pipeline.NodeOptions{num_partitions = 1})

	chaos := faults.make_chaos(task_failure_rate = 0.1, data_loss_rate = 0.1, seed = 3)
	pipeline.add_hooks(faults.hooks(&chaos))
	defer pipeline.remove_hooks(&chaos)

	counts_out := pipeline.compute(ctx, counts)
	defer batch.destroy(counts_out)
	print_sorted("Word counts (with faults):", counts_out, "word", "count")

	total_out := pipeline.compute(ctx, total)
	defer batch.destroy(total_out)
	fmt.println("Total words (with faults):", batch.get_column(total_out, "count"), "expected:", len(words))
}

main :: proc() {
	ctx: pipeline.Compute_Context
	defer pipeline.delete_context(&ctx)

//	squares_example(&ctx)
//	fmt.println()
//	word_count_example(&ctx)
//	fmt.println()
//	digit_sums_example(&ctx)
	fault_tolerance_example(&ctx)
}
