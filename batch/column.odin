package batch

import "core:mem"
import "core:strings"

Column :: union {
	[]i64,
	[]f64,
	[]string,
	[]bool,
}

column_len :: proc(col: Column) -> int {
	switch v in col {
	case []i64:    return len(v)
	case []f64:    return len(v)
	case []string: return len(v)
	case []bool:   return len(v)
	}
	return 0
}

column_slice :: proc(col: Column, start, end: int) -> Column {
	switch v in col {
	case []i64:    return v[start:end]
	case []f64:    return v[start:end]
	case []string: return v[start:end]
	case []bool:   return v[start:end]
	}
	return nil
}

column_filter :: proc(col: Column, mask: []bool, allocator := context.allocator) -> Column {
	switch v in col {
	case []i64:    return filter_typed(v, mask, allocator)
	case []f64:    return filter_typed(v, mask, allocator)
	case []string: return filter_typed(v, mask, allocator)
	case []bool:   return filter_typed(v, mask, allocator)
	}
	return nil
}

@(private)
filter_typed :: proc(x: []$T, mask: []bool, allocator: mem.Allocator) -> []T {
	out := make([dynamic]T, 0, len(x), allocator)
	for x, i in x do if mask[i] do append(&out, x)
	return out[:]
}

// All non-nil columns must have the same type.
column_concat :: proc(cols: []Column, allocator := context.allocator) -> Column {
	for col in cols {
		switch v in col {
		case []i64:    return concat_typed(i64, cols, allocator)
		case []f64:    return concat_typed(f64, cols, allocator)
		case []string: return concat_typed(string, cols, allocator)
		case []bool:   return concat_typed(bool, cols, allocator)
		}
	}
	return nil
}

@(private)
concat_typed :: proc($T: typeid, cols: []Column, allocator: mem.Allocator) -> []T {
	n := 0
	for col in cols do if col != nil do n += len(col.([]T)) // panics on mixed types

	out := make([]T, n, allocator)
	i := 0
	for col in cols do if col != nil {
		i += copy(out[i:], col.([]T))
	}

	// Copy the characters too, so the result doesn't point into the inputs.
	when T == string {
		for &s in out do s = strings.clone(s, allocator)
	}

	return out
}

column_delete :: proc(c: Column, allocator := context.allocator) {
	switch v in c {
	case []i64:    delete(v, allocator)
	case []f64:    delete(v, allocator)
	case []string:
		for s in v do delete(s, allocator)
		delete(v, allocator)
	case []bool:   delete(v, allocator)
	}
}

column_value :: proc(col: Column, i: int) -> Value {
	switch v in col {
	case []i64:    return v[i]
	case []f64:    return v[i]
	case []string: return v[i]
	case []bool:   return v[i]
	}
	return nil
}

// The rows at `indices`, in that order.
column_take :: proc(col: Column, indices: []int, allocator := context.allocator) -> Column {
	switch v in col {
	case []i64:    return take_typed(v, indices, allocator)
	case []f64:    return take_typed(v, indices, allocator)
	case []string: return take_typed(v, indices, allocator)
	case []bool:   return take_typed(v, indices, allocator)
	}
	return nil
}

@(private)
take_typed :: proc(x: []$T, indices: []int, allocator: mem.Allocator) -> []T {
	out := make([]T, len(indices), allocator)
	for index, i in indices do out[i] = x[index]
	return out
}

// All values must have the same type. No values give a nil column.
column_from_values :: proc(values: []Value, allocator := context.allocator) -> Column {
	if len(values) == 0 do return nil
	switch _ in values[0] {
	case i64:    return from_values_typed(i64, values, allocator)
	case f64:    return from_values_typed(f64, values, allocator)
	case string: return from_values_typed(string, values, allocator)
	case bool:   return from_values_typed(bool, values, allocator)
	}
	return nil
}

@(private)
from_values_typed :: proc($T: typeid, values: []Value, allocator: mem.Allocator) -> []T {
	out := make([]T, len(values), allocator)
	for v, i in values do out[i] = v.(T) // panics on mixed types
	return out
}

value_to_column :: proc(x: Value, allocator := context.allocator) -> Column {
	switch v in x {
	case i64:    return slice_of(v, allocator)
	case f64:    return slice_of(v, allocator)
	case string: return slice_of(v, allocator)
	case bool:   return slice_of(v, allocator)
	}
	return nil
}

@(private)
slice_of :: proc(x: $T, allocator: mem.Allocator) -> []T {
	out := make([]T, 1, allocator)
	out[0] = x
	return out
}
