package batch

import "core:hash"
import "core:mem"

Value :: union {
	i64,
	f64,
	string,
    bool
}

// Same value, same hash, in every run (unlike pointers), so all occurrences of
// a key go to the same shuffle bucket.
value_hash :: proc(v: Value) -> u64 {
	v := v
	switch &x in v {
	case i64:
		return hash.fnv64a(mem.ptr_to_bytes(&x))
	case f64:
		return hash.fnv64a(mem.ptr_to_bytes(&x))
	case string:
		return hash.fnv64a(transmute([]u8)x)
	case bool:
		return 1 if x else 2
	}
	return 0
}
