package pipeline

import "../storage"
import "../types"

Compute_Context :: struct {
	cached_output: storage.Store,
}

delete_context :: proc(ctx: ^Compute_Context) {
	storage.destroy(&ctx.cached_output)
}

uncache :: proc(ctx: ^Compute_Context, node: ^Node) {
	for i in 0..<node.options.num_partitions {
		key := storage.Partition_Key{node.id, types.Partition_Index(i)}
		storage.remove(&ctx.cached_output, key)
	}
	node.options.cache = false
}
