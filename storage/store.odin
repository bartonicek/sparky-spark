package storage

import "../batch"
import "../types"

Partition_Key :: struct {
	node_id:         types.Node_Id,
	partition_index: types.Partition_Index,
}

// One bucket of the shuffle output written for one partition of the parent.
Shuffle_Key :: struct {
	shuffle_id:             types.Node_Id, // the Reduce_By_Key node the shuffle feeds
	parent_partition_index: types.Partition_Index,
	bucket_index:           types.Bucket_Index,
}

// Owns the batches put into it. Reads are safe from several threads as long
// as nothing writes at the same time.
Store :: struct {
	partitions: map[Partition_Key]batch.Batch,
	shuffle:    map[Shuffle_Key]batch.Batch,
}

// Takes ownership of `b`; a batch already stored under `key` is freed.
put      :: proc{put_partition, put_shuffle}
get      :: proc{get_partition, get_shuffle}
contains :: proc{contains_partition, contains_shuffle}
remove   :: proc{remove_partition, remove_shuffle}

destroy :: proc(store: ^Store) {
	for _, b in store.partitions do batch.destroy(b)
	for _, b in store.shuffle do batch.destroy(b)
	delete(store.partitions)
	delete(store.shuffle)
}

put_partition :: proc(store: ^Store, key: Partition_Key, b: batch.Batch) { put_in(&store.partitions, key, b) }
put_shuffle   :: proc(store: ^Store, key: Shuffle_Key, b: batch.Batch)   { put_in(&store.shuffle, key, b) }

get_partition :: proc(store: ^Store, key: Partition_Key) -> (batch.Batch, bool) { return store.partitions[key] }
get_shuffle   :: proc(store: ^Store, key: Shuffle_Key) -> (batch.Batch, bool)   { return store.shuffle[key] }

contains_partition :: proc(store: ^Store, key: Partition_Key) -> bool { return key in store.partitions }
contains_shuffle   :: proc(store: ^Store, key: Shuffle_Key) -> bool   { return key in store.shuffle }

remove_partition :: proc(store: ^Store, key: Partition_Key) { remove_from(&store.partitions, key) }
remove_shuffle   :: proc(store: ^Store, key: Shuffle_Key)   { remove_from(&store.shuffle, key) }

@(private)
put_in :: proc(m: ^map[$K]batch.Batch, key: K, b: batch.Batch) {
	remove_from(m, key)
	m[key] = b
}

@(private)
remove_from :: proc(m: ^map[$K]batch.Batch, key: K) {
	if b, ok := m[key]; ok {
		batch.destroy(b)
		delete_key(m, key)
	}
}
