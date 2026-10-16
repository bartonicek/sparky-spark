package pipeline

import "../storage"

// Procs the driver calls at points in a job's life, e.g. for logging, metrics
// or fault injection. Only the driver calls them, never tasks, so they run one
// at a time in a fixed order: hooks added earlier run first, and later ones
// see the events as earlier ones left them. Leave a proc nil to skip it.
Hooks :: struct {
	data:         rawptr,
	before_stage: proc(data: rawptr, event: ^Stage_Event),
	after_stage:  proc(data: rawptr, event: ^Stage_Event),
	after_task:   proc(data: rawptr, event: ^Task_Event),
}

hooks: [dynamic]Hooks

add_hooks :: proc(h: Hooks) {
	append(&hooks, h)
}

// Removes the hooks added with this `data`.
remove_hooks :: proc(data: rawptr) {
	for i := len(hooks) - 1; i >= 0; i -= 1 {
		if hooks[i].data == data do ordered_remove(&hooks, i)
	}
	if len(hooks) == 0 {
		delete(hooks)
		hooks = nil // delete frees the buffer but leaves the old pointer in `hooks`
	}
}

Stage_Event :: struct {
	stage_id: int,
	node:     ^Node, // the stage's last node
	shuffle:  bool,  // the stage writes shuffle output for node's child

	// The job's stored outputs and the cache. A before_stage hook may remove
	// entries, e.g. to simulate a lost machine; missing data is recomputed.
	stored, cached: ^storage.Store,
}

Task_Event :: struct {
	stage_id:  int,
	partition: int,
	status:    Task_Status,

	// Set to throw away the result of an .Ok task, as if its executor died
	// before reporting back. The task is then retried.
	discard: bool,
}

DEFAULT_MAX_ATTEMPTS :: 4

@(private)
Stage_Moment :: enum { Before, After }

@(private)
notify_stage :: proc(moment: Stage_Moment, job: ^Job, stage_id: int, node: ^Node, shuffle: bool) {
	event := Stage_Event{stage_id, node, shuffle, &job.node_outputs, &job.ctx.cached_output}
	for h in hooks {
		hook := h.before_stage if moment == .Before else h.after_stage
		if hook != nil do hook(h.data, &event)
	}
}

@(private)
notify_task :: proc(event: ^Task_Event) {
	for h in hooks {
		if h.after_task != nil do h.after_task(h.data, event)
	}
}
