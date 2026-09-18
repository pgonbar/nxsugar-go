package nxsugar

// LogFields accumulates business fields that the framework merges into the
// canonical task log line emitted at task completion (completed, failed and
// the time-exceeded warning). It does not emit anything by itself: there is
// exactly one canonical line per task, enriched at completion.
//
// Rules: business fields only (counts, statuses, short identifiers) — never
// payloads (same principle as ParamsInLogs, but app-driven). Reserved framework
// keys (task_id, rpc.method, rpc.service, user, duration_s, pull_index,
// type, error.message) should not be used. Thread-safe; safe on zero-value
// tasks (e.g. built by test helpers without initialization).
//
// Enrichment belongs to the handler — the task's transaction boundary. Layers
// that only receive the context do not enrich the canonical line: their data
// bubbles up via return values, and they keep their usual channels (own logs,
// spans, metrics).
func (t *Task) LogFields(fields map[string]interface{}) {
	if t == nil || len(fields) == 0 {
		return
	}
	t.logFieldsMu.Lock()
	defer t.logFieldsMu.Unlock()
	if t.logFields == nil {
		t.logFields = make(map[string]interface{}, len(fields))
	}
	for k, v := range fields {
		t.logFields[k] = v
	}
}

// mergedLogFields returns a copy of the accumulated fields, or nil when the
// task has none. Used by the framework at canonical-line emission time.
func (t *Task) mergedLogFields() map[string]interface{} {
	if t == nil {
		return nil
	}
	t.logFieldsMu.Lock()
	defer t.logFieldsMu.Unlock()
	if len(t.logFields) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(t.logFields))
	for k, v := range t.logFields {
		out[k] = v
	}
	return out
}
