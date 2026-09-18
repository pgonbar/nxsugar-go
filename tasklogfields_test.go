package nxsugar

import (
	"sync"
	"testing"
)

func TestLogFieldsNilSafe(t *testing.T) {
	var task *Task
	task.LogFields(map[string]interface{}{"k": "v"}) // must not panic

	t2 := &Task{}
	t2.LogFields(nil)                      // must not panic
	t2.LogFields(map[string]interface{}{}) // must not panic
	if got := t2.mergedLogFields(); got != nil {
		t.Fatalf("expected nil fields for empty task, got %v", got)
	}
}

func TestLogFieldsMergeAndOverwrite(t *testing.T) {
	task := &Task{}
	task.LogFields(map[string]interface{}{"elements": 3})
	task.LogFields(map[string]interface{}{"domains": 1})
	task.LogFields(map[string]interface{}{"elements": 5}) // overwrite wins

	got := task.mergedLogFields()
	if got["elements"] != 5 || got["domains"] != 1 {
		t.Fatalf("unexpected merged fields: %v", got)
	}

	// mergedLogFields must return a copy: mutating it must not affect the task
	got["elements"] = 0
	if task.mergedLogFields()["elements"] != 5 {
		t.Fatal("mergedLogFields must return a copy of the fields")
	}
}

func TestLogFieldsConcurrent(t *testing.T) {
	task := &Task{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task.LogFields(map[string]interface{}{"k": i})
		}(i)
	}
	wg.Wait()
	if task.mergedLogFields()["k"] == nil {
		t.Fatal("expected key k to be present after concurrent writes")
	}
}
