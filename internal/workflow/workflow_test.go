package workflow

import (
	"testing"
	"time"
)

func TestStepOrder(t *testing.T) {
	got := []string{}
	for s := next(""); s != ""; s = next(s) {
		got = append(got, s)
	}
	want := []string{"upload", "submit", "wait", "download"}
	if len(got) != len(want) {
		t.Fatalf("steps %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("steps %v", got)
		}
	}
	if next("download") != "" {
		t.Fatal("nothing after download")
	}
	if next("submit") != "wait" {
		t.Fatal("resume after submit must wait, not submit again")
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	r := &Runner{BackoffBase: time.Second, BackoffMax: 10 * time.Second}
	r.defaults()
	for attempt := 1; attempt <= 12; attempt++ {
		d := r.backoff(attempt)
		ceiling := time.Second << min(attempt-1, 10)
		if ceiling > 10*time.Second {
			ceiling = 10 * time.Second
		}
		if d < ceiling/2 || d > ceiling {
			t.Fatalf("attempt %d: %s outside [%s, %s]", attempt, d, ceiling/2, ceiling)
		}
	}
}
