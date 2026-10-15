package main

import (
	"context"
	"testing"
	"time"
)

func TestNextBackoffWidensThenCaps(t *testing.T) {
	base := 5 * time.Minute
	cap := 30 * time.Minute
	var current time.Duration
	want := []time.Duration{
		5 * time.Minute, // first failure starts at the idle interval
		10 * time.Minute,
		20 * time.Minute,
		30 * time.Minute, // capped
		30 * time.Minute,
	}
	for i, expect := range want {
		current = nextBackoff(current, base, cap)
		if current != expect {
			t.Fatalf("step %d: backoff = %s; want %s", i, current, expect)
		}
	}
}

func TestNextBackoffStartsAtBase(t *testing.T) {
	if got := nextBackoff(0, 5*time.Minute, time.Hour); got != 5*time.Minute {
		t.Fatalf("got %s; want the base interval", got)
	}
}

func TestSleepUnlessDoneReturnsImmediatelyForZeroDelay(t *testing.T) {
	if !sleepUnlessDone(t.Context(), 0) {
		t.Fatal("a zero delay should not block")
	}
}

func TestSleepUnlessDoneStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now()
	if sleepUnlessDone(ctx, time.Hour) {
		t.Fatal("a cancelled context must stop the worker")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("waited %s; shutdown should be immediate", elapsed)
	}
}
