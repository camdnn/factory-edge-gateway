package main

import (
	"testing"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

func TestRecoverCursorFromEmptyTopicIsFirstRun(t *testing.T) {
	cur, ok := recoverCursor(nil)
	if ok {
		t.Fatalf("got cursor %+v from an empty topic, want first run", cur)
	}
}

func TestRecoverCursorResumesAfterHighestSequence(t *testing.T) {
	// a partition whose machines have been quiet ends lower than a busy one
	cur, ok := recoverCursor([]mtconnect.Observation{
		tail(7, 1040),
		tail(7, 1187),
		tail(7, 912),
	})
	want := mtconnect.Cursor{InstanceID: 7, Next: 1188}
	if !ok || cur != want {
		t.Fatalf("got %+v (ok=%v), want %+v", cur, ok, want)
	}
}

func TestRecoverCursorFollowsNewestAgentInstance(t *testing.T) {
	// the agent restarted and numbers sequences from 1 again; one partition
	// last heard from its machines before the restart
	cur, ok := recoverCursor([]mtconnect.Observation{
		tail(1791887049, 90210),
		tail(1791990000, 41),
		tail(1791990000, 63),
	})
	want := mtconnect.Cursor{InstanceID: 1791990000, Next: 64}
	if !ok || cur != want {
		t.Fatalf("got %+v (ok=%v), want %+v", cur, ok, want)
	}
}

// tail builds the newest observation record of one partition.
func tail(instance, sequence uint64) mtconnect.Observation {
	return mtconnect.Observation{InstanceID: instance, Sequence: sequence}
}
