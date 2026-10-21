package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

// Kafka redelivers after a crash between the DB write and the offset commit;
// replaying a batch must not duplicate rows or roll the latest value backwards.
func TestStoreReplayIsHarmless(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	t0 := time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC)
	obs := func(seq uint64, value string, ts time.Time) mtconnect.Observation {
		return mtconnect.Observation{InstanceID: 1, Sequence: seq, DeviceUUID: "m", Device: "M", DataItemID: "exec",
			Category: "Event", Type: "Execution", Value: value, Timestamp: ts}
	}
	first := []mtconnect.Observation{obs(10, "READY", t0), obs(11, "ACTIVE", t0.Add(time.Second))}

	if n, err := s.Insert(first); err != nil || n != 2 {
		t.Fatalf("first insert = %d, %v; want 2", n, err)
	}
	if n, err := s.Insert(first[:1]); err != nil || n != 0 {
		t.Fatalf("replayed insert = %d, %v; want 0", n, err)
	}

	if count, _ := s.Count(); count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
	latest, err := s.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 1 || latest[0].Value != "ACTIVE" || !latest[0].Timestamp.Equal(t0.Add(time.Second)) {
		t.Errorf("latest = %+v, want one ACTIVE row", latest)
	}
}
