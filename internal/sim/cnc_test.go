package sim

import (
	"testing"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/machine"
)

// test over .1 seconds 200 times simulates 20 seconds for step
func TestCNCRuns(t *testing.T) {
	//	c := NewCNC("cnc-1", uint64(time.Now().UnixNano()))
	c := NewCNC("cnc-1", 42)
	for i := 0; i < 200; i++ {
		c.Step(100 * time.Millisecond)
		t.Log(c.Snapshot(time.Now()))
	}

	if got := c.Snapshot(time.Now()).PartCount; got == 0 {
		t.Fatalf("expected parts after 20s, got %d", got)
	}
}

// test over .1 seconds 100000 times simulates 3 hours for step
func TestCNCRecovers(t *testing.T) {
	//	c := NewCNC("cnc-1", uint64(time.Now().UnixNano()))
	c := NewCNC("cnc-2", 42)
	var prev machine.State
	recoveries := 0
	var lastCount int64

	for i := 0; i < 100000; i++ {
		c.Step(100 * time.Millisecond)
		st := c.Snapshot(time.Now())

		// find the number of recoveries the machine makes
		if prev == machine.StateAlarm && st.State == machine.StateIdle {
			recoveries++
		}
		prev = st.State

		// check if the part cound went down at all
		if st.PartCount < lastCount {
			t.Fatalf("part count went from %d to %d", lastCount, st.PartCount)
		}
		lastCount = st.PartCount
	}

	if recoveries == 0 {
		t.Fatal("Machine never recovered from alarm")
	}
}
