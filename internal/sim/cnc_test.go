package sim

import (
	"testing"
	"time"
)

// test over .1 seconds 200 times (20 seconds)
func TestCNCRuns(t *testing.T) {
	c := NewCNC("cnc-1")
	for i := 0; i < 200; i++ {
		c.Step(100 * time.Millisecond)
		t.Log(c.Snapshot(time.Now()))
	}
}
