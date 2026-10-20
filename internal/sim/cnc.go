// Package sim: make a cnc mill simulation state machine
package sim

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/machine"
)

// CNC struct simulates a cnc machine
type CNC struct {
	id        string        // Unique CNC identifier
	state     machine.State // State of CNC Machine
	partCount int64         // the number of parts made
	alarmCode string        // Alarm code of the CNC machine
	remaining time.Duration // time left in current cycle or alarm
	mu        sync.Mutex    // mutual exclusion lock for goroutines
}

// NewCNC Init a new CNC machine and set status to idle
func NewCNC(id string) *CNC {
	return &CNC{
		id:        id,
		state:     machine.StateIdle,
		remaining: 0,
	}
}

const (
	cycleTime     = 4 * time.Second
	alarmDuration = 6 * time.Second
	startChance   = 0.5  // chance for a machine to run
	alarmChance   = 0.02 // chance for an alarm fo occur
)

func (c *CNC) Step(dt time.Duration) {
	// lock to ensure no rece conditions modifying CNC
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	// Advances the remaining time for simluation depending on its state
	case machine.StateIdle:
		if rand.Float64() < startChance {
			c.state = machine.StateRunning
			c.remaining = cycleTime
		}
		// set a initial timer for the machine to simulate making a part
	case machine.StateRunning:
		//  handle a running state, decrementing by dt

		// add random falure chance
		if rand.Float64() < alarmChance {
			c.state = machine.StateAlarm
			c.alarmCode = "SPINDLE_OVERLOAD"
			c.remaining = alarmDuration
			break
		}
		c.remaining -= dt
		if c.remaining <= 0 {
			c.partCount += 1
			c.state = machine.StateIdle
		}
	case machine.StateAlarm:
		// when the machine has an alarm decrement remaining time
		c.remaining -= dt
		if c.remaining <= 0 {
			c.alarmCode = ""
			c.state = machine.StateIdle
		}
	}
}

// Snapshot returns current status of c as machine status
func (c *CNC) Snapshot(now time.Time) machine.Status {
	c.mu.Lock()
	defer c.mu.Unlock()

	var spin float64 = 0
	if c.state == machine.StateRunning {
		spin = 8000
	}
	return machine.Status{
		MachineID:  c.id,
		State:      c.state,
		PartCount:  c.partCount,
		SpindleRPM: spin,
		AlarmCode:  c.alarmCode,
		ObservedAt: now.UTC(),
	}
}
