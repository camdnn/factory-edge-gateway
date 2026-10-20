// package machine : types used by whole program
package machine

import (
	"context"
	"time"
)

// State is a normalized state machine
type State string

const (
	StateIdle    State = "IDLE"
	StateRunning State = "RUNNING"
	StateAlarm   State = "ALARM"
	StateUnknown State = "UNKNOWN"
)

// Status is one observation of a machine at a point in time
type Status struct {
	MachineID  string    `json:"machine_id"`           // Unique MachineID
	State      State     `json:"state"`                // Normalized State
	PartCount  int64     `json:"part_count"`           // Total Parts Made
	SpindleRPM float64   `json:"spindle_rpm"`          // How fast cutting tool spins
	AlarmCode  string    `json:"alarm_code,omitempty"` // Machines error code
	ObservedAt time.Time `json:"observed_at"`          // When agent reads status
}

type Machine interface {
	// ID returns a unique name for machine
	ID() string

	// Poll reads the machines current status. Will give up when ctx is cancelled
	Poll(ctx context.Context) (Status, error)
}
