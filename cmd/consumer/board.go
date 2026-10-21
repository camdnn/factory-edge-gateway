package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

// Status is a machine's single health verdict (see GLOSSARY.md: Machine status).
type Status string

const (
	StatusAlarm   Status = "Alarm"
	StatusWarning Status = "Warning"
	StatusOffline Status = "Offline"
	StatusOK      Status = "OK"
)

var statusRank = map[Status]int{StatusAlarm: 0, StatusWarning: 1, StatusOffline: 2, StatusOK: 3}

// offlineAfter is how long a machine may send nothing new before it counts as Offline.
const offlineAfter = 5 * time.Minute

// agentDevice is the name the MTConnect agent reports itself under; it is not a machine.
const agentDevice = "Agent"

// Machine is one row of the status board.
type Machine struct {
	Name            string    `json:"name"`
	Status          Status    `json:"status"`
	Reason          string    `json:"reason"`
	Since           time.Time `json:"since,omitzero"` // when the condition behind the status began
	RunState        string    `json:"run_state"`
	Spindle         string    `json:"spindle"`
	PartCount       string    `json:"part_count"`
	Program         string    `json:"program"`
	LastObservation time.Time `json:"last_observation"`
}

// machineStatuses turns the latest observation of every data item into one
// status per machine, most severe first.
func machineStatuses(latest []mtconnect.Observation, now time.Time) []Machine {
	byMachine := map[string][]mtconnect.Observation{}
	for _, o := range latest {
		if o.Device != agentDevice {
			byMachine[o.Device] = append(byMachine[o.Device], o)
		}
	}

	machines := make([]Machine, 0, len(byMachine))
	for name, items := range byMachine {
		m := Machine{Name: name, Status: StatusOK, Reason: "All conditions normal"}
		var fault, warning, unavailable *mtconnect.Observation
		var spindleComponent string
		for i, o := range items {
			if o.Timestamp.After(m.LastObservation) {
				m.LastObservation = o.Timestamp
			}
			if o.Category != "Condition" {
				switch o.Type {
				case "Execution":
					m.RunState = o.Value
				case "PartCount":
					m.PartCount = o.Value
				case "Program":
					if m.Program == "" || o.SubType == "ACTIVE" {
						m.Program = o.Value
					}
				case "RotaryVelocity":
					// the measured speed of the first spindle, not the commanded one
					if o.SubType == "ACTUAL" && (spindleComponent == "" || o.Component < spindleComponent) {
						m.Spindle, spindleComponent = o.Value, o.Component
					}
				}
			}
			switch {
			case o.Type == "Availability" && o.Value == "UNAVAILABLE" && o.Component == o.Device:
				// only the machine's own availability; a component (say a bar feeder) can be unavailable on its own
				unavailable = &items[i]
			case o.Category == "Condition" && level(o.Value) == "Fault" && fault == nil:
				fault = &items[i]
			case o.Category == "Condition" && level(o.Value) == "Warning" && warning == nil:
				warning = &items[i]
			case o.Type == "EmergencyStop" && o.Value == "TRIGGERED":
				m.Status, m.Reason, m.Since = StatusAlarm, "Emergency stop", o.Timestamp
			}
		}
		switch {
		case m.Status == StatusAlarm:
		case fault != nil:
			m.Status, m.Reason, m.Since = StatusAlarm, message(*fault), fault.Timestamp
		case warning != nil:
			m.Status, m.Reason, m.Since = StatusWarning, message(*warning), warning.Timestamp
		case unavailable != nil:
			m.Status, m.Reason, m.Since = StatusOffline, "Machine reports unavailable", unavailable.Timestamp
		case now.Sub(m.LastObservation) > offlineAfter:
			m.Status, m.Reason, m.Since = StatusOffline, "No new observations", m.LastObservation
		}
		machines = append(machines, m)
	}

	slices.SortFunc(machines, func(a, b Machine) int {
		return cmp.Or(cmp.Compare(statusRank[a.Status], statusRank[b.Status]), cmp.Compare(a.Name, b.Name))
	})
	return machines
}

// agentAvailability is the MTConnect agent's report on itself, or UNKNOWN before it has sent one.
func agentAvailability(latest []mtconnect.Observation) string {
	for _, o := range latest {
		if o.Device == agentDevice && o.Type == "Availability" {
			return o.Value
		}
	}
	return "UNKNOWN"
}

// level is a condition's level: the value up to its message ("Fault: Program error" -> "Fault").
func level(value string) string {
	l, _, _ := strings.Cut(value, ":")
	return l
}

// message is what a condition says in words, falling back to its type when it says nothing.
func message(o mtconnect.Observation) string {
	if _, msg, ok := strings.Cut(o.Value, ":"); ok && strings.TrimSpace(msg) != "" {
		return strings.TrimSpace(msg)
	}
	return o.Type
}

// Verdict is the pipeline's single verdict.
type Verdict string

const (
	VerdictHealthy  Verdict = "Healthy"
	VerdictDegraded Verdict = "Degraded"
	VerdictDown     Verdict = "Down"
)

const (
	downAfter     = 30 * time.Second // nothing received for this long: Down
	degradedLag   = 1000             // observations waiting in Kafka: Degraded
	recentGapSpan = time.Hour        // a gap this recent: Degraded
)

// Pipeline is the health panel: a verdict and the facts behind it.
type Pipeline struct {
	Verdict      Verdict        `json:"verdict"`
	Why          string         `json:"why"`
	Lag          int64          `json:"lag"`
	LastReceived time.Time      `json:"last_received,omitzero"` // laptop clock
	Agent        string         `json:"agent"`                  // MTConnect agent availability
	LastGap      *mtconnect.Gap `json:"last_gap,omitempty"`
}

// pipelineHealth judges the pipeline: silence is worst, then anything that means observations are late or missing.
// kafkaErr is the consumer's latest Kafka error, if any; it explains silence but is not news while observations flow.
func pipelineHealth(lastReceived time.Time, lag int64, agent string, lastGap *mtconnect.Gap, kafkaErr string, now time.Time) Pipeline {
	p := Pipeline{Verdict: VerdictHealthy, Why: "Observations flowing, nothing lost recently",
		Lag: lag, LastReceived: lastReceived, Agent: agent, LastGap: lastGap}
	silent := lastReceived.IsZero() || now.Sub(lastReceived) > downAfter
	switch {
	case silent && kafkaErr != "":
		p.Verdict, p.Why = VerdictDown, "Kafka unreachable: "+kafkaErr
	case lastReceived.IsZero():
		p.Verdict, p.Why = VerdictDown, "Nothing received yet"
	case silent:
		p.Verdict, p.Why = VerdictDown, fmt.Sprintf("Nothing received for over %s", downAfter)
	case lag >= degradedLag:
		p.Verdict, p.Why = VerdictDegraded, fmt.Sprintf("Consumer is %d observations behind", lag)
	case agent != "AVAILABLE":
		p.Verdict, p.Why = VerdictDegraded, "MTConnect agent is "+agent
	case lastGap != nil && now.Sub(lastGap.DetectedAt) < recentGapSpan:
		p.Verdict, p.Why = VerdictDegraded, "Gap in the last hour"
	}
	return p
}
