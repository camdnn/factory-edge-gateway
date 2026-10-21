// Package mtconnect specifies how to format data as xml
package mtconnect

import (
	"encoding/xml"
	"strconv"

	"github.com/camdnn/factory-edge-gateway/internal/machine"
)

// Execution is MTConnect's execution state of a machine.
type Execution string

const (
	ExecReady       Execution = "READY"       // StateIdle
	ExecActive      Execution = "ACTIVE"      // StateRunning
	ExecStopped     Execution = "STOPPED"     // StateAlarm
	ExecUnavailable Execution = "UNAVAILABLE" // StateUnknown
)

// Streams is the root of an MTConnect /current response.
type Streams struct {
	XMLName xml.Name       `xml:"MTConnectStreams"`
	Devices []DeviceStream `xml:"Streams>DeviceStream"`
}

// DeviceStream holds the data for one machine.
type DeviceStream struct {
	Name      string          `xml:"name,attr"`       // an attribute: machine name
	Component ComponentStream `xml:"ComponentStream"` // child element
}

// ComponentStream groups a machine's data by category.
type ComponentStream struct {
	Events    Events    `xml:"Events"`    // state of machine
	Samples   Samples   `xml:"Samples"`   // continuous measurements of machine
	Condition Condition `xml:"Condition"` // health of machine
}

// Events holds discrete values that change at a specific moment.
type Events struct {
	Execution Execution `xml:"Execution"` // IDLE, RUNNING, ALARM
	PartCount string    `xml:"PartCount"` // snapshot of number of parts
}

// Samples holds continuously varying measurements.
type Samples struct {
	RotaryVelocity string `xml:"RotaryVelocity"`
}

// Condition reports machine health: Normal or Fault.
type Condition struct {
	Normal      *struct{} `xml:"Normal"`
	Fault       *Fault    `xml:"Fault"`
	Unavailable *struct{} `xml:"Unavailable"`
}

// Fault describes an active alarm.
type Fault struct {
	NativeCode string `xml:"nativeCode,attr"`
	Text       string `xml:",chardata"`
}

// FromStatus converts a machine status into an MTConnect document.
func FromStatus(s machine.Status) Streams {
	exec := ExecReady
	cond := Condition{Normal: &struct{}{}}

	switch s.State {
	case machine.StateRunning:
		exec = ExecActive
	case machine.StateAlarm:
		exec = ExecStopped
		cond = Condition{Fault: &Fault{NativeCode: s.AlarmCode, Text: "Machine alarm"}}
	case machine.StateUnknown:
		exec = ExecUnavailable
		cond = Condition{Unavailable: &struct{}{}}
	}

	return Streams{
		Devices: []DeviceStream{{
			Name: s.MachineID,
			Component: ComponentStream{
				Events: Events{
					Execution: exec,
					PartCount: strconv.FormatInt(s.PartCount, 10),
				},
				Samples: Samples{
					RotaryVelocity: strconv.FormatFloat(s.SpindleRPM, 'f', 0, 64),
				},
				Condition: cond,
			},
		}},
	}
}
