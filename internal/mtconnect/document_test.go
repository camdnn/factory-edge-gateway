package mtconnect

import (
	"encoding/xml"
	"testing"

	"github.com/camdnn/factory-edge-gateway/internal/machine"
)

func TestShape(t *testing.T) {
	doc := Streams{Devices: []DeviceStream{{
		Name: "cnc-1",
		Component: ComponentStream{
			Events:    Events{Execution: ExecActive, PartCount: "0"},
			Samples:   Samples{RotaryVelocity: "8000"},
			Condition: Condition{Normal: &struct{}{}},
		},
	}}}

	if out, err := xml.MarshalIndent(doc, "", " "); err != nil {
		t.Fatal(err)
	} else {
		t.Log(string(out))
	}
}

func TestFromStatus(t *testing.T) {
	tests := []struct {
		name     string
		in       machine.Status
		wantExec Execution
		wantCode string // "" means no fault
	}{
		{"idle", machine.Status{State: machine.StateIdle}, ExecReady, ""},
		{"running", machine.Status{State: machine.StateRunning}, ExecActive, ""},
		{"alarm", machine.Status{State: machine.StateAlarm, AlarmCode: "SPINDLE_OVERLOAD"}, ExecStopped, "SPINDLE_OVERLOAD"},
		{"unknown", machine.Status{State: machine.StateUnknown}, ExecUnavailable, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromStatus(tt.in).Devices[0].Component
			fault := got.Condition.Fault
			switch {
			case tt.wantCode == "" && fault != nil:
				t.Errorf("unexpected fault %q", fault.NativeCode)
			case tt.wantCode != "" && fault == nil:
				t.Errorf("no fault, want code %q", tt.wantCode)
			case tt.wantCode != "" && fault.NativeCode != tt.wantCode:
				t.Errorf("fault code = %q, want %q", fault.NativeCode, tt.wantCode)
			}
		})
	}
}
