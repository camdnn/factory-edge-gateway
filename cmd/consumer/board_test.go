package main

import (
	"testing"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

var now = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

func obs(device, category, typ, value string, ago time.Duration) mtconnect.Observation {
	return mtconnect.Observation{Device: device, DeviceUUID: device, Component: device, DataItemID: device + typ + value,
		Category: category, Type: typ, Value: value, Timestamp: now.Add(-ago)}
}

func TestMachineStatusWorstConditionWinsAndAlarmsSortFirst(t *testing.T) {
	latest := []mtconnect.Observation{
		obs("Agent", "Event", "Availability", "AVAILABLE", time.Second),
		obs("Okuma", "Condition", "SYSTEM", "Normal", time.Second),
		obs("Okuma", "Condition", "TEMPERATURE", "Unavailable", time.Second), // one silent sensor is not a status
		obs("Mazak", "Condition", "SYSTEM", "Warning: RAPID FEED OVERRIDE SET AT 0", time.Hour),
		obs("Mazak", "Condition", "LOGIC_PROGRAM", "Fault: Program error", 95*time.Second),
		obs("Haas", "Condition", "SYSTEM", "Warning: DESIGNATED DATA NOT FOUND", 4*time.Hour),
	}

	got := machineStatuses(latest, now)

	want := []struct {
		name   string
		status Status
		reason string
		since  time.Time
	}{
		{"Mazak", StatusAlarm, "Program error", now.Add(-95 * time.Second)},
		{"Haas", StatusWarning, "DESIGNATED DATA NOT FOUND", now.Add(-4 * time.Hour)},
		{"Okuma", StatusOK, "All conditions normal", time.Time{}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d machines %+v, want %d (the Agent is not a machine)", len(got), got, len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.Name != w.name || g.Status != w.status || g.Reason != w.reason || !g.Since.Equal(w.since) {
			t.Errorf("machine %d = {%s %s %q since %v}, want {%s %s %q since %v}", i, g.Name, g.Status, g.Reason, g.Since, w.name, w.status, w.reason, w.since)
		}
	}
}

func TestMachineStatusOfflineWhenUnavailableOrSilentForFiveMinutes(t *testing.T) {
	got := machineStatuses([]mtconnect.Observation{
		obs("Silent", "Event", "Execution", "ACTIVE", 6*time.Minute),
		obs("Unplugged", "Event", "Availability", "UNAVAILABLE", 30*time.Second),
		obs("Recent", "Event", "Execution", "ACTIVE", 4*time.Minute),
		obs("Faulted", "Condition", "SYSTEM", "Fault: Overload", 10*time.Minute), // an alarm still outranks silence
		func() mtconnect.Observation { // a component going unavailable is not the machine going offline
			o := obs("Busy", "Event", "Availability", "UNAVAILABLE", time.Second)
			o.Component = "BarFeeder"
			return o
		}(),
	}, now)

	want := map[string]Status{"Faulted": StatusAlarm, "Silent": StatusOffline, "Unplugged": StatusOffline, "Recent": StatusOK, "Busy": StatusOK}
	for _, m := range got {
		if m.Status != want[m.Name] {
			t.Errorf("%s = %s, want %s", m.Name, m.Status, want[m.Name])
		}
	}
	if s := got[1]; s.Name != "Silent" || s.Reason != "No new observations" || !s.Since.Equal(now.Add(-6*time.Minute)) {
		t.Errorf("silent machine = %+v, want \"No new observations\" since its last observation", s)
	}
}

func TestMachineCardShowsActualSpindleActiveProgramAndNewestObservation(t *testing.T) {
	item := func(component, typ, subType, value string, ago time.Duration) mtconnect.Observation {
		o := obs("OKUMA", "Sample", typ, value, ago)
		o.Component, o.SubType, o.DataItemID = component, subType, component+typ+subType
		return o
	}
	got := machineStatuses([]mtconnect.Observation{
		item("Controller", "Execution", "", "PROGRAM_COMPLETED", 3*time.Second),
		item("C2", "RotaryVelocity", "ACTUAL", "0", 2*time.Second),
		item("C1", "RotaryVelocity", "PROGRAMMED", "300", 2*time.Second),
		item("C1", "RotaryVelocity", "ACTUAL", "271", 2*time.Second),
		item("Controller", "PartCount", "", "12", time.Minute),
		item("path", "Program", "MAIN", "O1000", time.Hour),
		item("path", "Program", "ACTIVE", "IMTS-2022-1E-MM.MIN", time.Hour),
		item("Controller", "Load", "", "4", time.Second),
	}, now)[0]

	if got.RunState != "PROGRAM_COMPLETED" || got.Spindle != "271" || got.PartCount != "12" ||
		got.Program != "IMTS-2022-1E-MM.MIN" || !got.LastObservation.Equal(now.Add(-time.Second)) {
		t.Errorf("card = %+v, want run PROGRAM_COMPLETED, spindle 271 (C1 actual), parts 12, active program IMTS-2022-1E-MM.MIN, last observation 1s ago", got)
	}
}

func TestAgentAvailabilityComesFromTheAgentsOwnObservation(t *testing.T) {
	if got := agentAvailability(nil); got != "UNKNOWN" {
		t.Errorf("no observations: got %q, want UNKNOWN", got)
	}
	got := agentAvailability([]mtconnect.Observation{
		obs("Mazak", "Event", "Availability", "AVAILABLE", time.Second),
		obs("Agent", "Event", "Availability", "UNAVAILABLE", time.Second),
	})
	if got != "UNAVAILABLE" {
		t.Errorf("got %q, want the Agent's own UNAVAILABLE, not a machine's", got)
	}
}

func TestPipelineHealth(t *testing.T) {
	recent := now.Add(-2 * time.Second)
	oldGap := &mtconnect.Gap{DetectedAt: now.Add(-61 * time.Minute)}
	newGap := &mtconnect.Gap{DetectedAt: now.Add(-59 * time.Minute)}
	tests := []struct {
		name         string
		lastReceived time.Time
		lag          int64
		agent        string
		gap          *mtconnect.Gap
		want         Verdict
		why          string
		kafkaErr     string
	}{
		{"flowing", recent, 999, "AVAILABLE", oldGap, VerdictHealthy, "Observations flowing, nothing lost recently", ""},
		{"never received", time.Time{}, 0, "AVAILABLE", nil, VerdictDown, "Nothing received yet", ""},
		{"silent for 31s", now.Add(-31 * time.Second), 0, "AVAILABLE", nil, VerdictDown, "Nothing received for over 30s", ""},
		{"silent for 29s", now.Add(-29 * time.Second), 0, "AVAILABLE", nil, VerdictHealthy, "Observations flowing, nothing lost recently", ""},
		{"behind", recent, 1000, "AVAILABLE", nil, VerdictDegraded, "Consumer is 1000 observations behind", ""},
		{"agent down", recent, 0, "UNAVAILABLE", nil, VerdictDegraded, "MTConnect agent is UNAVAILABLE", ""},
		{"gap this hour", recent, 0, "AVAILABLE", newGap, VerdictDegraded, "Gap in the last hour", ""},
		{"silence beats lag", now.Add(-time.Minute), 5000, "UNAVAILABLE", newGap, VerdictDown, "Nothing received for over 30s", ""},
		{"never received, kafka unreachable", time.Time{}, 0, "UNKNOWN", nil, VerdictDown, "Kafka unreachable: connection refused", "connection refused"},
		{"kafka unreachable", now.Add(-time.Minute), 0, "AVAILABLE", nil, VerdictDown, "Kafka unreachable: dial tcp: connection refused", "dial tcp: connection refused"},
		{"kafka error while flowing is not down", recent, 0, "AVAILABLE", nil, VerdictHealthy, "Observations flowing, nothing lost recently", "dial tcp: connection refused"},
	}
	for _, tt := range tests {
		got := pipelineHealth(tt.lastReceived, tt.lag, tt.agent, tt.gap, tt.kafkaErr, now)
		if got.Verdict != tt.want || got.Why != tt.why {
			t.Errorf("%s: got %s %q, want %s %q", tt.name, got.Verdict, got.Why, tt.want, tt.why)
		}
	}
}

func TestMachineStatusEmergencyStopIsAnAlarm(t *testing.T) {
	got := machineStatuses([]mtconnect.Observation{
		obs("Mazak", "Condition", "SYSTEM", "Normal", time.Second),
		obs("Mazak", "Event", "EmergencyStop", "TRIGGERED", 10*time.Second),
	}, now)
	if got[0].Status != StatusAlarm || got[0].Reason != "Emergency stop" || !got[0].Since.Equal(now.Add(-10*time.Second)) {
		t.Errorf("got %+v, want Alarm \"Emergency stop\" since 10s ago", got[0])
	}
}
