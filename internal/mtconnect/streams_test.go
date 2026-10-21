package mtconnect

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// testdata/sample.xml is a real /sample response from demo.mtconnect.org,
// with one Events/Condition component added so every category is covered.
func TestParseStreams(t *testing.T) {
	f, err := os.Open("testdata/sample.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, obs, err := ParseStreams(f)
	if err != nil {
		t.Fatal(err)
	}
	if h.InstanceID != 1791887049 || h.FirstSequence != 30942971 || h.NextSequence != 31074012 {
		t.Errorf("header = %+v", h)
	}
	if len(obs) != 15 {
		t.Fatalf("got %d observations, want 15", len(obs))
	}

	find := func(id string) Observation {
		for _, o := range obs {
			if o.DataItemID == id {
				return o
			}
		}
		t.Fatalf("no observation %q", id)
		return Observation{}
	}

	load := find("LZ1load")
	if load.Device != "OKUMA" || load.DeviceUUID != "OKUMA.123456" || load.Component != "Z1" ||
		load.Category != "Sample" || load.Type != "Load" || load.Value != "4" || load.Sequence != 31074001 ||
		load.InstanceID != 1791887049 || load.Timestamp.IsZero() {
		t.Errorf("sample = %+v", load)
	}
	if exec := find("execution"); exec.Category != "Event" || exec.Type != "Execution" || exec.Value != "ACTIVE" {
		t.Errorf("event = %+v", exec)
	}
	if cond := find("logic_cond"); cond.Category != "Condition" || cond.Type != "LOGIC_PROGRAM" || cond.Value != "Fault: Program error" {
		t.Errorf("condition = %+v", cond)
	}
}

func TestParseStreamsAgentError(t *testing.T) {
	for name, doc := range map[string]string{
		// what demo.mtconnect.org (2.8) sends for a stale 'from'
		"2.x": `<?xml version="1.0"?><MTConnectError xmlns="urn:mtconnect.org:MTConnectError:2.8"><Header instanceId="1"/>` +
			`<Errors><OutOfRange errorCode="OUT_OF_RANGE"><ErrorMessage>'from' must be greater than 5</ErrorMessage></OutOfRange></Errors></MTConnectError>`,
		"1.x": `<?xml version="1.0"?><MTConnectError xmlns="urn:mtconnect.org:MTConnectError:1.3"><Header instanceId="1"/>` +
			`<Errors><Error errorCode="OUT_OF_RANGE">'from' must be greater than 5</Error></Errors></MTConnectError>`,
	} {
		_, _, err := ParseStreams(strings.NewReader(doc))
		var ae *AgentError
		if !errors.As(err, &ae) || ae.Code != "OUT_OF_RANGE" || ae.Text != "'from' must be greater than 5" {
			t.Errorf("%s: err = %v, want OUT_OF_RANGE AgentError", name, err)
		}
	}
}
