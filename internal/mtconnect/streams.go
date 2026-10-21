package mtconnect

import (
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"time"
)

// Header is the buffer bookkeeping a real agent sends with every response.
type Header struct {
	InstanceID    uint64 `xml:"instanceId,attr"`    // changes when the agent restarts
	FirstSequence uint64 `xml:"firstSequence,attr"` // oldest item still in the agent's buffer
	LastSequence  uint64 `xml:"lastSequence,attr"`  // newest item in the buffer
	NextSequence  uint64 `xml:"nextSequence,attr"`  // where the next /sample should start
}

// Observation is one data item value reported by a real agent.
type Observation struct {
	InstanceID uint64    `json:"instance_id"`
	Sequence   uint64    `json:"sequence"`
	Device     string    `json:"device"`
	DeviceUUID string    `json:"device_uuid"`
	Component  string    `json:"component"`
	DataItemID string    `json:"data_item_id"`
	Name       string    `json:"name,omitempty"`
	Category   string    `json:"category"` // Sample, Event or Condition
	Type       string    `json:"type"`     // e.g. RotaryVelocity, Execution, LOGIC_PROGRAM
	SubType    string    `json:"sub_type,omitempty"`
	Value      string    `json:"value"` // for conditions: the level plus any message, e.g. "Fault: Program error"
	Timestamp  time.Time `json:"timestamp"`
}

// AgentError is an MTConnectError document, e.g. OUT_OF_RANGE when we ask
// for a sequence the agent no longer has.
type AgentError struct {
	Code string
	Text string
}

func (e *AgentError) Error() string { return fmt.Sprintf("mtconnect agent: %s: %s", e.Code, e.Text) }

// streamsDoc mirrors the parts of MTConnectStreams (and MTConnectError) we read.
// Untagged element names match regardless of the agent's XML namespace.
type streamsDoc struct {
	XMLName xml.Name
	Header  Header `xml:"Header"`
	Devices []struct {
		Name       string `xml:"name,attr"`
		UUID       string `xml:"uuid,attr"`
		Components []struct {
			Component string   `xml:"component,attr"`
			Name      string   `xml:"name,attr"`
			Samples   itemList `xml:"Samples"`
			Events    itemList `xml:"Events"`
			Condition itemList `xml:"Condition"`
		} `xml:"ComponentStream"`
	} `xml:"Streams>DeviceStream"`
	// 1.x: <Error errorCode="...">text</Error>; 2.x: <OutOfRange errorCode="..."><ErrorMessage>text</ErrorMessage>
	Errors struct {
		Items []struct {
			Code    string `xml:"errorCode,attr"`
			Text    string `xml:",chardata"`
			Message string `xml:"ErrorMessage"`
		} `xml:",any"`
	} `xml:"Errors"`
}

// itemList collects every child element, whatever its data item type.
type itemList struct {
	Items []item `xml:",any"`
}

type item struct {
	XMLName    xml.Name
	DataItemID string    `xml:"dataItemId,attr"`
	Name       string    `xml:"name,attr"`
	Sequence   uint64    `xml:"sequence,attr"`
	Timestamp  time.Time `xml:"timestamp,attr"`
	SubType    string    `xml:"subType,attr"`
	Type       string    `xml:"type,attr"` // only set on conditions
	// ponytail: DataSet/Table items (nested <Entry>) flatten to their raw text; parse entries if a consumer needs them
	Value string `xml:",chardata"`
}

// ParseStreams reads a real /current or /sample response into flat observations.
func ParseStreams(r io.Reader) (Header, []Observation, error) {
	var doc streamsDoc
	if err := xml.NewDecoder(r).Decode(&doc); err != nil {
		return Header{}, nil, fmt.Errorf("decode mtconnect: %w", err)
	}
	if doc.XMLName.Local == "MTConnectError" {
		if len(doc.Errors.Items) == 0 {
			return Header{}, nil, &AgentError{Code: "UNKNOWN"}
		}
		e := doc.Errors.Items[0]
		return Header{}, nil, &AgentError{Code: e.Code, Text: strings.TrimSpace(e.Message + e.Text)}
	}
	if doc.XMLName.Local != "MTConnectStreams" {
		return Header{}, nil, fmt.Errorf("unexpected mtconnect document %q", doc.XMLName.Local)
	}

	var obs []Observation
	for _, d := range doc.Devices {
		for _, c := range d.Components {
			add := func(category string, items []item) {
				for _, it := range items {
					o := Observation{
						InstanceID: doc.Header.InstanceID,
						Sequence:   it.Sequence,
						Device:     d.Name,
						DeviceUUID: d.UUID,
						Component:  c.Name,
						DataItemID: it.DataItemID,
						Name:       it.Name,
						Category:   category,
						Type:       it.XMLName.Local,
						SubType:    it.SubType,
						Value:      strings.TrimSpace(it.Value),
						Timestamp:  it.Timestamp,
					}
					// conditions are named by level (<Normal>, <Fault>); the type is an attribute
					if category == "Condition" {
						o.Type, o.Value = it.Type, it.XMLName.Local
						if text := strings.TrimSpace(it.Value); text != "" {
							o.Value += ": " + text
						}
					}
					obs = append(obs, o)
				}
			}
			add("Sample", c.Samples.Items)
			add("Event", c.Events.Items)
			add("Condition", c.Condition.Items)
		}
	}
	return doc.Header, obs, nil
}
