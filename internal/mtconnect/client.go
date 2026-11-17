package mtconnect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Cursor is where to resume reading from an agent. The zero value means
// "start from a fresh /current snapshot".
type Cursor struct {
	InstanceID uint64 `json:"instance_id"`
	Epoch      uint64 `json:"epoch,omitempty"` // see Observation.Epoch
	Next       uint64 `json:"next"`
}

// Client reads a real MTConnect agent's buffer without skipping sequences.
type Client struct {
	BaseURL string // e.g. https://demo.mtconnect.org
	HTTP    *http.Client
	Count   int // max observations per /sample request
}

// GapReason says why observations could not be collected.
type GapReason string

const (
	GapAgentRestarted GapReason = "agent restarted"
	GapFellBehind     GapReason = "fell behind the agent's buffer"
	// the agent broke the standard: it numbered sequences again without a new instance (ADR 0005)
	GapSequencesBackwards GapReason = "sequences went backwards"
)

// Gap describes observations the agent no longer has, so they were never collected.
type Gap struct {
	DetectedAt time.Time `json:"detected_at"`
	InstanceID uint64    `json:"instance_id"`     // agent instance the missed sequences belonged to
	Epoch      uint64    `json:"epoch,omitempty"` // and their epoch
	From       uint64    `json:"from_sequence"`   // first sequence never collected
	ResumedAt  uint64    `json:"resumed_at"`      // sequence collection resumed at (in the agent's current instance)
	// the epoch collection resumed in: 0 after a restart, Epoch+1 after sequences went backwards
	ResumedEpoch uint64    `json:"resumed_epoch,omitempty"`
	Missed       uint64    `json:"missed"` // 0 when unknowable (the agent restarted)
	Reason       GapReason `json:"reason"`
}

// Next returns every observation after cur and the cursor to use next time.
// gap is non-nil when data was lost because the agent restarted or we fell
// behind its ring buffer; the caller resumes from a fresh /current snapshot.
func (c *Client) Next(ctx context.Context, cur Cursor) (next Cursor, obs []Observation, gap *Gap, err error) {
	if cur.Next == 0 {
		next, obs, err = c.current(ctx)
		return next, obs, nil, err
	}

	q := url.Values{"from": {strconv.FormatUint(cur.Next, 10)}, "count": {strconv.Itoa(c.Count)}}
	h, obs, err := c.get(ctx, "/sample?"+q.Encode())
	var ae *AgentError
	switch {
	case errors.As(err, &ae) && ae.Code == "OUT_OF_RANGE":
		// asked for sequences the agent already dropped (or never had after a restart)
	case err != nil:
		return cur, nil, nil, err
	case h.InstanceID != cur.InstanceID || cur.Next < h.FirstSequence:
		// agent restarted, or our sequences already left its buffer
	default:
		return Cursor{InstanceID: h.InstanceID, Epoch: cur.Epoch, Next: h.NextSequence}, inEpoch(obs, cur.Epoch), nil, nil
	}

	next, obs, err = c.current(ctx)
	if err != nil {
		return cur, nil, nil, err
	}
	gap = &Gap{InstanceID: cur.InstanceID, Epoch: cur.Epoch, From: cur.Next, ResumedAt: next.Next, Reason: GapFellBehind}
	switch {
	case next.InstanceID != cur.InstanceID:
		gap.Reason = GapAgentRestarted
	case next.Next < cur.Next: // a new epoch keeps the reused sequences apart from the old ones
		gap.Reason = GapSequencesBackwards
		next.Epoch = cur.Epoch + 1
	default:
		next.Epoch = cur.Epoch
		gap.Missed = next.Next - cur.Next
	}
	gap.ResumedEpoch = next.Epoch
	return next, inEpoch(obs, next.Epoch), gap, nil
}

// inEpoch gives every observation the epoch it was read in.
func inEpoch(obs []Observation, epoch uint64) []Observation {
	for i := range obs {
		obs[i].Epoch = epoch
	}
	return obs
}

func (c *Client) current(ctx context.Context) (Cursor, []Observation, error) {
	h, obs, err := c.get(ctx, "/current")
	if err != nil {
		return Cursor{}, nil, err
	}
	return Cursor{InstanceID: h.InstanceID, Next: h.NextSequence}, obs, nil
}

func (c *Client) get(ctx context.Context, path string) (Header, []Observation, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+path, nil)
	if err != nil {
		return Header{}, nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Header{}, nil, err
	}
	defer resp.Body.Close()

	// agents answer errors like OUT_OF_RANGE with a 400 and an MTConnectError body, so parse either way
	h, obs, err := ParseStreams(resp.Body)
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("mtconnect agent: GET %s: %s", path, resp.Status)
	}
	return h, obs, err
}
