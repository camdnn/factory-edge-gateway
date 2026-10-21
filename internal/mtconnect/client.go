package mtconnect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Cursor is where to resume reading from an agent. The zero value means
// "start from a fresh /current snapshot".
type Cursor struct {
	InstanceID uint64 `json:"instance_id"`
	Next       uint64 `json:"next"`
}

// Client reads a real MTConnect agent's buffer without skipping sequences.
type Client struct {
	BaseURL string // e.g. https://demo.mtconnect.org
	HTTP    *http.Client
	Count   int // max observations per /sample request
}

// Next returns every observation after cur and the cursor to use next time.
// gap is true when data was lost because the agent restarted or we fell
// behind its ring buffer; the caller resumes from a fresh /current snapshot.
func (c *Client) Next(ctx context.Context, cur Cursor) (next Cursor, obs []Observation, gap bool, err error) {
	if cur.Next == 0 {
		return c.current(ctx)
	}

	q := url.Values{"from": {strconv.FormatUint(cur.Next, 10)}, "count": {strconv.Itoa(c.Count)}}
	h, obs, err := c.get(ctx, "/sample?"+q.Encode())
	var ae *AgentError
	switch {
	case errors.As(err, &ae) && ae.Code == "OUT_OF_RANGE":
		// asked for sequences the agent already dropped (or never had after a restart)
	case err != nil:
		return cur, nil, false, err
	case h.InstanceID != cur.InstanceID || cur.Next < h.FirstSequence:
		// agent restarted: our sequence numbers mean nothing in its new buffer
	default:
		return Cursor{InstanceID: h.InstanceID, Next: h.NextSequence}, obs, false, nil
	}

	next, obs, _, err = c.current(ctx)
	return next, obs, err == nil, err
}

func (c *Client) current(ctx context.Context) (Cursor, []Observation, bool, error) {
	h, obs, err := c.get(ctx, "/current")
	if err != nil {
		return Cursor{}, nil, false, err
	}
	return Cursor{InstanceID: h.InstanceID, Next: h.NextSequence}, obs, false, nil
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
