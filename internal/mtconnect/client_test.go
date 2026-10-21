package mtconnect

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// fakeAgent serves a buffer holding sequences [first, next) and one observation per response.
type fakeAgent struct {
	instance, first, next uint64
	lastPath              string
}

func (a *fakeAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.lastPath = r.URL.Path
	seq := a.next - 1
	if r.URL.Path == "/sample" {
		from, _ := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
		if from < a.first || from > a.next {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `<MTConnectError><Errors><OutOfRange errorCode="OUT_OF_RANGE"><ErrorMessage>bad from</ErrorMessage></OutOfRange></Errors></MTConnectError>`)
			return
		}
		seq = from
	}
	fmt.Fprintf(w, `<MTConnectStreams><Header instanceId="%d" firstSequence="%d" nextSequence="%d"/>`+
		`<Streams><DeviceStream name="M" uuid="m"><ComponentStream name="c"><Events>`+
		`<Execution dataItemId="exec" sequence="%d" timestamp="2026-10-21T06:10:30Z">ACTIVE</Execution>`+
		`</Events></ComponentStream></DeviceStream></Streams></MTConnectStreams>`,
		a.instance, a.first, a.next, seq)
}

func TestClientNext(t *testing.T) {
	agent := &fakeAgent{instance: 7, first: 100, next: 200}
	srv := httptest.NewServer(agent)
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, HTTP: srv.Client(), Count: 1000}
	ctx := context.Background()

	step := func(cur Cursor, wantPath string, wantGap *Gap, want Cursor) Cursor {
		t.Helper()
		next, obs, gap, err := c.Next(ctx, cur)
		if err != nil {
			t.Fatal(err)
		}
		gapOK := (gap == nil) == (wantGap == nil) && (gap == nil || *gap == *wantGap)
		if agent.lastPath != wantPath || !gapOK || next != want || len(obs) != 1 {
			t.Fatalf("Next(%+v) = %+v gap=%+v obs=%d via %s; want %+v gap=%+v via %s",
				cur, next, gap, len(obs), agent.lastPath, want, wantGap, wantPath)
		}
		return next
	}

	// first run takes a snapshot, no gap
	cur := step(Cursor{}, "/current", nil, Cursor{InstanceID: 7, Next: 200})

	// new data arrives: resume from where we left off
	agent.next = 250
	cur = step(cur, "/sample", nil, Cursor{InstanceID: 7, Next: 250})

	// we fell behind the ring buffer: sequences 250..399 are gone, resnapshot
	agent.first, agent.next = 300, 400
	cur = step(cur, "/current",
		&Gap{InstanceID: 7, From: 250, ResumedAt: 400, Missed: 150, Reason: GapFellBehind},
		Cursor{InstanceID: 7, Next: 400})

	// agent restarted with a new instance whose buffer happens to cover our cursor:
	// how much the old instance produced after 400 is unknowable
	agent.instance, agent.first, agent.next = 8, 1, 500
	cur = step(cur, "/current",
		&Gap{InstanceID: 7, From: 400, ResumedAt: 500, Missed: 0, Reason: GapAgentRestarted},
		Cursor{InstanceID: 8, Next: 500})

	// a misbehaving agent's sequences go backwards without a restart: the missed count
	// must not wrap around to an enormous number
	agent.first, agent.next = 1, 100
	step(cur, "/current",
		&Gap{InstanceID: 8, From: 500, ResumedAt: 100, Missed: 0, Reason: GapFellBehind},
		Cursor{InstanceID: 8, Next: 100})
}
