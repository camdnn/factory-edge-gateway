package mtconnect

import "testing"

// Positions in one instance are ordered by epoch, then sequence; across instances they have no order.
func TestPositionBefore(t *testing.T) {
	at := Position{Instance: 7, Epoch: 1, Sequence: 50}
	for _, tc := range []struct {
		name string
		p    Position
		want bool
	}{
		{"earlier sequence", Position{Instance: 7, Epoch: 1, Sequence: 49}, true},
		{"same", at, false},
		{"later sequence", Position{Instance: 7, Epoch: 1, Sequence: 51}, false},
		{"earlier epoch, later sequence", Position{Instance: 7, Epoch: 0, Sequence: 900}, true},
		{"later epoch, earlier sequence", Position{Instance: 7, Epoch: 2, Sequence: 1}, false},
		{"other instance, lower numbers", Position{Instance: 6, Epoch: 0, Sequence: 1}, false},
		{"other instance, higher numbers", Position{Instance: 8, Epoch: 9, Sequence: 900}, false},
	} {
		if got := tc.p.Before(at); got != tc.want {
			t.Errorf("%s: %+v.Before(%+v) = %v, want %v", tc.name, tc.p, at, got, tc.want)
		}
		if tc.p.Instance != at.Instance && at.Before(tc.p) {
			t.Errorf("%s: %+v.Before(%+v) = true, want no order across instances", tc.name, at, tc.p)
		}
	}
}
