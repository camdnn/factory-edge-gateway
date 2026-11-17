package mtconnect

// Position is where an observation falls in its agent's numbering: its
// instance, epoch and sequence together, which identify it (ADR 0005).
type Position struct {
	Instance uint64 `json:"instance,omitempty"`
	Epoch    uint64 `json:"epoch,omitempty"`
	Sequence uint64 `json:"sequence,omitempty"`
}

// Before reports whether p comes before q. Positions in one instance are
// ordered by epoch, then sequence; Positions in different instances have no
// order, so neither comes before the other.
func (p Position) Before(q Position) bool {
	return p.Instance == q.Instance && (p.Epoch < q.Epoch || (p.Epoch == q.Epoch && p.Sequence < q.Sequence))
}

// Position is where o falls in its agent's numbering.
func (o Observation) Position() Position {
	return Position{Instance: o.InstanceID, Epoch: o.Epoch, Sequence: o.Sequence}
}

// Start is the Position of the Gap's first missed observation.
func (g Gap) Start() Position {
	return Position{Instance: g.InstanceID, Epoch: g.Epoch, Sequence: g.From}
}
