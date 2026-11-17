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

// BeforeSQL is a SQL condition that a row's Position comes before p, and its
// arguments, for a table with instance_id, epoch and sequence columns. Its
// negation is "at or past p", which includes rows in other instances. It must
// agree with Before.
func (p Position) BeforeSQL() (string, []any) {
	return `(instance_id = ? AND (epoch < ? OR (epoch = ? AND sequence < ?)))`, []any{p.Instance, p.Epoch, p.Epoch, p.Sequence}
}
