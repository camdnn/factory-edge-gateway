package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

// tsFormat is fixed width so timestamps compare correctly as text.
const tsFormat = "2006-01-02T15:04:05.000000Z07:00"

const schema = `
CREATE TABLE IF NOT EXISTS observations (
	instance_id  INTEGER NOT NULL,
	epoch        INTEGER NOT NULL, -- ADR 0005
	sequence     INTEGER NOT NULL,
	device       TEXT, device_uuid TEXT, component TEXT, data_item_id TEXT, name TEXT,
	category     TEXT, type TEXT, sub_type TEXT, value TEXT, ts TEXT,
	PRIMARY KEY (instance_id, epoch, sequence) -- unique per observation, so replays are no-ops
);
CREATE TABLE IF NOT EXISTS latest (
	device_uuid  TEXT NOT NULL,
	data_item_id TEXT NOT NULL,
	device TEXT, component TEXT, name TEXT, category TEXT, type TEXT, sub_type TEXT, value TEXT, ts TEXT,
	PRIMARY KEY (device_uuid, data_item_id)
);
CREATE TABLE IF NOT EXISTS gaps (
	instance_id   INTEGER NOT NULL,
	epoch         INTEGER NOT NULL,
	from_sequence INTEGER NOT NULL,
	resumed_at    INTEGER, resumed_epoch INTEGER NOT NULL, missed INTEGER, reason TEXT, detected_at TEXT,
	PRIMARY KEY (instance_id, epoch, from_sequence) -- the edge may re-send the same gap
);
CREATE TABLE IF NOT EXISTS status_changes (
	machine TEXT NOT NULL, status TEXT NOT NULL, reason TEXT, at TEXT NOT NULL
);
-- its own table, not a column of latest: latest skips rows whose agent timestamp went backwards
CREATE TABLE IF NOT EXISTS received (
	machine TEXT PRIMARY KEY, at TEXT NOT NULL -- laptop clock
);
-- Cycles and Cycle progress are kept forever: they outlive the observations they came from
CREATE TABLE IF NOT EXISTS cycles (
	machine TEXT NOT NULL, program TEXT NOT NULL, start TEXT NOT NULL, end TEXT NOT NULL, interrupted INTEGER NOT NULL,
	PRIMARY KEY (machine, end) -- a machine finishes one part at a time, so a Cycle found twice is kept once
);
CREATE TABLE IF NOT EXISTS cycle_progress (
	machine TEXT PRIMARY KEY,
	after   INTEGER NOT NULL, -- observations rowid worked through; rowids only grow, as Count relies on too
	state   TEXT NOT NULL     -- CycleProgress as JSON
);
CREATE INDEX IF NOT EXISTS cycles_program ON cycles (machine, program, end);
CREATE INDEX IF NOT EXISTS status_changes_machine ON status_changes (machine);
CREATE INDEX IF NOT EXISTS status_changes_at ON status_changes (at);
CREATE INDEX IF NOT EXISTS gaps_detected_at ON gaps (detected_at);`

// Store keeps every observation plus the latest value of each data item.
type Store struct{ db *sql.DB }

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// rebuilds copies each table stored before epochs (ADR 0005) into its new
// shape, with epoch 0. Observations keep their rowids: Cycle progress and Count
// rely on arrival order.
var rebuilds = []struct{ table, copy string }{
	{"observations", `INSERT INTO observations (rowid, instance_id, epoch, sequence, device, device_uuid, component, data_item_id, name, category, type, sub_type, value, ts)
		SELECT rowid, instance_id, 0, sequence, device, device_uuid, component, data_item_id, name, category, type, sub_type, value, ts FROM old_observations`},
	{"gaps", `INSERT INTO gaps (instance_id, epoch, from_sequence, resumed_at, resumed_epoch, missed, reason, detected_at)
		SELECT instance_id, 0, from_sequence, resumed_at, 0, missed, reason, detected_at FROM old_gaps`},
}

// migrate creates any missing tables, first rebuilding those stored before epochs:
// SQLite can't change a primary key in place.
func migrate(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var stale []int
	for i, b := range rebuilds {
		var columns, epochs int
		if err := tx.QueryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE name = 'epoch') FROM pragma_table_info(?)`,
			b.table).Scan(&columns, &epochs); err != nil {
			return err
		}
		if columns > 0 && epochs == 0 {
			if _, err := tx.Exec(`ALTER TABLE ` + b.table + ` RENAME TO old_` + b.table); err != nil {
				return err
			}
			stale = append(stale, i)
		}
	}
	if _, err := tx.Exec(schema); err != nil {
		return err
	}
	for _, i := range stale {
		if _, err := tx.Exec(rebuilds[i].copy); err != nil {
			return fmt.Errorf("rebuild %s with epochs: %w", rebuilds[i].table, err)
		}
		if _, err := tx.Exec(`DROP TABLE old_` + rebuilds[i].table); err != nil {
			return err
		}
	}
	if len(stale) > 0 {
		// indexes went with the old tables, and their names kept the new ones from being made
		if _, err := tx.Exec(schema); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Close() error { return s.db.Close() }

// Insert stores obs in one transaction and returns how many were new. receivedAt
// (laptop clock) becomes the received-at time of each machine that sent a new one.
func (s *Store) Insert(obs []mtconnect.Observation, receivedAt time.Time) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var inserted int64
	for _, o := range obs {
		ts := o.Timestamp.UTC().Format(tsFormat)
		res, err := tx.Exec(`INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			o.InstanceID, o.Epoch, o.Sequence, o.Device, o.DeviceUUID, o.Component, o.DataItemID, o.Name,
			o.Category, o.Type, o.SubType, o.Value, ts)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		inserted += n
		if n > 0 {
			if _, err := tx.Exec(`INSERT INTO received VALUES (?,?) ON CONFLICT (machine) DO UPDATE SET at=excluded.at`,
				o.Device, receivedAt.UTC().Format(tsFormat)); err != nil {
				return 0, err
			}
		}

		// compare timestamps, not sequences: sequences restart when the agent does
		if _, err := tx.Exec(`INSERT INTO latest VALUES (?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT (device_uuid, data_item_id) DO UPDATE SET
				device=excluded.device, component=excluded.component, name=excluded.name, category=excluded.category,
				type=excluded.type, sub_type=excluded.sub_type, value=excluded.value, ts=excluded.ts
			WHERE excluded.ts >= latest.ts`,
			o.DeviceUUID, o.DataItemID, o.Device, o.Component, o.Name, o.Category, o.Type, o.SubType, o.Value, ts); err != nil {
			return 0, err
		}
	}
	return inserted, tx.Commit()
}

// Latest returns the current value of every data item seen.
func (s *Store) Latest() ([]mtconnect.Observation, error) {
	rows, err := s.db.Query(`SELECT device, device_uuid, component, data_item_id, name, category, type, sub_type, value, ts
		FROM latest ORDER BY device, component, data_item_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []mtconnect.Observation
	for rows.Next() {
		var o mtconnect.Observation
		var ts string
		if err := rows.Scan(&o.Device, &o.DeviceUUID, &o.Component, &o.DataItemID, &o.Name,
			&o.Category, &o.Type, &o.SubType, &o.Value, &ts); err != nil {
			return nil, err
		}
		if o.Timestamp, err = time.Parse(tsFormat, ts); err != nil {
			return nil, fmt.Errorf("latest %s/%s: %w", o.Device, o.DataItemID, err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// ReceivedAt is when each machine's newest observation was stored, by the laptop's
// clock. A machine with nothing stored since received-at was first recorded is absent.
func (s *Store) ReceivedAt() (map[string]time.Time, error) {
	rows, err := s.db.Query(`SELECT machine, at FROM received`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var machine, ts string
		if err := rows.Scan(&machine, &ts); err != nil {
			return nil, err
		}
		at, err := time.Parse(tsFormat, ts)
		if err != nil {
			// a zero time would read as "no received-at yet" and hide the machine going Offline
			return nil, fmt.Errorf("received-at for %s: %w", machine, err)
		}
		out[machine] = at
	}
	return out, rows.Err()
}

// Count is how many observations have ever been stored. New rows always get
// MAX(rowid)+1, ignored duplicates take none, and Prune only removes the oldest
// rows, so MAX(rowid) counts them without a full scan.
func (s *Store) Count() (int64, error) {
	var n sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(rowid) FROM observations`).Scan(&n)
	return n.Int64, err
}

const pruneBatch = 10000

// Prune deletes observations made before cutoff and returns how many it removed.
// It works in small batches so the consumer's inserts never wait long.
// ponytail: batches are found in arrival (rowid) order with no index on ts, so the last
// batch of each run scans the table once; fine hourly, add an index on ts if it gets slow.
func (s *Store) Prune(cutoff time.Time) (int64, error) {
	ts := cutoff.UTC().Format(tsFormat)
	var total int64
	for {
		res, err := s.db.Exec(`DELETE FROM observations WHERE rowid IN
			(SELECT rowid FROM observations WHERE ts < ? ORDER BY rowid LIMIT ?)`, ts, pruneBatch)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
		if n < pruneBatch {
			return total, nil
		}
	}
}

// InsertGap stores a gap event; a re-sent copy of the same gap is ignored.
func (s *Store) InsertGap(g mtconnect.Gap) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO gaps VALUES (?,?,?,?,?,?,?,?)`,
		g.InstanceID, g.Epoch, g.From, g.ResumedAt, g.ResumedEpoch, g.Missed, string(g.Reason), g.DetectedAt.UTC().Format(tsFormat))
	return err
}

// LastGap is the most recently detected gap, or nil if there has never been one.
func (s *Store) LastGap() (*mtconnect.Gap, error) {
	gaps, err := s.gaps(1)
	if err != nil || len(gaps) == 0 {
		return nil, err
	}
	return &gaps[0].Gap, nil
}

// Gaps returns every Gap ever stored, newest first.
func (s *Store) Gaps() ([]mtconnect.Gap, error) {
	stored, err := s.GapArrivals()
	var out []mtconnect.Gap
	for _, g := range stored {
		out = append(out, g.Gap)
	}
	return out, err
}

// GapArrival is a stored Gap with its place in the order Gaps were stored.
type GapArrival struct {
	Row int64
	mtconnect.Gap
}

// GapArrivals returns every Gap ever stored, newest first, with its place in
// the order Gaps were stored.
func (s *Store) GapArrivals() ([]GapArrival, error) { return s.gaps(-1) } // a negative LIMIT is no limit in SQLite

func (s *Store) gaps(limit int) ([]GapArrival, error) {
	rows, err := s.db.Query(`SELECT rowid, instance_id, epoch, from_sequence, resumed_at, resumed_epoch, missed, reason, detected_at
		FROM gaps ORDER BY detected_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GapArrival
	for rows.Next() {
		var g GapArrival
		var reason, ts string
		if err := rows.Scan(&g.Row, &g.InstanceID, &g.Epoch, &g.From, &g.ResumedAt, &g.ResumedEpoch, &g.Missed, &reason, &ts); err != nil {
			return nil, err
		}
		g.Reason = mtconnect.GapReason(reason)
		if g.DetectedAt, err = time.Parse(tsFormat, ts); err != nil {
			return nil, fmt.Errorf("gap in instance %d: %w", g.InstanceID, err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Late reports whether Gap g is late for machine: housekeeping has worked
// through, up to row upTo, the machine's first stored observation past the Gap
// (ADR 0006). If so, parts holds the agent times of the machine's part count
// increases past the Gap, up to upTo.
func (s *Store) Late(machine string, g mtconnect.Gap, upTo int64) (parts []time.Time, late bool, err error) {
	// before and past the Gap as between places them, by the agent's numbering
	before := `instance_id = ? AND (epoch < ? OR (epoch = ? AND sequence < ?)) AND device = ?`
	beforeArgs := []any{g.InstanceID, g.Epoch, g.Epoch, g.From, machine}
	var row int64
	err = s.db.QueryRow(`SELECT rowid FROM observations WHERE `+before+` ORDER BY epoch DESC, sequence DESC LIMIT 1`, beforeArgs...).Scan(&row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	past := `rowid > ? AND rowid <= ? AND device = ? AND (instance_id != ? OR epoch > ? OR (epoch = ? AND sequence >= ?))`
	pastArgs := []any{row, upTo, machine, g.InstanceID, g.Epoch, g.Epoch, g.From}
	if err = s.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM observations WHERE `+past+`)`, pastArgs...).Scan(&late); err != nil || !late {
		return nil, false, err
	}

	var prev int64
	err = s.db.QueryRow(`SELECT value FROM observations WHERE `+before+` AND type = 'PartCount' AND value GLOB '[0-9]*'
		ORDER BY epoch DESC, sequence DESC LIMIT 1`, beforeArgs...).Scan(&prev)
	counted := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	rows, err := s.db.Query(`SELECT value, ts FROM observations WHERE `+past+` AND type = 'PartCount' ORDER BY rowid`, pastArgs...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var value, ts string
		if err := rows.Scan(&value, &ts); err != nil {
			return nil, false, err
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue // UNAVAILABLE says nothing about parts made
		}
		if counted && n > prev {
			at, err := time.Parse(tsFormat, ts)
			if err != nil {
				return nil, false, fmt.Errorf("part count of %s: %w", machine, err)
			}
			parts = append(parts, at)
		}
		prev, counted = n, true
	}
	return parts, true, rows.Err()
}

// RecordStatuses stores each machine whose status differs from the last one
// recorded for it. A machine nobody has recorded yet counts as OK.
func (s *Store) RecordStatuses(machines []Machine, at time.Time) error {
	for _, m := range machines {
		prev := string(StatusOK)
		err := s.db.QueryRow(`SELECT status FROM status_changes WHERE machine = ? ORDER BY rowid DESC LIMIT 1`, m.Name).Scan(&prev)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if Status(prev) == m.Status {
			continue
		}
		if _, err := s.db.Exec(`INSERT INTO status_changes VALUES (?,?,?,?)`,
			m.Name, string(m.Status), m.Reason, at.UTC().Format(tsFormat)); err != nil {
			return err
		}
	}
	return nil
}

// StatusChange is a machine's status changing, as recorded.
type StatusChange struct {
	Status Status
	At     time.Time
}

// LastStatusChanges is each machine's newest recorded status change, picked by
// rowid as RecordStatuses picks it. A machine with no recorded change is absent.
func (s *Store) LastStatusChanges() (map[string]StatusChange, error) {
	rows, err := s.db.Query(`SELECT machine, status, at FROM status_changes
		WHERE rowid IN (SELECT MAX(rowid) FROM status_changes GROUP BY machine)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]StatusChange{}
	for rows.Next() {
		var machine, status, ts string
		if err := rows.Scan(&machine, &status, &ts); err != nil {
			return nil, err
		}
		at, err := time.Parse(tsFormat, ts)
		if err != nil {
			return nil, fmt.Errorf("status change for %s: %w", machine, err)
		}
		out[machine] = StatusChange{Status: Status(status), At: at}
	}
	return out, rows.Err()
}

// Event is one line of the recent-events list: a machine status change or a gap.
type Event struct {
	At      time.Time      `json:"at"`
	Kind    string         `json:"kind"` // "status" or "gap"
	Machine string         `json:"machine,omitempty"`
	Status  Status         `json:"status,omitempty"`
	Reason  string         `json:"reason,omitempty"`
	Gap     *mtconnect.Gap `json:"gap,omitempty"`
}

// RecentEvents returns status changes and gaps together, newest first.
func (s *Store) RecentEvents(limit int) ([]Event, error) {
	gaps, err := s.gaps(limit)
	if err != nil {
		return nil, err
	}
	events := make([]Event, 0, 2*limit)
	for i := range gaps {
		events = append(events, Event{At: gaps[i].DetectedAt, Kind: "gap", Gap: &gaps[i].Gap})
	}

	rows, err := s.db.Query(`SELECT machine, status, reason, at FROM status_changes ORDER BY at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var e Event
		var status, ts string
		if err := rows.Scan(&e.Machine, &status, &e.Reason, &ts); err != nil {
			return nil, err
		}
		e.Kind, e.Status = "status", Status(status)
		if e.At, err = time.Parse(tsFormat, ts); err != nil {
			return nil, fmt.Errorf("status change for %s: %w", e.Machine, err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	slices.SortStableFunc(events, func(a, b Event) int { return b.At.Compare(a.At) })
	return events[:min(limit, len(events))], nil
}

// Arrival is a stored observation with its place in arrival order.
type Arrival struct {
	Row int64
	mtconnect.Observation
}

// CycleObservations returns up to limit stored observations that finding Cycles
// reads (part counts, Programs, Run states, Conditions and emergency stops), in
// arrival order after row after.
func (s *Store) CycleObservations(after int64, limit int) ([]Arrival, error) {
	rows, err := s.db.Query(`SELECT rowid, instance_id, epoch, sequence, device, device_uuid, component, data_item_id, name, category, type, sub_type, value, ts
		FROM observations
		WHERE rowid > ? AND (type IN ('PartCount', 'Program', 'Execution', 'EmergencyStop') OR category = 'Condition')
		ORDER BY rowid LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Arrival
	for rows.Next() {
		var a Arrival
		var ts string
		if err := rows.Scan(&a.Row, &a.InstanceID, &a.Epoch, &a.Sequence, &a.Device, &a.DeviceUUID, &a.Component, &a.DataItemID, &a.Name,
			&a.Category, &a.Type, &a.SubType, &a.Value, &ts); err != nil {
			return nil, err
		}
		if a.Timestamp, err = time.Parse(tsFormat, ts); err != nil {
			return nil, fmt.Errorf("observation %d: %w", a.Row, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// CycleProgress is how far finding Cycles has got for each machine. A machine
// it has never worked on, or whose progress was cleared, is absent.
func (s *Store) CycleProgress() (map[string]CycleProgress, error) {
	rows, err := s.db.Query(`SELECT machine, after, state FROM cycle_progress`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]CycleProgress{}
	for rows.Next() {
		var machine, state string
		var p CycleProgress
		if err := rows.Scan(&machine, &p.After, &state); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(state), &p); err != nil {
			return nil, fmt.Errorf("cycle progress for %s: %w", machine, err)
		}
		out[machine] = p
	}
	return out, rows.Err()
}

// LateGap is a Gap stored after housekeeping had worked past it on Machine
// (ADR 0006). Parts are the agent times of the machine's part count increases
// past the Gap that housekeeping has worked through.
type LateGap struct {
	Machine string
	Parts   []time.Time
}

// SaveCycles deletes the Cycle spanning each late Gap, then stores new Cycles
// and each machine's progress, all together, so a restart neither loses nor
// repeats a Cycle or a correction. A Cycle already stored is ignored.
func (s *Store) SaveCycles(late []LateGap, cycles []Cycle, progress map[string]CycleProgress) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, g := range late {
		if len(g.Parts) == 0 {
			continue
		}
		args := []any{g.Machine}
		for range 2 {
			for _, at := range g.Parts {
				args = append(args, at.UTC().Format(tsFormat))
			}
		}
		in := strings.Repeat(",?", len(g.Parts))[1:]
		// the spanning Cycle ends at a part past the Gap and starts at one before it
		if _, err := tx.Exec(`DELETE FROM cycles WHERE machine = ? AND end IN (`+in+`) AND start NOT IN (`+in+`)`, args...); err != nil {
			return err
		}
	}
	for _, c := range cycles {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO cycles VALUES (?,?,?,?,?)`, c.Machine, c.Program,
			c.Start.UTC().Format(tsFormat), c.End.UTC().Format(tsFormat), c.Interrupted); err != nil {
			return err
		}
	}
	for machine, p := range progress {
		state, err := json.Marshal(p)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO cycle_progress VALUES (?,?,?)
			ON CONFLICT (machine) DO UPDATE SET after=excluded.after, state=excluded.state`, machine, p.After, string(state)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ProgramKey names one Program on one machine.
type ProgramKey struct{ Machine, Program string }

// RecentCycles returns the n newest Cycles of each machine and Program, newest
// first, and how many non-Interrupted Cycles each has ever had.
func (s *Store) RecentCycles(n int) ([]Cycle, map[ProgramKey]int, error) {
	rows, err := s.db.Query(`SELECT machine, program, start, end, interrupted FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY machine, program ORDER BY end DESC) AS age FROM cycles
		) WHERE age <= ? ORDER BY end DESC, machine`, n)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var recent []Cycle
	for rows.Next() {
		var c Cycle
		var start, end string
		if err := rows.Scan(&c.Machine, &c.Program, &start, &end, &c.Interrupted); err != nil {
			return nil, nil, err
		}
		if c.Start, err = time.Parse(tsFormat, start); err != nil {
			return nil, nil, fmt.Errorf("cycle of %s: %w", c.Machine, err)
		}
		if c.End, err = time.Parse(tsFormat, end); err != nil {
			return nil, nil, fmt.Errorf("cycle of %s: %w", c.Machine, err)
		}
		recent = append(recent, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	rows, err = s.db.Query(`SELECT machine, program, COUNT(*) FROM cycles WHERE NOT interrupted GROUP BY machine, program`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	counts := map[ProgramKey]int{}
	for rows.Next() {
		var k ProgramKey
		var c int
		if err := rows.Scan(&k.Machine, &k.Program, &c); err != nil {
			return nil, nil, err
		}
		counts[k] = c
	}
	return recent, counts, rows.Err()
}
