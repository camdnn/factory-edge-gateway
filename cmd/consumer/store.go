package main

import (
	"database/sql"
	"errors"
	"slices"
	"time"

	_ "modernc.org/sqlite"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

// tsFormat is fixed width so timestamps compare correctly as text.
const tsFormat = "2006-01-02T15:04:05.000000Z07:00"

const schema = `
CREATE TABLE IF NOT EXISTS observations (
	instance_id  INTEGER NOT NULL,
	sequence     INTEGER NOT NULL,
	device       TEXT, device_uuid TEXT, component TEXT, data_item_id TEXT, name TEXT,
	category     TEXT, type TEXT, sub_type TEXT, value TEXT, ts TEXT,
	PRIMARY KEY (instance_id, sequence) -- an agent's sequence numbers are unique, so replays are no-ops
);
CREATE TABLE IF NOT EXISTS latest (
	device_uuid  TEXT NOT NULL,
	data_item_id TEXT NOT NULL,
	device TEXT, component TEXT, name TEXT, category TEXT, type TEXT, sub_type TEXT, value TEXT, ts TEXT,
	PRIMARY KEY (device_uuid, data_item_id)
);
CREATE TABLE IF NOT EXISTS gaps (
	instance_id   INTEGER NOT NULL,
	from_sequence INTEGER NOT NULL,
	resumed_at    INTEGER, missed INTEGER, reason TEXT, detected_at TEXT,
	PRIMARY KEY (instance_id, from_sequence) -- the edge may re-send the same gap
);
CREATE TABLE IF NOT EXISTS status_changes (
	machine TEXT NOT NULL, status TEXT NOT NULL, reason TEXT, at TEXT NOT NULL
);
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
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Insert stores obs in one transaction and returns how many were new.
func (s *Store) Insert(obs []mtconnect.Observation) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var inserted int64
	for _, o := range obs {
		ts := o.Timestamp.UTC().Format(tsFormat)
		res, err := tx.Exec(`INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
			o.InstanceID, o.Sequence, o.Device, o.DeviceUUID, o.Component, o.DataItemID, o.Name,
			o.Category, o.Type, o.SubType, o.Value, ts)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		inserted += n

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
		o.Timestamp, _ = time.Parse(tsFormat, ts)
		out = append(out, o)
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
	_, err := s.db.Exec(`INSERT OR IGNORE INTO gaps VALUES (?,?,?,?,?,?)`,
		g.InstanceID, g.From, g.ResumedAt, g.Missed, string(g.Reason), g.DetectedAt.UTC().Format(tsFormat))
	return err
}

// LastGap is the most recently detected gap, or nil if there has never been one.
func (s *Store) LastGap() (*mtconnect.Gap, error) {
	gaps, err := s.gaps(1)
	if err != nil || len(gaps) == 0 {
		return nil, err
	}
	return &gaps[0], nil
}

func (s *Store) gaps(limit int) ([]mtconnect.Gap, error) {
	rows, err := s.db.Query(`SELECT instance_id, from_sequence, resumed_at, missed, reason, detected_at
		FROM gaps ORDER BY detected_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mtconnect.Gap
	for rows.Next() {
		var g mtconnect.Gap
		var reason, ts string
		if err := rows.Scan(&g.InstanceID, &g.From, &g.ResumedAt, &g.Missed, &reason, &ts); err != nil {
			return nil, err
		}
		g.Reason = mtconnect.GapReason(reason)
		g.DetectedAt, _ = time.Parse(tsFormat, ts)
		out = append(out, g)
	}
	return out, rows.Err()
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
		events = append(events, Event{At: gaps[i].DetectedAt, Kind: "gap", Gap: &gaps[i]})
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
		e.At, _ = time.Parse(tsFormat, ts)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	slices.SortStableFunc(events, func(a, b Event) int { return b.At.Compare(a.At) })
	return events[:min(limit, len(events))], nil
}
