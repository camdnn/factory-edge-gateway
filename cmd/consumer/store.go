package main

import (
	"database/sql"
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
);`

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

// Count is the number of stored observations. Rows are never deleted and
// ignored duplicates don't take a rowid, so MAX(rowid) avoids a full scan.
func (s *Store) Count() (int64, error) {
	var n sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(rowid) FROM observations`).Scan(&n)
	return n.Int64, err
}
