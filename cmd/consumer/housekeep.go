package main

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	retention   = 7 * 24 * time.Hour // observations older than this are deleted
	statusEvery = 2 * time.Second    // how often machine status changes are checked
	pruneEvery  = time.Hour
	cycleReads  = 10000 // observations worked through per tick when finding Cycles
)

// housekeep finds new Cycles, records machine status changes (including machines
// going quiet, which no incoming record would reveal) and deletes observations past
// retention. continuous names the machines with continuous cycle timing.
func housekeep(ctx context.Context, store *Store, state *pipelineState, pinger *kgo.Client, continuous map[string]bool) {
	statusTick, pruneTick := time.NewTicker(statusEvery), time.NewTicker(pruneEvery)
	defer statusTick.Stop()
	defer pruneTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-statusTick.C:
			// Cycles come only from stored observations, so this goes on while the pipeline is silent
			if err := findNewCycles(store, continuous, cycleReads); err != nil {
				log.Printf("find cycles: %v", err)
			}
			// while the pipeline itself is silent every machine would look Offline;
			// that is a pipeline outage, not machine history
			if !state.flowing(now) {
				pingCtx, cancel := context.WithTimeout(ctx, statusEvery/2)
				err := pinger.Ping(pingCtx)
				cancel()
				msg := ""
				if err != nil {
					msg = err.Error()
				}
				state.kafkaErr.Store(msg)
				continue
			}
			state.kafkaErr.Store("")
			latest, err := store.Latest()
			var receivedAt map[string]time.Time
			if err == nil {
				receivedAt, err = store.ReceivedAt()
			}
			if err == nil {
				// only the statuses are recorded, so no "since" history is needed
				err = store.RecordStatuses(machineStatuses(latest, nil, receivedAt, now), now)
			}
			if err != nil {
				log.Printf("record statuses: %v", err)
			}
		case now := <-pruneTick.C:
			n, err := store.Prune(now.Add(-retention))
			if err != nil {
				log.Printf("prune: %v", err)
			} else if n > 0 {
				log.Printf("pruned %d observations older than %s", n, retention)
			}
		}
	}
}

// findNewCycles finds Cycles in up to limit stored observations past each
// machine's progress and stores them with the progress, in one go.
func findNewCycles(store *Store, continuous map[string]bool, limit int) error {
	progress, err := store.CycleProgress()
	if err != nil {
		return err
	}
	latest, err := store.Latest()
	if err != nil {
		return err
	}
	machines := map[string]bool{}
	for _, o := range latest {
		if o.Device != agentDevice {
			machines[o.Device] = true
		}
	}
	// start from the machine furthest behind; one never worked on starts from the beginning
	from := int64(math.MaxInt64)
	for m := range machines {
		from = min(from, progress[m].After)
	}
	arrivals, err := store.CycleObservations(from, limit)
	if err != nil || len(arrivals) == 0 {
		return err
	}
	gaps, err := store.Gaps()
	if err != nil {
		return err
	}

	byMachine := map[string][]mtconnect.Observation{}
	for _, a := range arrivals {
		if machines[a.Device] && a.Row > progress[a.Device].After {
			byMachine[a.Device] = append(byMachine[a.Device], a.Observation)
		}
	}
	end := arrivals[len(arrivals)-1].Row
	var cycles []Cycle
	saved := map[string]CycleProgress{}
	for m := range machines {
		if progress[m].After >= end {
			continue
		}
		found, p := findCycles(byMachine[m], gaps, progress[m], continuous[m])
		p.After = end
		cycles = append(cycles, found...)
		saved[m] = p
	}
	return store.SaveCycles(cycles, saved)
}
