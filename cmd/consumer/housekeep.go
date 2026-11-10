package main

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	retention   = 7 * 24 * time.Hour // observations older than this are deleted
	statusEvery = 2 * time.Second    // how often machine status changes are checked
	pruneEvery  = time.Hour
)

// housekeep records machine status changes (including machines going quiet, which
// no incoming record would reveal) and deletes observations past retention.
func housekeep(ctx context.Context, store *Store, state *pipelineState, pinger *kgo.Client) {
	statusTick, pruneTick := time.NewTicker(statusEvery), time.NewTicker(pruneEvery)
	defer statusTick.Stop()
	defer pruneTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-statusTick.C:
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
			if err == nil {
				err = store.RecordStatuses(machineStatuses(latest, now), now)
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
