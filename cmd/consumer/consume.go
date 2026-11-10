package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

// pipelineState is what the consume loop and housekeeping see of the pipeline
// itself, shared with the status board to decide the Pipeline verdict.
type pipelineState struct {
	lag          atomic.Int64 // observations in Kafka not yet stored
	lastReceived atomic.Int64 // unix nanos of the last stored batch, laptop clock
	kafkaErr     atomic.Value // string: why the last Kafka ping failed, "" if it answered
}

// receivedAt is when the last batch was stored, zero if none yet.
func (s *pipelineState) receivedAt() time.Time {
	if n := s.lastReceived.Load(); n > 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

// flowing reports whether observations are arriving, so silence means the machines, not the pipeline.
func (s *pipelineState) flowing(now time.Time) bool {
	t := s.receivedAt()
	return !t.IsZero() && now.Sub(t) <= downAfter
}

// consume reads observations and gap events from Kafka until ctx is done, stores
// them, then commits their offsets, keeping Lag and Last received in state.
// newClient is called again whenever a topic is recreated.
func consume(ctx context.Context, newClient func() *kgo.Client, topic, gapsTopic string, store *Store, state *pipelineState) {
	kc := newClient()
	defer func() { kc.Close() }()

	partitionLag := map[int32]int64{}
	for {
		fetches := kc.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		recreated := false
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, kerr.UnknownTopicID) {
				recreated = true
				return
			}
			log.Printf("fetch %s/%d: %v", topic, partition, err)
		})

		var obs []mtconnect.Observation
		var gaps []mtconnect.Gap
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, r := range p.Records {
				var err error
				if r.Topic == gapsTopic {
					var g mtconnect.Gap
					if err = json.Unmarshal(r.Value, &g); err == nil {
						if err = storable(g.InstanceID, g.From, g.ResumedAt, g.Missed); err == nil {
							gaps = append(gaps, g)
						}
					}
				} else {
					var o mtconnect.Observation
					if err = json.Unmarshal(r.Value, &o); err == nil {
						if err = storable(o.InstanceID, o.Sequence); err == nil {
							obs = append(obs, o)
						}
					}
				}
				if err != nil {
					log.Printf("skip malformed record %s/%d@%d: %v", r.Topic, r.Partition, r.Offset, err)
				}
			}
			// lag: what Kafka holds beyond the last record we just read
			if p.Topic == topic && len(p.Records) > 0 {
				partitionLag[p.Partition] = p.HighWatermark - p.Records[len(p.Records)-1].Offset - 1
			}
		})

		inserted, err := store.Insert(obs, time.Now())
		for _, g := range gaps {
			if err == nil {
				err = store.InsertGap(g)
			}
		}
		if err != nil {
			// exit without committing: on restart Kafka redelivers from the last committed offset
			log.Fatalf("store: %v", err)
		}
		records := fetches.Records()
		if err := kc.CommitRecords(ctx, records...); err != nil {
			log.Printf("commit: %v (records will be redelivered and ignored as duplicates)", err)
		}
		if len(records) > 0 {
			var lag int64
			for _, l := range partitionLag {
				lag += l
			}
			state.lag.Store(lag)
			state.lastReceived.Store(time.Now().UnixNano())
			log.Printf("consumed %d records, %d new, lag %d", len(records), inserted, lag)
		}

		// the topic was deleted and recreated (e.g. Kafka's data was wiped): this client
		// keeps asking for the old topic ID forever, so start over with a fresh one
		if recreated {
			log.Printf("a topic was recreated in Kafka; reconnecting and re-reading from the start")
			kc.Close()
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			kc = newClient()
			clear(partitionLag)
		}
	}
}

// storable rejects numbers SQLite cannot hold. They originate at an external MTConnect
// agent, and one bad record must be skipped, not crash the consumer on every redelivery.
func storable(nums ...uint64) error {
	for _, n := range nums {
		if n > math.MaxInt64 {
			return fmt.Errorf("number %d too large to store", n)
		}
	}
	return nil
}
