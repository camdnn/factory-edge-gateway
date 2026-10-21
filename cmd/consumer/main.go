// Command consumer runs on the laptop: it reads machine observations and gap
// events from Kafka on the Pi (over Tailscale), stores them in SQLite and serves
// a live status board. Offsets are committed only after the SQLite write.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

//go:embed index.html
var indexHTML []byte

const (
	retention     = 7 * 24 * time.Hour // observations older than this are deleted
	statusEvery   = 2 * time.Second    // how often machine status changes are checked
	pruneEvery    = time.Hour
	eventsOnBoard = 10
)

// pipelineState is what only the consume loop can see, shared with the status board.
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

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers, e.g. pi.tailnet.ts.net:9092")
	topic := flag.String("topic", "mtconnect.observations", "Kafka topic")
	gapsTopic := flag.String("gaps-topic", "pipeline.gaps", "Kafka topic for gap events (see docs/adr/0001)")
	group := flag.String("group", "laptop", "Kafka consumer group")
	dbPath := flag.String("db", "observations.db", "SQLite database file")
	addr := flag.String("addr", "127.0.0.1:8090", "status board address (localhost only by default)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := OpenStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	newClient := func() *kgo.Client {
		kc, err := kgo.NewClient(
			kgo.SeedBrokers(strings.Split(*brokers, ",")...),
			kgo.ConsumerGroup(*group),
			kgo.ConsumeTopics(*topic, *gapsTopic),
			kgo.DisableAutoCommit(),
			// if Kafka loses data, re-read everything it still has; the store ignores duplicates
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			log.Fatal(err)
		}
		return kc
	}
	kc := newClient()
	defer func() { kc.Close() }()

	var state pipelineState
	state.kafkaErr.Store("")
	go serve(*addr, store, &state)
	// franz-go retries an unreachable broker silently, so ask Kafka directly when nothing arrives
	pinger, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	if err != nil {
		log.Fatal(err)
	}
	defer pinger.Close()
	go housekeep(ctx, store, &state, pinger)

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
				if r.Topic == *gapsTopic {
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
			if p.Topic == *topic && len(p.Records) > 0 {
				partitionLag[p.Partition] = p.HighWatermark - p.Records[len(p.Records)-1].Offset - 1
			}
		})

		inserted, err := store.Insert(obs)
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

func serve(addr string, store *Store, state *pipelineState) {
	internalError := func(w http.ResponseWriter, err error) {
		log.Printf("api: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}

	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	// everything the board shows, and nothing else
	http.HandleFunc("GET /api/board", func(w http.ResponseWriter, r *http.Request) {
		latest, err := store.Latest()
		if err != nil {
			internalError(w, err)
			return
		}
		lastGap, err := store.LastGap()
		if err != nil {
			internalError(w, err)
			return
		}
		events, err := store.RecentEvents(eventsOnBoard)
		if err != nil {
			internalError(w, err)
			return
		}
		stored, err := store.Count()
		if err != nil {
			internalError(w, err)
			return
		}

		now := time.Now()
		writeJSON(w, map[string]any{
			"pipeline": pipelineHealth(state.receivedAt(), state.lag.Load(), agentAvailability(latest), lastGap, state.kafkaErr.Load().(string), now),
			"machines": machineStatuses(latest, now),
			"events":   events,
			"stored":   stored,
		})
	})

	// one machine's full list of data items, loaded only when its details are opened
	http.HandleFunc("GET /api/machines/{name}", func(w http.ResponseWriter, r *http.Request) {
		latest, err := store.Latest()
		if err != nil {
			internalError(w, err)
			return
		}
		items := []mtconnect.Observation{}
		for _, o := range latest {
			if o.Device == r.PathValue("name") {
				items = append(items, o)
			}
		}
		writeJSON(w, items)
	})

	log.Printf("status board on http://%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
