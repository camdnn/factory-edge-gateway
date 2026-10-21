// Command consumer runs on the laptop: it reads machine observations from
// Kafka on the Pi (over Tailscale), stores them in SQLite and serves a live
// status board. Offsets are committed only after the SQLite write.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

//go:embed index.html
var indexHTML []byte

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers, e.g. pi.tailnet.ts.net:9092")
	topic := flag.String("topic", "mtconnect.observations", "Kafka topic")
	group := flag.String("group", "laptop", "Kafka consumer group")
	dbPath := flag.String("db", "observations.db", "SQLite database file")
	addr := flag.String("addr", ":8090", "status board address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := OpenStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()

	go serve(*addr, store)

	for {
		fetches := kc.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch %s/%d: %v", topic, partition, err)
		})

		records := fetches.Records()
		obs := make([]mtconnect.Observation, 0, len(records))
		for _, r := range records {
			var o mtconnect.Observation
			if err := json.Unmarshal(r.Value, &o); err != nil {
				log.Printf("skip malformed record %s/%d@%d: %v", r.Topic, r.Partition, r.Offset, err)
				continue
			}
			obs = append(obs, o)
		}

		inserted, err := store.Insert(obs)
		if err != nil {
			// exit without committing: on restart Kafka redelivers from the last committed offset
			log.Fatalf("store: %v", err)
		}
		if err := kc.CommitRecords(ctx, records...); err != nil {
			log.Printf("commit: %v (records will be redelivered and ignored as duplicates)", err)
		}
		if len(records) > 0 {
			log.Printf("consumed %d records, %d new", len(records), inserted)
		}
	}
}

func serve(addr string, store *Store) {
	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("GET /api/latest", func(w http.ResponseWriter, r *http.Request) {
		latest, err := store.Latest()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stored, err := store.Count()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"stored": stored, "observations": latest})
	})
	log.Printf("status board on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
