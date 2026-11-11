// Command consumer runs on the laptop: it reads machine observations and gap
// events from Kafka on the Pi (over Tailscale), stores them in SQLite and serves
// a live status board. Offsets are committed only after the SQLite write.
//
// main only reads flags and wires together the consume loop, housekeeping and
// the HTTP API, which run side by side.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers, e.g. pi.tailnet.ts.net:9092")
	topic := flag.String("topic", "mtconnect.observations", "Kafka topic")
	gapsTopic := flag.String("gaps-topic", "pipeline.gaps", "Kafka topic for gap events (see docs/adr/0001)")
	group := flag.String("group", "laptop", "Kafka consumer group")
	dbPath := flag.String("db", "observations.db", "SQLite database file")
	continuousTiming := flag.String("continuous-cycle-timing", "", "comma-separated machines whose Cycles are timed across part count resets (see docs/adr/0002)")
	addr := flag.String("addr", "127.0.0.1:8090", "status board address (localhost only by default)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := OpenStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	seeds := strings.Split(*brokers, ",")
	newClient := func() *kgo.Client {
		kc, err := kgo.NewClient(
			kgo.SeedBrokers(seeds...),
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

	var state pipelineState
	state.kafkaErr.Store("")
	go serve(*addr, store, &state)
	// franz-go retries an unreachable broker silently, so ask Kafka directly when nothing arrives
	pinger, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		log.Fatal(err)
	}
	defer pinger.Close()
	continuous := map[string]bool{}
	for _, m := range strings.Split(*continuousTiming, ",") {
		if m = strings.TrimSpace(m); m != "" {
			continuous[m] = true
		}
	}
	go housekeep(ctx, store, &state, pinger, continuous)

	consume(ctx, newClient, *topic, *gapsTopic, store, &state)
}
