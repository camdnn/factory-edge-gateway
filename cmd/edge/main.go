// Command edge runs on the Raspberry Pi: it reads a real MTConnect agent and
// writes every observation to Kafka, advancing its cursor only after Kafka acks.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

func main() {
	agent := flag.String("agent", "https://demo.mtconnect.org", "MTConnect agent base URL")
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "mtconnect.observations", "Kafka topic")
	interval := flag.Duration("interval", time.Second, "poll interval once caught up")
	statePath := flag.String("state", "edge-cursor.json", "file that remembers the agent cursor across restarts")
	gapsTopic := flag.String("gaps-topic", "pipeline.gaps", "Kafka topic for gap events (see docs/adr/0001)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// franz-go defaults to acks=all with an idempotent producer
	newClient := func() *kgo.Client {
		kc, err := kgo.NewClient(
			kgo.SeedBrokers(strings.Split(*brokers, ",")...),
			kgo.DefaultProduceTopic(*topic),
			kgo.AllowAutoTopicCreation(),
		)
		if err != nil {
			log.Fatal(err)
		}
		return kc
	}
	kc := newClient()
	defer func() { kc.Close() }()

	client := &mtconnect.Client{BaseURL: *agent, HTTP: &http.Client{Timeout: 10 * time.Second}, Count: 1000}

	cur, found, err := loadCursor(*statePath)
	if err != nil {
		log.Fatal(err)
	}
	start := "from cursor file " + *statePath
	if !found {
		cur, start = recoverFromKafka(ctx, kc, strings.Split(*brokers, ","), *topic, *interval)
	}
	log.Printf("edge: %s -> %s/%s, resuming at %+v %s", *agent, *brokers, *topic, cur, start)

	for ctx.Err() == nil {
		next, obs, gap, err := client.Next(ctx, cur)
		if err != nil {
			log.Printf("read agent: %v", err)
			sleep(ctx, *interval)
			continue
		}
		records := make([]*kgo.Record, 0, len(obs)+1)
		if gap != nil {
			gap.DetectedAt = time.Now().UTC()
			log.Printf("GAP: %s; sequences from %d not collected (%d missed), resuming from snapshot at %d", gap.Reason, gap.From, gap.Missed, gap.ResumedAt)
			// sent in the same batch as the observations, so it is confirmed before the cursor moves
			value, err := json.Marshal(gap)
			if err != nil {
				log.Fatal(err)
			}
			key := fmt.Sprintf("%d-%d", gap.InstanceID, gap.From)
			records = append(records, &kgo.Record{Topic: *gapsTopic, Key: []byte(key), Value: value})
		}
		// in sequence order, so each partition's newest record is its highest sequence
		// (startup recovery relies on this); agents group observations by component
		slices.SortFunc(obs, func(a, b mtconnect.Observation) int { return cmp.Compare(a.Sequence, b.Sequence) })
		for _, o := range obs {
			value, err := json.Marshal(o)
			if err != nil {
				log.Fatal(err)
			}
			// keyed by device so each machine's observations stay ordered in one partition
			records = append(records, &kgo.Record{Key: []byte(o.DeviceUUID), Value: value})
		}
		// blocks while Kafka is down; the cursor stays put, so nothing is skipped
		if err := kc.ProduceSync(ctx, records...).FirstErr(); err != nil {
			log.Printf("produce: %v", err)
			// the topic was deleted and recreated (e.g. Kafka's data was wiped): this client
			// keeps sending to the old topic ID forever, so start over with a fresh one
			if errors.Is(err, kerr.UnknownTopicID) {
				log.Printf("topic %s was recreated in Kafka; reconnecting", *topic)
				kc.Close()
				kc = newClient()
			}
			sleep(ctx, *interval)
			continue
		}

		cur = next
		if err := saveCursor(*statePath, cur); err != nil {
			log.Printf("save cursor: %v", err)
		}
		if len(obs) > 0 {
			log.Printf("produced %d observations, next sequence %d", len(obs), cur.Next)
		}
		// a full page means the agent has more buffered; fetch it right away
		if len(obs) < client.Count {
			sleep(ctx, *interval)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// recoverFromKafka rebuilds a lost cursor from the newest observations already in
// Kafka, so the client's gap rules can decide whether anything was missed, and
// says how the edge started. It retries until Kafka answers: treating an
// unreachable Kafka as a first run would skip observations without reporting a Gap.
func recoverFromKafka(ctx context.Context, kc *kgo.Client, brokers []string, topic string, retry time.Duration) (mtconnect.Cursor, string) {
	for {
		tails, err := newestRecords(ctx, kc, brokers, topic)
		switch {
		case err == nil:
			cur, ok := recoverCursor(tails)
			if !ok {
				return cur, fmt.Sprintf("(first run: no cursor file and %s is empty)", topic)
			}
			return cur, "(recovered from Kafka: no cursor file)"
		case errors.Is(err, errNotObservation), ctx.Err() != nil:
			log.Fatalf("edge: can't recover the cursor from %s: %v", topic, err)
		}
		log.Printf("edge: no cursor file and can't read %s yet, retrying: %v", topic, err)
		sleep(ctx, retry)
	}
}

// loadCursor reads the saved cursor; found is false when there is no cursor file.
func loadCursor(path string) (cur mtconnect.Cursor, found bool, err error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return cur, false, nil
	}
	if err != nil {
		return cur, false, err
	}
	return cur, true, json.Unmarshal(b, &cur)
}

// saveCursor writes atomically so a power cut never leaves a half-written file.
func saveCursor(path string, cur mtconnect.Cursor) error {
	b, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
