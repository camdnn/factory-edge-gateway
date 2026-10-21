// Command edge runs on the Raspberry Pi: it reads a real MTConnect agent and
// writes every observation to Kafka, advancing its cursor only after Kafka acks.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

func main() {
	agent := flag.String("agent", "https://demo.mtconnect.org", "MTConnect agent base URL")
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "mtconnect.observations", "Kafka topic")
	interval := flag.Duration("interval", time.Second, "poll interval once caught up")
	statePath := flag.String("state", "edge-cursor.json", "file that remembers the agent cursor across restarts")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// franz-go defaults to acks=all with an idempotent producer
	kc, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()

	client := &mtconnect.Client{BaseURL: *agent, HTTP: &http.Client{Timeout: 10 * time.Second}, Count: 1000}

	cur, err := loadCursor(*statePath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("edge: %s -> %s/%s, resuming at %+v", *agent, *brokers, *topic, cur)

	for ctx.Err() == nil {
		next, obs, gap, err := client.Next(ctx, cur)
		if err != nil {
			log.Printf("read agent: %v", err)
			sleep(ctx, *interval)
			continue
		}
		if gap {
			log.Printf("GAP: agent no longer has sequences from %d (restart or buffer overrun); resuming from snapshot at %d", cur.Next, next.Next)
		}

		records := make([]*kgo.Record, 0, len(obs))
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

func loadCursor(path string) (mtconnect.Cursor, error) {
	var cur mtconnect.Cursor
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return cur, nil
	}
	if err != nil {
		return cur, err
	}
	return cur, json.Unmarshal(b, &cur)
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
