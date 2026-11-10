package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

// errNotObservation means a partition's newest record can't be decoded, so the
// cursor can't be recovered; retrying won't fix it.
var errNotObservation = errors.New("newest record is not an observation")

// recoverCursor rebuilds the cursor from the newest observation record of each
// partition. ok is false when there are none: a genuine first run.
//
// The newest agent instance is the one with the highest instance ID (agents set
// it from their start time), never the edge's clock, which can be wrong after
// the Pi reboots. Each partition's newest record is its highest sequence because
// the edge produces every batch in sequence order.
func recoverCursor(tails []mtconnect.Observation) (cur mtconnect.Cursor, ok bool) {
	for _, o := range tails {
		switch {
		case o.InstanceID > cur.InstanceID:
			cur = mtconnect.Cursor{InstanceID: o.InstanceID, Next: o.Sequence + 1}
		case o.InstanceID == cur.InstanceID && o.Sequence+1 > cur.Next:
			cur.Next = o.Sequence + 1
		}
	}
	return cur, len(tails) > 0
}

// newestRecords reads the newest observation of each partition of topic. It
// never scans the topic: it reads only the record at offset high watermark - 1.
// A topic that is empty or doesn't exist yet returns none.
func newestRecords(ctx context.Context, kc *kgo.Client, brokers []string, topic string) ([]mtconnect.Observation, error) {
	admin := kadm.NewClient(kc)
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := ends.Error(); err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return nil, nil // nothing was ever written
		}
		return nil, err
	}
	if err := starts.Error(); err != nil {
		return nil, err
	}

	offsets := map[int32]kgo.Offset{}
	ends.Each(func(end kadm.ListedOffset) {
		if start, ok := starts.Lookup(topic, end.Partition); ok && end.Offset > start.Offset {
			offsets[end.Partition] = kgo.NewOffset().At(end.Offset - 1)
		}
	})
	if len(offsets) == 0 {
		return nil, nil
	}

	// a separate client: the producer must not start consuming
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
	)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	read := map[int32]bool{}
	var tails []mtconnect.Observation
	for len(read) < len(offsets) {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		var decodeErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if read[r.Partition] {
				return
			}
			var o mtconnect.Observation
			if err := json.Unmarshal(r.Value, &o); err != nil {
				decodeErr = fmt.Errorf("%w: partition %d offset %d: %v", errNotObservation, r.Partition, r.Offset, err)
				return
			}
			read[r.Partition] = true
			tails = append(tails, o)
		})
		if decodeErr != nil {
			return nil, decodeErr
		}
	}
	return tails, nil
}
