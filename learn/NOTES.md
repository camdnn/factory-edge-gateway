# Notes

- Workspace lives in `learn/` at the repo root, not in `cmd/simulation` where `/teach` was first run, so lessons stay out of the Go packages.
- Camden asked for "all the inner workings" up front. The reference doc `reference/system-map.html` covers the whole system in one place; lessons then drill into one idea each.
- Prefers plain explanations; was new to SSH, Tailscale and Pi setup during the build.
- Planned lesson sequence (adjust to learning records):
  1. The data's journey (big picture)
  2. MTConnect: agents, /current vs /sample, sequence numbers and the ring buffer  ← lesson 0002 written 2026-10-21
  3. XML in, JSON out: streams.go and the record format (inserted after learning record 0002)
  4. Kafka: topics, partitions, keys, offsets, consumer groups
  5. Not losing data: commit-after-write, idempotent producer, dedupe by (instance_id, sequence)
  6. Tailscale and advertised listeners
  7. Incident stories: storage wipe, recreated topic ID, power loss and GAP
