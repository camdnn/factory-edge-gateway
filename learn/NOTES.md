# Notes

- Workspace lives in `learn/` at the repo root, not in `cmd/simulation` where `/teach` was first run, so lessons stay out of the Go packages.
- Camden asked for "all the inner workings" up front. The reference doc `reference/system-map.html` covers the whole system in one place; lessons then drill into one idea each.
- Prefers plain explanations; was new to SSH, Tailscale and Pi setup during the build.
- Planned lesson sequence (adjust to learning records):
  1. The data's journey (big picture)
  2. MTConnect: agents, /current vs /sample, sequence numbers and the ring buffer
  3. Kafka: topics, partitions, keys, offsets, consumer groups
  4. Not losing data: commit-after-write, idempotent producer, dedupe by (instance_id, sequence)
  5. Tailscale and advertised listeners
  6. Incident stories: storage wipe, recreated topic ID, power loss and GAP
