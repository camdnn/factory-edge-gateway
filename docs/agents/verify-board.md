# Verifying the pipeline end to end

Unit tests cover the pure parts (machine status, Pipeline verdict, cursor choice, gap rules). The wiring (Kafka reads, HTTP, Docker volumes) is verified against the local stack with the steps below.

## Start the local stack

```sh
docker compose -f deploy/pi/docker-compose.yml up -d --build   # Kafka + edge, reading demo.mtconnect.org
```

`--build` rebuilds the edge from the working tree, so restart it this way after changing `cmd/edge`. The compose project is `pi`, which names the containers `pi-kafka-1` and `pi-edge-1` and the edge's volume `pi_edge-state`.

## Consumer and status board changes

Run `scripts/board-diff.sh [base-ref]`. Its header says what it compares and what its exit codes mean.

- **Refactor**: expect `boards match`.
- **Intended board change**: expect a diff that shows exactly that change and nothing else. For example, giving OK machines a since flips `has_since` and adds `machines.[].since` to `shape`.

## Edge cursor recovery

> **Unverified:** these cases follow the spec and `cmd/edge`, but nobody has run them from this doc yet. Fix this doc if a case behaves differently.

The edge logs how it started on its `edge: ... resuming at` line: `from cursor file ...`, `(recovered from Kafka: no cursor file)` or `(first run: no cursor file and <topic> is empty)`. A Gap logs a `GAP: <reason>; ...` line.

A restarted container keeps its old logs, so read only the newest start:

```sh
docker logs --since "$started" pi-edge-1 2>&1 | grep -E "edge:|GAP"   # started=$(date -u +%Y-%m-%dT%H:%M:%SZ) just before starting the edge
```

The cursor lives in `pi_edge-state` at `/var/lib/edge/cursor.json` as `{"instance_id":…,"next":…}`. The edge image is distroless, so read and write it through a throwaway container: `docker run --rm -v pi_edge-state:/s alpine cat /s/cursor.json`.

| Case | Setup, with the stack running | Expect |
| --- | --- | --- |
| Lost cursor | `docker compose -f deploy/pi/docker-compose.yml rm -sf edge`, then `docker volume rm pi_edge-state`, then `up -d edge` | `recovered from Kafka`, no `GAP` |
| Cursor past the agent's buffer | Read `firstSequence` from the agent's `/current` header. Stop the edge, rewrite `cursor.json` with the same `instance_id` and `next` below `firstSequence` (`docker run --rm -v pi_edge-state:/s alpine sh -c 'echo "{\"instance_id\":<id>,\"next\":<n>}" > /s/cursor.json'`), then start the edge | `GAP: fell behind the agent's buffer`, or `GAP: agent restarted` if the agent restarted meanwhile |
| First run | Remove the edge and its volume as in the first case, delete the topic (`docker exec pi-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic mtconnect.observations`), then start the edge | `first run`, no `GAP` |

The last two cases change local data: a Gap event lands on `pipeline.gaps`, and the first-run case erases the topic.
