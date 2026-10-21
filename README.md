# Factory Edge Gateway

An edge agent for factory machines, written in Go and built to run on a
Raspberry Pi. It reads machine state over industrial protocols, translates
every machine into one common format, and reports what changes.

> **Status:** in active development. I'm building this from scratch to learn
> how industrial machines connect to software. The roadmap below shows what's
> done and what's next.

## Why

Factories run machines from many vendors, and each one speaks its own
protocol. Software that wants to know "is this machine running, and how many
parts has it made?" has to deal with all of them, often over networks that
drop.

## How it works

```
Real machines (MTConnect agent)      Raspberry Pi 4                       Laptop
┌──────────────────────────┐        ┌──────────────────────────────┐     ┌───────────────────────────┐
│ demo.mtconnect.org       │ HTTPS  │ edge (cmd/edge)              │     │ consumer (cmd/consumer)   │
│ Okuma lathe, Mazak mill  │ ─────> │   reads /sample by sequence  │     │   Kafka consumer group    │
│ live MTConnect 2.8 XML   │        │   produces one record per    │     │   writes to SQLite        │
└──────────────────────────┘        │   observation                │     │   serves a status board   │
                                    │ Kafka 4.1 (single-node KRaft)│ ──> │   on localhost:8090       │
                                    └──────────────────────────────┘     └───────────────────────────┘
                                                        Tailscale (WireGuard), no open ports
```

- **Edge agent** (`cmd/edge`, on the Pi): polls a real MTConnect agent and
  writes every observation to the Kafka topic `mtconnect.observations`, keyed
  by machine so each machine's events stay in order.
- **Kafka** (on the Pi): buffers everything for 24 hours, so the laptop can be
  offline and catch up later.
- **Consumer** (`cmd/consumer`, on the laptop): reads over Tailscale, stores
  observations in SQLite, and serves a live status page per machine
  (execution state, spindle speed, part count, alarms).
- **Simulator** (`cmd/simulation`): a fake CNC mill modeled as a state machine
  that publishes MTConnect XML. Useful for offline development.

## Real data

The data comes from the MTConnect Institute's public demo agent, which
streams a real Okuma and Mazak. NIST's Smart Manufacturing Systems Test Bed
used to publish a similar feed, but it is offline. Any MTConnect agent works:
pass `-agent <url>`.

## Not losing data

Every MTConnect agent keeps a ring buffer where each observation has a
sequence number. The edge agent asks for `/sample?from=<next>` and only
advances `next` after Kafka confirms the write (`acks=all`, idempotent
producer). The cursor is saved to disk, so a restart picks up where it left off.

On the laptop, offsets are committed only after the SQLite write, and rows
are keyed by `(instance_id, sequence)`. A crash between the two replays
records that are then ignored. The result is effectively-once delivery.

If the agent restarts, or the edge falls further behind than the agent's
buffer holds, the edge logs a `GAP` line and resumes from a fresh snapshot
rather than silently skipping data.

Check that the stored data has no holes:

```sh
sqlite3 observations.db "SELECT COUNT(*) FROM (SELECT sequence - LAG(sequence) OVER (ORDER BY sequence) d FROM observations) WHERE d > 1"
```

Expect only the gaps from the first startup snapshot, which is a
`/current` snapshot, not a contiguous range.

## Roadmap

- [x] HAL: `Machine` interface and `Status` type
- [x] CNC simulator as a state machine
- [x] Simulator HTTP server publishing MTConnect XML
- [x] MTConnect parser for real agents
- [x] Gap-free polling with MTConnect sequence numbers
- [x] Edge agent producing to Kafka
- [x] Store-and-forward: no events lost when the network drops
- [x] Docker Compose deployment for the Pi
- [ ] Running on a real Raspberry Pi over Tailscale
- [ ] Continuous integration

## Running it

Everything on one machine (Docker required):

```sh
docker compose -f deploy/pi/docker-compose.yml up -d --build   # Kafka + edge
go run ./cmd/consumer                                           # http://localhost:8090
```

On the Pi (Raspberry Pi 4 with 4 GB of RAM, **64-bit** Raspberry Pi OS since
the Kafka image has no 32-bit ARM build, plus Docker and Tailscale):

```sh
sudo tailscale up
cd deploy/pi
PI_TAILNET_NAME=<pi-name>.<tailnet>.ts.net PI_TAILSCALE_IP=$(tailscale ip -4) docker compose up -d --build
```

On the laptop (joined to the same tailnet):

```sh
go run ./cmd/consumer -brokers <pi-name>.<tailnet>.ts.net:9092
```

The simulator on its own:

```sh
go run ./cmd/simulation/
```

## Built with

Go, [franz-go](https://github.com/twmb/franz-go) (Kafka client),
[modernc.org/sqlite](https://modernc.org/sqlite) (pure-Go SQLite), Apache
Kafka, Docker, Tailscale, Raspberry Pi, MTConnect.
