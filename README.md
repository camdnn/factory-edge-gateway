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
CNC machine (simulated)          Edge agent
┌──────────────────────┐         ┌──────────────────────────────┐
│ state machine        │  HTTP   │ MTConnect adapter            │
│ idle, running, alarm │ ──────> │   translates XML -> Status   │
│ publishes MTConnect  │         │ poller                       │
└──────────────────────┘         │   checks every second,       │
                                 │   reports only changes       │
                                 └──────────────────────────────┘
```

- **Simulator:** a fake CNC mill modeled as a state machine. It publishes its
  status as XML using [MTConnect](https://www.mtconnect.org/), an open
  standard many real CNC machines support.
- **Adapter:** fetches that XML and converts it into the shared `Status` type.
- **Poller:** checks each machine on a timer and reports only meaningful
  changes, like a finished part or a new alarm.

## Roadmap

- HAL: `Machine` interface and `Status` type
- CNC simulator as a state machine
- Simulator HTTP server publishing MTConnect XML
- MTConnect adapter
- Poller that reports changes
- Agent program wiring it together
- Tests and continuous integration
- Running on a Raspberry Pi
- Store-and-forward: no events lost when the network drops

## Running it

go run ./cmd/simulation/
open localhost link & refresh the page to see machine status snapshots

## Built with

Go (standard library), Raspberry Pi, MTConnect.
