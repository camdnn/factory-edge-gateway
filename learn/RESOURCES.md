# Factory Edge Gateway Resources

## Knowledge

- [Apache Kafka: Introduction](https://kafka.apache.org/intro)
  The official one-page overview of events, producers, consumers, topics, partitions and retention. Use for: anything Kafka-shaped in lesson 1 onward. Primary source for lesson 0001.
- [Apache Kafka docs: Consumer position](https://kafka.apache.org/documentation/#design_consumerposition)
  How Kafka tracks where each consumer is (offsets) and why that makes catching up cheap. Use for: offsets, commits, replaying after an outage.
- [MTConnect C++ Agent (reference implementation)](https://github.com/mtconnect/cppagent)
  README for the agent software that real machines and demo.mtconnect.org run. Covers agents vs adapters, /probe, /current and /sample, and the 131,072-slot ring buffer. Use for: where the data comes from.
- [MTConnect standard documents](https://docs.mtconnect.org/)
  The full spec PDFs (Part 1 Overview, Part 5 Interfaces). Use for: exact definitions of sequence numbers and the `from` parameter, only when the README isn't enough.
- [Tailscale: How Tailscale works](https://tailscale.com/blog/how-tailscale-works)
  The founders' explanation of the WireGuard mesh, coordination server and NAT traversal. Use for: why the Mac can reach the Pi without opening router ports.
- [franz-go (Go Kafka client)](https://github.com/twmb/franz-go)
  The client library both programs use. Its doc comments explain idempotent producers, consumer groups and data-loss resets. Use for: Go-level details.
- [Confluent: Exactly-once semantics in Kafka](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
  Clear explanation of at-least-once, at-most-once and exactly-once delivery. Use for: the "effectively-once" design (commit after write plus dedupe).

## Wisdom (Communities)

- [r/apachekafka](https://www.reddit.com/r/apachekafka/)
  Active, practitioner-heavy Kafka community. Use for: sanity-checking design decisions such as running a single broker at the edge.
- [r/PLC](https://www.reddit.com/r/PLC/)
  Large industrial automation community (controls engineers and integrators). Use for: how shop floors really connect machines, and whether MTConnect is what people see in the field.

## Gaps
- No good free tutorial for MTConnect sequence numbers and `/sample?from=`. Lessons rely on the spec PDFs plus what we observed from the live agent.
- The NIST Smart Manufacturing Systems test bed (smstestbed.nist.gov) was offline when checked on 2026-10-20.

## Go standard library (added for lesson 0003)

- [Go encoding/xml: Unmarshal](https://pkg.go.dev/encoding/xml#Unmarshal)
  The official rules for struct tags: `,attr`, `,chardata`, `,any`, `a>b` paths and `XMLName`. Use for: how `streams.go` reads the agent's XML. Primary source for lesson 0003.
- [Go encoding/json: Marshal](https://pkg.go.dev/encoding/json#Marshal)
  How `json:"name,omitempty"` tags shape the JSON written to Kafka. Use for: the record format on the topic.
