# A Gap starts a new batch

A Cycle that spans a Gap is timed across parts the edge never saw (on 2026-11-11, two such Cycles took 7.9 h and 1.9 h against normal times of 10m56s and 3m25s), so a Gap starts a new batch, like a part count reset: the first part after it has no Cycle. Unlike a reset, it does this on machines with continuous cycle timing too, and for every machine of the agent instance the Gap belongs to. A restart the edge caught at once still counts, since parts may have been made while the agent was down. A Gap is placed among a machine's observations by the agent's numbering, not by arrival: it lies between two observations when the earlier one is in the Gap's instance before its first missed sequence and the later one is at or past it, or from another instance. Arrival order won't do, because Gaps reach the consumer on their own Kafka topic (ADR 0001). It costs one Cycle per Gap per machine.

## Considered Options

- **A Cycle spanning a Gap is Interrupted.** Keeps the Cycle out of Baseline learning, but stretches Interrupted (a stop or Fault on the machine) to cover a pipeline problem.
- **Leave it.** Gaps are rare, but one wrong Cycle would later show as Slow for a reason unrelated to production.
