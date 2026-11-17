# Backwards numbering starts a new epoch

An MTConnect agent should get a new instance ID whenever it restarts. An agent that breaks this rule can start its sequence numbers again within the same instance. The edge already notices this and reports a Gap. Downstream, though, the consumer stores observations keyed by `(instance_id, sequence)` with `INSERT OR IGNORE`. So every new observation that reuses a key still within retention is silently ignored as a replay. Cycle timing also places the Gap wrong (ADR 0004), because nothing after it is at or past its first missed sequence. To fix both, the edge gives every observation an **epoch** as well as an instance ID. The epoch starts at 0 for each new instance and goes up by 1 each time the edge sees sequences go backwards within that instance.

- Each observation is identified by `(instance_id, epoch, sequence)`. That is its storage key, and the epoch is part of every Kafka observation record.
- The edge reports this case as its own Gap reason, `sequences went backwards`, rather than `fell behind the agent's buffer`.
- A Gap carries the epoch it left from and the epoch it resumed in. Gaps are stored keyed by `(instance_id, epoch, from_sequence)`.
- A Gap lies between two observations when the earlier one is in the Gap's instance, in an earlier epoch or in the Gap's epoch before its first missed sequence, and the later one is in the Gap's epoch at or past that sequence, or in a later epoch or another instance.
- The epoch is part of the edge's saved cursor. The edge can also rebuild it from the newest Kafka records, so it survives an edge restart.
- When the consumer starts on a database stored without epochs, it rebuilds those tables and gives existing rows epoch 0.

The edge cannot tell when numbering went backwards between two of its polls and then climbed back past its cursor. That case stays undetected.

## Considered Options

- **Report it as its own Gap reason only.** This makes the problem visible, but new observations are still ignored as replays.
- **Leave it.** Rely on agents following the standard. Cheap, but it breaks "a gap is always reported, never silent".
- **An epoch counter that never resets.** It would work too, but resetting it for each instance keeps the numbers small and tied to the instance they belong to.
