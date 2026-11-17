# A late Gap deletes the Cycle that spans it

A Gap starts a new batch (ADR 0004), but housekeeping can only see a Gap that is already stored. The edge sends a Gap and the observations after it on two Kafka topics (ADR 0001), so the consumer can store the Gap in a later poll than those observations. If housekeeping works through them in between, the Gap is **late** for that machine: the machine's first stored observation past the Gap has already been worked through. Housekeeping never reads those observations again, so it has timed one Cycle across the Gap, or is still timing one from the last part before it. When housekeeping finds a late Gap, it corrects each machine of the Gap's instance that it has passed:

- It deletes that machine's Cycle that spans the Gap: one that starts at or before the machine's last stored observation before the Gap and ends at or after its first stored observation past it.
- If the machine's progress is still timing from a part before the Gap, it starts a new batch, so the next part has no Cycle.

Only one Cycle per machine can be wrong. The Cycles after it are timed from the first part past the Gap, the same as when the Gap arrives on time. So deleting that one Cycle gives the same Cycles as a Gap that arrived on time, with no re-finding. Housekeeping makes the correction, not the consume loop, and saves it together with the Cycles and progress it finds. A Gap stored while housekeeping is part way through a tick is then late on the next tick, and is not lost between the two.

## Considered Options

- **Rebuild the affected machines from before the Gap.** Gives the same result, but re-finds every Cycle from the start of retention to correct one, and needs care to keep Cycles older than retention.
- **Hold housekeeping back** a few seconds behind arrival. Cheap, but the delay only guesses how far apart Kafka delivers the two topics, and the board's Cycles lag by that much.
- **Send Gaps on the observations topic**, in order with each machine's observations. Contradicts ADR 0001, and every machine's partition would need its own copy of each Gap.
- **Leave it**, documented, with a manual rebuild as the fix. One wrong Cycle would show as Slow once Baselines exist.
