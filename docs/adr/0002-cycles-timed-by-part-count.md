# Cycles are timed between part count increases

A Cycle runs from one genuine part count increase to the next, not from Run state ACTIVE to PROGRAM_COMPLETED, because a part count increase is what manufacturing means by "a part was made", and ACTIVE → finish is a separate execution-time measure that breaks when a machine stays ACTIVE across parts. Raw increases can't be trusted as-is: on the demo Okuma the counter ticks up about 2 seconds after a real part at some Program changes, so an increase sooner than max(5 s, 25% of the Program's Baseline) after the previous one is not a Cycle. A part count reset normally starts a new batch whose first part has no Cycle, but the Okuma makes one part per Program and resets at every Program change, so under that rule three of its four Programs would never get a Cycle. Rather than detect this automatically, the consumer takes `-continuous-cycle-timing=<machines>`: for those machines timing carries on across resets, accepting a few seconds of changeover in each Cycle. Each Program's Baseline is the median of the first 10 non-Interrupted Cycles, frozen until someone explicitly resets it, so tool wear can never drag "normal" upward.

## Considered Options

- **ACTIVE → PROGRAM_COMPLETED.** Clean on the demo feed, but not what a Cycle means on the shop floor.
- **Strict reset rule for every machine.** Faithful to the definition, but leaves the Okuma with about 60 judgeable Cycles in four weeks, all from one Program.
- **A rolling median of recent Normal Cycles.** Adapts to real process changes, but early worn Cycles still fall within ±15% and quietly raise the Baseline.
