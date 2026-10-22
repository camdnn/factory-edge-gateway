# Status board follow-ups

**Status:** ready-for-agent · from the two-axis review of `392a12e`

## Problem Statement

The status board shipped in `392a12e` answers its two questions, "is a machine alarming?" and "is the pipeline healthy?", but its review left four known limits that make it less trustworthy or harder to maintain:

- An OK machine shows no "since", so you can't tell a machine that has been fine all week from one that recovered from an Alarm a minute ago.
- If the edge loses its saved cursor (for example, its state volume is deleted), it starts again from a fresh snapshot and reports no Gap at all, even if observations were missed. That breaks the promise that a Gap is always reported, never silent (see GLOSSARY.md).
- A machine counts as Offline when its newest observation is more than 5 minutes old by the laptop's clock, while that observation's timestamp comes from the agent's clock. A machine whose agent clock is wrong by a few minutes looks Offline (or never Offline) for no reason.
- The consumer's entry point now does four unrelated jobs: consuming from Kafka, computing Lag, housekeeping (status history and pruning), and serving the HTTP API. It is harder to read and change than the rest of the codebase.

## Solution

- Every machine row shows how long its current Machine status has lasted, including OK ("OK · since 3h").
- When the edge starts with no saved cursor, it recovers its position from the newest observation it already wrote to Kafka. If the MTConnect agent still has the following sequences, collection resumes with nothing missed. If it doesn't, a Gap is reported like any other. Only a truly first run (nothing in Kafka) starts silently.
- Offline is judged entirely by the laptop's clock: a machine is Offline when the consumer has received nothing new from it for 5 minutes. Last observation (the agent-clock time) is still shown, but no longer decides the status.
- The consumer's code is split so each part changes for one reason. Behaviour does not change.

## User Stories

1. As an operator, I want an OK machine to show how long it has been OK, so that I can tell steady machines from ones that just recovered.
2. As an operator, I want a machine that recovered from an Alarm to show "OK · since 1m", so that I notice recent trouble even after it cleared.
3. As an operator, I want a machine that has been OK since the consumer started tracking it to show no "since" rather than a made-up time, so that the board never claims more history than it has.
4. As an operator, I want the "since" for Alarm and Warning to keep describing when the condition behind it began, so that existing behaviour doesn't change.
5. As a pipeline owner, I want the edge to resume from where Kafka says it left off when its cursor file is missing, so that losing a Docker volume doesn't silently lose observations.
6. As a pipeline owner, I want a Gap reported when the recovered position is no longer in the MTConnect agent's buffer, so that every missed stretch is visible on the board.
7. As a pipeline owner, I want a Gap reported with reason "agent restarted" when the recovered position belongs to an older agent instance, so that the reason is accurate.
8. As a pipeline owner, I want a genuine first run (empty Kafka topic, no cursor) to start from a snapshot without a Gap, so that a new install doesn't show a false alarm.
9. As a pipeline owner, I want the edge to log which of these happened at startup (cursor file, recovered from Kafka, or first run), so that I can tell from the logs how it resumed.
10. As an operator, I want a machine to count as Offline only when the laptop has received nothing new from it for 5 minutes, so that a wrong machine clock can't make a healthy machine look Offline.
11. As an operator, I want a machine whose agent clock runs hours behind but whose observations keep arriving to stay OK, so that clock problems don't hide real status.
12. As an operator, I want Last observation to keep showing the agent's timestamp, so that I can still spot a machine with a wrong clock.
13. As an operator, I want the board to behave exactly as before during a pipeline outage (no Offline flood in history), so that the earlier fix still holds.
14. As a developer, I want consuming, housekeeping and the HTTP API in separate parts of the consumer, so that I can change one without reading the others.
15. As a developer learning this codebase, I want each part of the consumer to have one clear job, so that I can study the backend one piece at a time.
16. As a reviewer at Hadrian, I want gaps to be reported even after storage loss, so that the "never silent" claim in the README is true.

## Implementation Decisions

- **OK since from status history.** The consumer already keeps a status-change history (each change of a machine's Machine status, kept forever). The board's machine view takes, for each machine, the time of its most recent recorded change. When the machine's current status is OK and a recorded change exists, that time is its "since". When no change was ever recorded, OK has no "since". Alarm, Warning and Offline keep their current "since" (the condition or silence that caused them).
- The machine-status computation stays a pure function of its inputs. It gains the per-machine "last status change" times as an input; it does not query storage itself. The store gains a read of the latest change time per machine.
- **Cursor recovery from Kafka.** At startup, if the cursor file is missing, the edge reads the newest observation record from each partition of the observations topic. It takes the one with the highest sequence for the newest agent instance and rebuilds the cursor as that instance plus (that sequence + 1). The existing gap logic in the MTConnect client then decides the outcome exactly as it does for a cursor loaded from file: resume, "fell behind the agent's buffer", or "agent restarted". No new gap rules.
- Recovery reads only the newest record per partition (Kafka offset = high watermark − 1). It never scans the topic. An empty topic means first run.
- The cursor file remains the primary source; Kafka recovery is the fallback when it's missing. Recovery failure (Kafka unreachable) is retried, not treated as a first run, because a wrong first run is exactly the silent loss being fixed.
- **Offline by receive time.** The store records, per machine, the laptop-clock time at which its newest observation was stored. A machine is Offline when that time is more than 5 minutes ago, or the machine reports UNAVAILABLE (machine-level Availability only, as today). The Alarm > Warning > Offline > OK ordering is unchanged.
- Schema change: the per-machine "latest" data gains a received-at time, written by the consumer when it stores observations. Existing rows have none until their next observation arrives; a machine with no received-at time is judged by its next observation, not assumed Offline.
- **Split the consumer** into: the consume loop (Kafka, Lag, pipeline state), housekeeping (status recording, pruning, Kafka ping), and the HTTP API. Pure refactor: same flags, same endpoints, same JSON.
- Vocabulary stays as in GLOSSARY.md. Update the Last observation entry only if its meaning changes (it doesn't: it remains the agent-clock time).

## Testing Decisions

- Good tests check behaviour through a module's public interface with literal expected values (worked examples), never internal state or the database directly. The existing tests are the model: machine status tests build observations and assert the resulting rows; store tests write then read through the store's own methods.
- **Seams (all already exist, no new ones):**
  - Machine status function: OK since from a last-change time; no since without history; Offline from receive time, not agent time (a machine with an agent clock 3h behind and a recent receive stays OK; a machine silent for 6 minutes by receive time goes Offline).
  - Store: the latest change time per machine; received-at is recorded when observations are stored.
  - MTConnect client (fake agent server): already covers gap outcomes for any cursor. Recovery feeds it a cursor, so its tests cover the decisions. Add a case only if recovery exposes an untested cursor shape.
- Cursor recovery wiring (reading Kafka's newest record) is verified end to end in local Docker, not unit-tested, matching how the edge loop is verified today:
  - delete the edge state volume with the stack running, restart edge, and expect "recovered from Kafka" and no Gap;
  - repeat with the cursor rewound past the agent's buffer, and expect a Gap;
  - start with an empty topic, and expect a silent first run.
- The consumer split is covered by the existing tests plus one end-to-end run of the board (pipeline verdict, machines, events load as before).

## Out of Scope

- Acknowledging alarms, notifications, sounds.
- A Stale-alarm rule (a Fault on a silent machine stays Alarm, as agreed: worst wins).
- Changing the 5-minute, 30-second, 1,000 or 1-hour thresholds.
- Making stored-count survive a full prune (the consumer down for 7+ days).
- Splitting the edge program, or sharing flag definitions between edge and consumer.

## Further Notes

- These come from the two-axis review (Standards + Spec) of `392a12e`.
- Related decision: docs/adr/0001-edge-reports-gaps.md (gaps travel on their own topic). Cursor recovery keeps that: a recovered cursor produces gap events through the same path.
- Prototype for the board layout: branch `prototype/status-board`.
