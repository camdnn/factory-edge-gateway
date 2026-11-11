# Factory Edge Gateway

Moving live machine data from shop-floor machines to a place where people can watch it, without losing any of it on the way.

## Machines and their data

**Machine**:
A piece of shop-floor equipment (e.g. the Okuma lathe or the Mazak mill) that reports its state through an MTConnect agent.
_Avoid_: Device (MTConnect also calls the agent itself a device), asset

**MTConnect agent**:
The server that collects observations from machines, numbers them, and keeps the most recent ones for clients to fetch. It is the source of the pipeline, not a machine.
_Avoid_: Source server, adapter (an adapter feeds an agent; it is a different thing)

**Observation**:
One data item's value on one machine at one moment, stamped with the agent's sequence number.
_Avoid_: Reading, data point, sample (an MTConnect sample is one category of observation)

**Program**:
The list of instructions a machine follows to make one kind of part. A machine may run several programs in turn, each with its own normal running time.
_Avoid_: Job, recipe, G-code (the language a program is written in)

**Condition**:
An observation about a machine's health, reported at a level: Normal, Warning, Fault or Unavailable.
_Avoid_: Alert, error

**Run state**:
What a machine is doing right now, such as ACTIVE, READY, STOPPED or PROGRAM_COMPLETED. It says nothing about health.
_Avoid_: Status (that is the health verdict), execution

**Machine status**:
The single health verdict for a machine, the worst that applies: Alarm (a Fault condition or emergency stop triggered), Warning (a Warning condition), Offline (the machine reports unavailable, or the laptop has received nothing new from it for 5 minutes by the laptop's own clock), otherwise OK.
_Avoid_: Health, state

**Status change**:
A machine's Machine status changing, as recorded by the consumer and kept forever. Housekeeping records changes every 2 seconds and not at all while the pipeline is silent, so the newest recorded change can lag the board: a machine that just cleared an Alarm shows OK while its newest change is still the change to Alarm. A machine with no recorded change has never been recorded as anything but OK.
_Avoid_: Status history entry, transition

## Production

How well machines are making parts, judged one part at a time.

**Cycle**:
The making of one part, from one genuine part count increase to the next, belonging to the Program running when the part finished. A part count reset starts a new batch, and the first part after it has no Cycle, except on machines that reset at every Program change, where timing carries on across the reset. An increase too soon after the previous one to be a real part is not a Cycle.
_Avoid_: Run, execution time (the time from ACTIVE to finish, a different measure)

**Cycle health**:
How well one Cycle went, judged by its time against its Program's Baseline: Normal, Slow, Fast or Interrupted (a stop or Fault happened during the Cycle), or Learning while the Program has no Baseline yet. It judges production, not the machine's condition; that is Machine status.
_Avoid_: Cycle status, performance

**Baseline**:
A Program's normal Cycle time: the median of the first 10 Cycles after it was started, which are assumed healthy. Once learned it is fixed; Cycle results never change it, and it is only learned again when someone deliberately resets it.
_Avoid_: Target, expected time, average

**Drifting**:
A mark on a Program, not on a Cycle, saying its recent Cycles have been running well above its Baseline and getting longer: the usual sign of tool wear. It stays until Cycles return to normal, so a Program can be Drifting while its latest Cycle is Normal or Slow.
_Avoid_: Trend, degradation

**Scenario**:
A kind of trouble the simulator acts out on purpose (normal production, tool wear, feed interruption, or alarm) so you can watch the pipeline and the board react.
_Avoid_: Mode, test case

## The pipeline

**Pipeline**:
The whole path an observation travels, from the MTConnect agent through the edge, Kafka and the consumer to the status board.

**Gap**:
A stretch of observations the edge could not collect because the MTConnect agent restarted or had already discarded them. A gap is always reported, never silent.
_Avoid_: Data loss, hole, drop

**Pipeline verdict**:
The pipeline's single health judgement: Healthy, Degraded (observations late or some missed) or Down (nothing arriving).
_Avoid_: Pipeline status (status belongs to machines), pipeline health

**Lag**:
The number of observations held in Kafka that the consumer has not yet stored.
_Avoid_: Delay, backlog, latency

**Last received**:
How long ago the consumer stored anything, measured by the laptop's own clock.

**Received-at**:
When the consumer last stored a new observation from a machine, measured by the laptop's own clock. It decides whether a machine is Offline; a machine stored before received-at was recorded has none until its next observation.
_Avoid_: Last seen

**Last observation**:
How long ago a machine's newest observation was made, measured by that machine's agent clock.
_Avoid_: Last seen, last update
