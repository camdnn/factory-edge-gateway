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

**Condition**:
An observation about a machine's health, reported at a level: Normal, Warning, Fault or Unavailable.
_Avoid_: Alert, error

**Run state**:
What a machine is doing right now, such as ACTIVE, READY, STOPPED or PROGRAM_COMPLETED. It says nothing about health.
_Avoid_: Status (that is the health verdict), execution

**Machine status**:
The single health verdict for a machine, the worst that applies: Alarm (a Fault condition or emergency stop triggered), Warning (a Warning condition), Offline (the machine reports unavailable or has sent nothing new for 5 minutes), otherwise OK.
_Avoid_: Health, state

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

**Last observation**:
How long ago a machine's newest observation was made, measured by that machine's agent clock.
_Avoid_: Last seen, last update
