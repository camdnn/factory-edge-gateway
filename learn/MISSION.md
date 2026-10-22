# Mission: Understand and defend the factory-edge-gateway pipeline

## Why
Camden is presenting this project to Hadrian (an automated-factory company) as a passion project. He needs to own it: explain how live machine data gets from a real CNC machine to his laptop, why each piece exists, and what went wrong along the way, without leaning on Claude.

## Success looks like
- Draw the full data path from memory (machine → MTConnect agent → edge on the Pi → Kafka → Tailscale → consumer → SQLite → status page) and say what each hop is for
- Explain why no data is lost when the laptop or the network drops, using sequences, offsets and commits
- Tell the story of the real bugs hit while building it (Kafka storage wipe, recreated topic ID, power-loss gap) and how each was found and fixed
- Make a small change to the code and deploy it to the Pi alone

## Constraints
- Learning alongside an active build; lessons should be short and use this repo's real code and logs
- Comfortable with Go basics already (wrote the simulator and machine types)

## Out of scope
- Running Kafka clusters at scale (replication, multi-broker tuning)
- The MTConnect standard beyond what this project uses
