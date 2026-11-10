#!/usr/bin/env bash
# Compares the status board of the consumer at <base-ref> (default HEAD) with the
# consumer in the working tree, both reading the same running Kafka. Each gets its
# own fresh database and consumer group, so both read the topic from the start.
#
# Values that differ between any two runs (Lag, Last received, stored count,
# Last observation, part count, event times) are left out; everything else is
# compared, including the shape of every JSON response and the -h flag output.
#
#   scripts/board-diff.sh [base-ref]
#
# Exits 0 when the boards match, 1 with a diff when they don't, and 2 when it
# can't run. On 1 or 2 it keeps the consumer logs and raw JSON and says where.
# BASE_PORT, NEW_PORT (8091, 8092), BROKERS (localhost:9092) and TIMEOUT (120s)
# override the defaults. Needs jq and the local stack (docs/agents/verify-board.md).
set -euo pipefail
cd "$(dirname "$0")/.."

base_ref=${1:-HEAD}
base_port=${BASE_PORT:-8091}
new_port=${NEW_PORT:-8092}
brokers=${BROKERS:-localhost:9092}
timeout=${TIMEOUT:-120}

git rev-parse -q --verify "$base_ref^{commit}" >/dev/null || { echo "board-diff: $base_ref is not a commit" >&2; exit 2; }
command -v jq >/dev/null || { echo "board-diff: jq is required" >&2; exit 2; }
first_broker=${brokers%%,*}
nc -z "${first_broker%%:*}" "${first_broker##*:}" 2>/dev/null || {
	echo "board-diff: no Kafka at $first_broker; start the local stack (docs/agents/verify-board.md)" >&2
	exit 2
}

work=$(mktemp -d)
pids=()
keep=1 # cleared only when the boards match, so every failure leaves its evidence
cleanup() {
	# the ${a[@]+...} form keeps bash 3.2 (macOS) from failing on an empty array under set -u
	for p in ${pids[@]+"${pids[@]}"}; do kill "$p" 2>/dev/null || true; done
	if [[ $keep == 1 ]]; then echo "board-diff: logs and raw JSON kept in $work" >&2; else rm -rf "$work"; fi
}
trap cleanup EXIT

echo "board-diff: building $base_ref and the working tree"
mkdir "$work/base-src"
git archive "$base_ref" | tar -x -C "$work/base-src"
go build -C "$work/base-src" -o "$work/consumer-base" ./cmd/consumer
go build -o "$work/consumer-new" ./cmd/consumer

run_id="board-diff-$$-$(date +%s)"
for side in base new; do
	port_var=${side}_port
	"$work/consumer-$side" -brokers "$brokers" -db "$work/$side.db" -group "$run_id-$side" \
		-addr "127.0.0.1:${!port_var}" >"$work/$side.log" 2>&1 &
	pids+=($!)
done

# A consumer has caught up once it logs a batch with nothing left in Kafka. This
# matches the "consumed ... lag N" line in cmd/consumer/consume.go.
echo "board-diff: waiting for both consumers to catch up (up to ${timeout}s)"
deadline=$((SECONDS + timeout))
until grep -q "lag 0$" "$work/base.log" && grep -q "lag 0$" "$work/new.log"; do
	for p in "${pids[@]}"; do
		kill -0 "$p" 2>/dev/null || { echo "board-diff: a consumer exited early; see its log" >&2; exit 2; }
	done
	((SECONDS < deadline)) || { echo "board-diff: consumers did not catch up in ${timeout}s" >&2; exit 2; }
	sleep 1
done
sleep 5 # Housekeeping updates machine status every 2s; let it run at least once.

compared='{
  shape: ([paths | map(if type == "number" then "[]" else . end) | join(".")] | unique),
  pipeline: (.pipeline | {verdict, why, agent, last_gap}),
  machines: [.machines[] | {name, status, reason, has_since: (.since != null), run_state, spindle, program}],
  events: [.events[] | del(.at)],
  stored_any: (.stored > 0)
}'
# Writes what one consumer serves to $work/<side>.view, with only the compared values.
snapshot() {
	local side=$1 port=$2 out="$work/$1.view"
	curl -sf "localhost:$port/api/board" | tee "$work/$side.board.json" | jq "$compared" >"$out"
	echo "page checksum: $(curl -sf "localhost:$port/" | cksum)" >>"$out"
	for m in $(jq -r '.machines[].name' "$work/$side.board.json"); do
		curl -sf "localhost:$port/api/machines/$m" |
			jq -c --arg m "$m" '{machine: $m, items: length, keys: ([.[] | keys] | unique)}' >>"$out"
	done
	# -h exits 2, and its first line names the binary, which differs by design.
	{ "$work/consumer-$side" -h 2>&1 || true; } | tail -n +2 >>"$out"
}

# Live values (spindle speed, run state) can change between the two snapshots,
# so only a difference that survives three attempts counts.
for attempt in 1 2 3; do
	snapshot base "$base_port"
	snapshot new "$new_port"
	if cmp -s "$work/base.view" "$work/new.view"; then
		keep=0
		echo "board-diff: boards match"
		exit 0
	fi
	sleep 2
done
diff -u --label "$base_ref" --label "working tree" "$work/base.view" "$work/new.view" || true
exit 1
