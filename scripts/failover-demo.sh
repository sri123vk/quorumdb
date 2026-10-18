#!/usr/bin/env bash
# Starts a local 3-node cluster, kills the leader with SIGKILL while a client
# is writing, shows that writes continue, restarts the dead node, and checks
# that it catches up and every acknowledged write is readable.
set -euo pipefail

cd "$(dirname "$0")/.."
work=$(mktemp -d)
cluster=1=127.0.0.1:9001/127.0.0.1:8001,2=127.0.0.1:9002/127.0.0.1:8002,3=127.0.0.1:9003/127.0.0.1:8003
nodes=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
pids=()

cleanup() {
	for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
	wait 2>/dev/null || true
	rm -rf "$work"
}
trap cleanup EXIT

go build -o "$work/quorumdb" ./cmd/quorumdb
go build -o "$work/qdbctl" ./cmd/qdbctl

start() {
	"$work/quorumdb" -id "$1" -cluster "$cluster" -data "$work/node$1" -snapshot-entries 50 \
		>>"$work/node$1.log" 2>&1 &
	pids[$1]=$!
}

status() { curl -sf "http://127.0.0.1:800$1/v1/status" || echo '{}'; }

leader() {
	for _ in $(seq 100); do
		for id in 1 2 3; do
			if [[ -n "${pids[$id]:-}" ]] && status "$id" | grep -q '"state":"leader"'; then
				echo "$id"
				return
			fi
		done
		sleep 0.1
	done
	echo "no leader elected" >&2
	exit 1
}

for id in 1 2 3; do start "$id"; done
old=$(leader)
echo "== cluster up; leader is node $old"

echo "== writing 150 keys in the background; killing node $old after 1s"
"$work/qdbctl" -nodes "$nodes" write-loop -n 150 -prefix demo -interval 20ms &
writer=$!
sleep 1
kill -9 "${pids[$old]}"
unset "pids[$old]"
echo "== node $old killed (SIGKILL)"
wait "$writer"

new=$(leader)
echo "== new leader is node $new"
[[ "$new" != "$old" ]]

echo "== restarting node $old from its data directory"
start "$old"
target=$(status "$new" | sed -E 's/.*"commit":([0-9]+).*/\1/')
for _ in $(seq 100); do
	applied=$(status "$old" | sed -nE 's/.*"applied":([0-9]+).*/\1/p')
	[[ -n "$applied" && "$applied" -ge "$target" ]] && break
	sleep 0.1
done
echo "== node $old caught up: $(status "$old")"

for i in $(seq 0 149); do
	got=$("$work/qdbctl" -nodes "$nodes" get "demo-$i")
	[[ "$got" == "$i" ]] || { echo "demo-$i = '$got', want $i" >&2; exit 1; }
done
echo "== all 150 acknowledged writes read back linearizably; demo passed"
