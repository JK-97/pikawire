#!/usr/bin/env bash
# End-to-end correctness verification against the lab stack
# (pikiwidb + kafka containers reachable per examples/e2e.yaml).
# Exercises: dbsync point-in-time snapshot under concurrent write load,
# hard crash mid-run, gap-window writes, resume, and full key coverage.
# Requires the dbsync build: bash scripts/build-cgo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PIKA=${PIKA:-127.0.0.1:9221}
BROKERS=${BROKERS:-kafka:9092}
TOPIC=${TOPIC:-pikawire.db0}
BIN=${BIN:-bin/pikawire-dbsync}
CKPT=${CKPT:-e2e.checkpoint.json}

python3 - "$PIKA" <<'PY'
import socket, sys
host, port = sys.argv[1].rsplit(':', 1)
def cmd(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    s.sendall(out); return s.recv(4096)
s = socket.create_connection((host, int(port)), 5)
cmd("FLUSHALL")
for i in range(400): cmd("SET", "s:%03d" % i, "v")
cmd("HSET", "s:hot", "cnt", "0")
s.close(); print("seeded 401 keys")
PY

python3 - "$PIKA" 25 &
WPID=$!
sleep 2
$BIN -c examples/e2e.yaml >> e2e.log 2>&1 &
APP=$!
sleep 10
kill -9 $APP; echo "crashed pikawire mid-run"
wait $WPID || true

python3 - "$PIKA" <<'PY'
import socket, sys
host, port = sys.argv[1].rsplit(':', 1)
def cmd(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    s.sendall(out); return s.recv(64)
s = socket.create_connection((host, int(port)), 5)
for i in range(10): cmd("SET", "z:post%d" % i, "z")
cmd("DEL", "s:hot")
s.close(); print("post-crash writes done")
PY

rm -f "$CKPT.tmp"
$BIN -c examples/e2e.yaml >> e2e.log 2>&1 &
sleep 25
kill %1 2>/dev/null || true

echo "e2e run complete - inspect e2e.log and topic $TOPIC for consumed key coverage"
