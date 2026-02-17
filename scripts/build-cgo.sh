#!/usr/bin/env bash
# Build pikawire with the dbsync dump reader embedded for MULTIPLE pika
# storage-layout families in ONE artifact:
#
#   drivers/v35  <- statically linked against pikiwidb v3.5.x libstorage.a
#   drivers/v40  <- statically linked against pikiwidb v4.0.x libstorage.a
#
# each compiled to a self-contained sidecar executable (pikawire-dumper-<name>)
# that the parent process launches on demand — this keeps two rocksdb worlds
# apart without dlopen's static-TLS limits, and contains storage-layer
# LOG(FATAL) crashes away from the product. The Go side needs no pika
# toolchain at all: it embeds the dumper binaries and sniffs the dump's
# engine-dir naming to pick one. Adding a future pika major = new
# drivers/<name> port + a ref here; the Go binary is unchanged.
#
#   PIKIWIDB_V35=v3.5.6 PIKIWIDB_V40=v4.0.2 bash scripts/build-cgo.sh
#   # first run per version: ~15-25 min dependency builds, cached afterwards
set -euo pipefail
V35_REF=${PIKIWIDB_V35:-v3.5.6}
V40_REF=${PIKIWIDB_V40:-v4.0.2}
CACHE=${PIKIWIDB_CACHE:-$HOME/.cache/pikawire-pikiwidb}
OUT=${OUT:-bin/pikawire-dbsync}
STAMP_V35=unknown STAMP_V40=unknown

cd "$(dirname "$0")/.."
DRIVERS_OUT=internal/dbsync/cgoadapter/drivers
mkdir -p "$DRIVERS_OUT" bin

build_driver() { # $1=ref $2=driver name (v35|v40)
  local ref=$1 drv=$2 root
  root="$CACHE/pikiwidb-$ref"
  PIKIWIDB_ROOT="$root" PIKIWIDB_REF="$ref" bash scripts/prepare-pikiwidb-deps.sh
  local stamp
  stamp=$(git -C "$root" describe --tags --always 2>/dev/null || echo "$ref")
  echo "compiling dumper $drv from pikiwidb $stamp"
  g++ -std=c++17 -O2 -DNDEBUG -s \
    drivers/common/dumper_main.cc "drivers/$drv/driver.cc" \
    -o "$DRIVERS_OUT/pikawire-dumper-$drv" \
    -I"drivers/include" \
    -I"$root/src" -I"$root/src/storage" -I"$root/src/storage/include" \
    -I"$root/src/pstd/include" \
    -I"$root/deps/include" \
    -L"$root/output/src/storage" -L"$root/output/src/pstd" \
    -L"$root/deps/lib" -L"$root/deps/lib64" \
    -Wl,--start-group -lstorage -lpstd -lrocksdb -lz -lzstd -lsnappy -llz4 \
    -lglog -lgflags -lfmt -l:libjemalloc.a -lunwind -lpthread -ldl \
    -Wl,--end-group -lstdc++ -lstdc++fs -lm
  echo "$stamp" > "$DRIVERS_OUT/.$drv.stamp"
}

build_driver "$V35_REF" v35
build_driver "$V40_REF" v40
STAMP_V35=$(cat "$DRIVERS_OUT/.v35.stamp")
STAMP_V40=$(cat "$DRIVERS_OUT/.v40.stamp")

# The Go side embeds the built dumper binaries; it needs no pika headers at all.
CGO_ENABLED=1 go build -tags pikadump -trimpath -buildvcs=false \
  -ldflags "-s -w -X github.com/jk-97/pikawire/internal/dbsync/cgoadapter.pikaSourceRefs=v35:$STAMP_V35,v40:$STAMP_V40" \
  -o "$OUT" ./cmd/pikawire
echo "built $OUT"
echo "  drivers: $(ls -1 "$DRIVERS_OUT"/*.so | tr '\n' ' ') (v35@$STAMP_V35 v40@$STAMP_V40)"
