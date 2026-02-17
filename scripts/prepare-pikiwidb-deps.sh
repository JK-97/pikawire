#!/usr/bin/env bash
# Ensure $PIKIWIDB_ROOT is a pikiwidb checkout whose storage layer is built
# as PIC static archives (libpstd.a + libstorage.a) ready to link into a
# pikawire reader driver .so. Idempotent; ~15-25 min cold per version.
#
#   PIKIWIDB_ROOT=~/.cache/pikawire-pikiwidb/pikiwidb-v4.0.2 \
#   PIKIWIDB_REF=v4.0.2 bash scripts/prepare-pikiwidb-deps.sh
#
# Everything is rebuilt with -fPIC (CMAKE_POSITION_INDEPENDENT_CODE) because
# the archives are consumed by shared libraries; this also injects the flag
# into every ExternalProject (rocksdb/glog/gflags/fmt/...) configure line.
set -euo pipefail
PIKIWIDB_ROOT=${PIKIWIDB_ROOT:-/tmp/pikiwidb}
PIKIWIDB_REF=${PIKIWIDB_REF:-v3.5.6}
BUILD_JOBS=${BUILD_JOBS:-$(nproc 2>/dev/null || echo 2)}

if [ ! -d "$PIKIWIDB_ROOT/.git" ]; then
  echo "cloning pikiwidb $PIKIWIDB_REF -> $PIKIWIDB_ROOT"
  git clone --depth 1 --branch "$PIKIWIDB_REF" \
    https://github.com/OpenAtomFoundation/pikiwidb "$PIKIWIDB_ROOT"
fi

if [ -f "$PIKIWIDB_ROOT/output/src/storage/libstorage.a" ] \
   && [ -f "$PIKIWIDB_ROOT/.pic-ok" ]; then
  echo "pikiwidb $PIKIWIDB_REF storage layer already built ($PIKIWIDB_ROOT)"
  exit 0
fi

# PIC-ify every ExternalProject configure block (idempotent)
if ! grep -q "CMAKE_POSITION_INDEPENDENT_CODE=ON" "$PIKIWIDB_ROOT/CMakeLists.txt"; then
  sed -i 's|\(^\s*\)CMAKE_ARGS$|\1CMAKE_ARGS\n  -DCMAKE_POSITION_INDEPENDENT_CODE=ON|' \
    "$PIKIWIDB_ROOT/CMakeLists.txt"
fi

# v4.0+ splits the cache out into an external rediscache project
TARGETS="rocksdb glog gflags fmt snappy zstd lz4 zlib"
if grep -q "ExternalProject_Add(rediscache" "$PIKIWIDB_ROOT/CMakeLists.txt"; then
  TARGETS="$TARGETS rediscache"
fi

echo "building pikiwidb deps + storage (PIC) with -j$BUILD_JOBS — this takes a while"
mkdir -p "$PIKIWIDB_ROOT/output"
cd "$PIKIWIDB_ROOT/output"
cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON .. >prepare-cmake.log 2>&1
make -j"$BUILD_JOBS" $TARGETS >prepare-deps.log 2>&1

# autotools-built deps (jemalloc) do not carry the CMake PIC flag — rebuild
# them explicitly with -fPIC (idempotent; rocksdb links them only at final
# binary/driver link time, so no rocksdb rebuild is triggered)
SRC=$PIKIWIDB_ROOT/buildtrees/Source/jemalloc
if [ -d "$SRC" ]; then
  ( export CFLAGS="-g -O2 -fPIC" CXXFLAGS="-g -O2 -fPIC"
    cd "$SRC"
    ./autogen.sh --prefix="$PIKIWIDB_ROOT/deps" >prepare-jem.log 2>&1
    make -j"$BUILD_JOBS" clean >>prepare-jem.log 2>&1
    make -j"$BUILD_JOBS" >>prepare-jem.log 2>&1
    make install >>prepare-jem.log 2>&1 )
fi

cd "$PIKIWIDB_ROOT/output"
make -j"$BUILD_JOBS" storage >prepare-storage.log 2>&1
touch "$PIKIWIDB_ROOT/.pic-ok"
echo "ready: $PIKIWIDB_ROOT/output/src/{storage,pstd}/lib{storage,pstd}.a"
