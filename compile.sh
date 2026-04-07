#!/usr/bin/env bash
set -euo pipefail

on_interrupt(){
  echo "[INFO] Interrupted. If this was during PGO training, you can build faster with: FLEXQL_PGO_SKIP_TRAIN=1 ./compile.sh" >&2
  exit 130
}
trap on_interrupt INT TERM

TMPROOT="${FLEXQL_TMPDIR:-"$(pwd)/.flexql_tmp"}"
mkdir -p -- "${TMPROOT}"
export TMPDIR="${TMPROOT}"

safe_rmdir(){
  local d="$1"
  if [ -e "${d}" ]; then
    command rm -rf -- "${d}" 2>/dev/null || true
  fi
  if [ -e "${d}" ]; then
    local ts
    ts="$(date +%s)"
    command mv -f -- "${d}" "${d}.stale.${ts}.$$" 2>/dev/null || true
  fi
}

safe_rmdir build-pgo
safe_rmdir build

# Use an absolute path without spaces. Relative paths can resolve differently between:
# - compile-time (invoked from build dir)
# - runtime (invoked from repo root)
# which leads to missing-profile warnings and can make PGO slower than non-PGO.
PROFDIR="${FLEXQL_PGO_PROFDIR:-/tmp/flexql_pgo_prof}"
SKIP_TRAIN="${FLEXQL_PGO_SKIP_TRAIN:-0}"
TRAIN_ROWS="${FLEXQL_PGO_TRAIN_ROWS:-10000000}"

if [ "${SKIP_TRAIN}" != "1" ]; then
  command rm -rf -- "${PROFDIR}"
fi

if [ "${SKIP_TRAIN}" = "1" ]; then
  if ! ls -1 "${PROFDIR}"/*.gcda >/dev/null 2>&1; then
    echo "[WARN] FLEXQL_PGO_SKIP_TRAIN=1 but no existing profiles in ${PROFDIR}; falling back to non-PGO Release build." >&2
    safe_rmdir build
    echo "[INFO] Configuring (non-PGO Release)..." >&2
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_FLAGS_RELEASE="-Ofast -march=native -mtune=native -flto -DNDEBUG -funroll-loops -ffast-math -fomit-frame-pointer -fno-exceptions -fno-rtti" \
      -DCMAKE_EXE_LINKER_FLAGS_RELEASE="-flto"
    echo "[INFO] Building (non-PGO Release)..." >&2
    cmake --build build --target benchmark flexql-server flexql-client microbench_exec -j"$(nproc)"
    echo "[INFO] Build complete: ./build" >&2
    exit 0
  fi

  safe_rmdir build-pgo
  echo "[INFO] Configuring (PGO use)..." >&2
  cmake -S . -B build-pgo -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS_RELEASE="-Ofast -march=native -mtune=native -flto -DNDEBUG -funroll-loops -ffast-math -fomit-frame-pointer -fprofile-use=${PROFDIR} -fprofile-correction -Wno-missing-profile -Wno-coverage-mismatch -Wno-error=coverage-mismatch -fno-exceptions -fno-rtti" \
    -DCMAKE_EXE_LINKER_FLAGS_RELEASE="-flto -fprofile-use=${PROFDIR} -fprofile-correction -lgcov"

  set +e
  echo "[INFO] Building (PGO use)..." >&2
  cmake --build build-pgo --target benchmark flexql-server flexql-client microbench_exec -j"$(nproc)"
  st=$?
  set -e

  if [ "${st}" -ne 0 ]; then
    echo "[WARN] PGO -use build failed (likely stale profiles); falling back to non-PGO Release build." >&2
    safe_rmdir build
    echo "[INFO] Configuring (non-PGO Release)..." >&2
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_FLAGS_RELEASE="-Ofast -march=native -mtune=native -flto -DNDEBUG -funroll-loops -ffast-math -fomit-frame-pointer -fno-exceptions -fno-rtti" \
      -DCMAKE_EXE_LINKER_FLAGS_RELEASE="-flto"
    echo "[INFO] Building (non-PGO Release)..." >&2
    cmake --build build --target benchmark flexql-server flexql-client microbench_exec -j"$(nproc)"
    echo "[INFO] Build complete: ./build" >&2
    exit 0
  fi

  safe_rmdir build
  cp -a build-pgo build
  echo "[INFO] Build complete: ./build (PGO)" >&2
  exit 0
fi

echo "[INFO] Configuring (PGO generate)..." >&2
cmake -S . -B build-pgo -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS_RELEASE="-Ofast -march=native -mtune=native -flto -DNDEBUG -funroll-loops -ffast-math -fomit-frame-pointer -fprofile-generate=${PROFDIR} -fno-exceptions -fno-rtti" \
  -DCMAKE_EXE_LINKER_FLAGS_RELEASE="-flto -fprofile-generate=${PROFDIR} -lgcov"
echo "[INFO] Building (PGO generate)..." >&2
cmake --build build-pgo --target benchmark flexql-server flexql-client microbench_exec -j"$(nproc)"
echo "[INFO] Training PGO profiles (rows=${TRAIN_ROWS})..." >&2
PGO_WAL="${TMPROOT}/pgo_${$}.wal"
PGO_SNAP="${TMPROOT}/pgo_${$}.snap"
command rm -f -- "${PGO_WAL}" "${PGO_SNAP}" 2>/dev/null || true
env FLEXQL_NET=0 FLEXQL_PERSIST=1 FLEXQL_CHECKPOINT_BYTES=0 FLEXQL_WAL_PATH="${PGO_WAL}" FLEXQL_SNAPSHOT_PATH="${PGO_SNAP}" ./build-pgo/benchmark "${TRAIN_ROWS}" || true

# If profile data is missing, PGO -use can be slower than a normal -Ofast build.
# In that case, fall back to a plain optimized build to protect benchmark throughput.
if ! ls -1 "${PROFDIR}"/*.gcda >/dev/null 2>&1; then
  echo "[WARN] No PGO profiles produced in ${PROFDIR}; falling back to non-PGO Release build." >&2
  safe_rmdir build
  echo "[INFO] Configuring (non-PGO Release)..." >&2
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS_RELEASE="-Ofast -march=native -mtune=native -flto -DNDEBUG -funroll-loops -ffast-math -fomit-frame-pointer -fno-exceptions -fno-rtti" \
    -DCMAKE_EXE_LINKER_FLAGS_RELEASE="-flto"
  echo "[INFO] Building (non-PGO Release)..." >&2
  cmake --build build --target benchmark flexql-server flexql-client microbench_exec -j"$(nproc)"
  echo "[INFO] Build complete: ./build" >&2
  exit 0
fi

echo "[INFO] Configuring (PGO use)..." >&2
cmake -S . -B build-pgo -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS_RELEASE="-Ofast -march=native -mtune=native -flto -DNDEBUG -funroll-loops -ffast-math -fomit-frame-pointer -fprofile-use=${PROFDIR} -fprofile-correction -Wno-missing-profile -Wno-coverage-mismatch -Wno-error=coverage-mismatch -fno-exceptions -fno-rtti" \
  -DCMAKE_EXE_LINKER_FLAGS_RELEASE="-flto -fprofile-use=${PROFDIR} -fprofile-correction -lgcov"
echo "[INFO] Building (PGO use)..." >&2
cmake --build build-pgo --target benchmark flexql-server flexql-client microbench_exec -j"$(nproc)"

safe_rmdir build
cp -a build-pgo build
echo "[INFO] Build complete: ./build (PGO)" >&2
