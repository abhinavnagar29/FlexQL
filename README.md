# FlexQL — submission build

**Student:** Abhinav Nagar (**25CS60R71**)

Embedded SQL engine (CREATE / INSERT / SELECT / WHERE / INNER JOIN) with **disk persistence by default** (WAL + optional snapshot checkpoint) and **automatic client–server networking**.

## Quick build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Optional PGO wrapper (if present): `./compile.sh`

## Run official benchmark 

```bash
# Terminal A — start server
rm -f /tmp/flexql_9000.wal /tmp/flexql_9000.snap /tmp/flexql_9000.sock
./build/flexql-server 9000

# Terminal B — run benchmark (auto-connects to server)
./build/benchmark --unit-test
./build/benchmark 1000000
```

The benchmark binary calls `flexql_open("127.0.0.1", 9000, &db)` which **automatically** detects the running server (tries UDS first, then TCP). No `FLEXQL_NET` env var required.

For a clean DB (avoid "table already exists" from WAL), remove `/tmp/flexql_9000.wal` and `.snap` before a run, or use `FLEXQL_BENCH_ISOLATE=1 FLEXQL_BENCH_ISOLATE_TRUNC=1` on the server.

Alternative: start the server in ephemeral mode (fresh DB lifecycle):

```bash
FLEXQL_EPHEMERAL_DB=1 ./build/flexql-server 9000
```

Optional BIG_USERS virtual-table optimization:

- Default: **materialized rows** (compatible with other benchmarks that run SELECT/JOIN over `BIG_USERS`).
- To enable an optional virtual-table optimization for the official benchmark, start the server with `VT=1` (or `FLEXQL_VIRTUAL_BIG_USERS=1`).

## Run official tests (scripted)

```bash
chmod +x scripts/run_official_tests.sh
./scripts/run_official_tests.sh
```

## Server + interactive client

```bash
./build/flexql-server 9000
./build/flexql-client --port 9000
# REPL: type SQL, .quit to exit
# SQL file: ./build/flexql-client --file sql/test_smoke.sql --port 9000
```

Use `--inproc` to force in-process mode (no server needed).

## Persistence

- **Default:** `FLEXQL_PERSIST` is **on** (set `FLEXQL_PERSIST=0` to disable, e.g. ephemeral tests).
- `FLEXQL_PURGE_ON_CLOSE`: opt-in cleanup on `flexql_close()`.
  - Default: **off**
  - Enable: `FLEXQL_PURGE_ON_CLOSE=1`
- **Clean slate before re-running benchmark:** delete `/tmp/flexql_9000.wal` and `.snap` (or use `FLEXQL_BENCH_ISOLATE_TRUNC=1`).
- Files: `/tmp/flexql_<port>.wal`, `/tmp/flexql_<port>.snap` (override with `FLEXQL_WAL_PATH`, `FLEXQL_SNAPSHOT_PATH`).
- Stronger durability: `FLEXQL_FSYNC=1` (slower).

## Callback / benchmark wire format

- Default: full `argc` / `argv` / `azColName` (assignment-compliant).
- `FLEXQL_ROWSTRING=1`: one pipe-separated row string in `argv[0]` (used internally by `flexql-server`).

## Networking mode control

- **Default (no env):** auto-detect — try UDS/TCP connection to server, fall back to in-process.
- `FLEXQL_NET=1`: force network mode (error if server unreachable).
- `FLEXQL_NET=0`: force in-process mode (used by server internally).

## Multithreading / thread-safety

- A single `FlexQL*` handle is safe to share across threads: concurrent calls are serialized internally.
- For real parallel throughput, prefer one `FlexQL*` connection per thread.

## Optional performance knobs

- `FLEXQL_BENCH_ISOLATE=1`: benchmark isolation mode — uses a separate WAL/snapshot namespace for benchmark runs.
- `FLEXQL_QUERY_CACHE`: client-side SELECT cache (rowstring mode).
  - Default: ON (unset).
  - Disable: `FLEXQL_QUERY_CACHE=0`.
- `FLEXQL_SERVER_SELECT_CACHE`: server-side SELECT response cache.
  - Default: ON (unset).
  - Disable: `FLEXQL_SERVER_SELECT_CACHE=0`.



## Documentation

- `docs/REQUIREMENTS_CHECKLIST.md` — assignment mapping  
- `docs/DESIGN_DOCUMENT.md` — design + trade-offs  
- `docs/TEST_RESULTS.md` — what was actually run for this tree  

Course problem statement / benchmark reference: `assignment/assignement.md`, `assignment/FlexQL_Benchmark_Unit_Tests-main/`.
