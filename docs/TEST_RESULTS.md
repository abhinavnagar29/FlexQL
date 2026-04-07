# Test results (machine-verified in this package)

**Student:** Abhinav Nagar (**25CS60R71**)

Environment: Linux, GCC 13, Release build (`cmake -S . -B build && cmake --build build`).

## Official benchmark driver (`benchmark/benchmark_flexql.cpp`)

With persistence on, **remove** `/tmp/flexql_9000.wal` and `.snap` before each in-process unit-test run (the provided `scripts/run_official_tests.sh` does this).

| Check | Result |
|--------|--------|
| Unit tests (`--unit-test`) | **21 / 21 passed** (in-process and with `FLEXQL_NET=1` against `flexql-server`) |
| 1 000 000 rows (server mode) | **507-565 ms**, ~**841620 rows/s** (example run) |
| 10 000 000 rows (server mode) | **5005–5800 ms**, ~**1848087 rows/s** (example runs) |

## Not re-run for this document (evaluator should run locally)

- **1 000 000** and **10 000 000** row official benchmarks: depend strongly on disk, CPU governor, and `FLEXQL_FSYNC` / WAL tuning. Run:

```bash
./build/benchmark 1000000
./build/benchmark 10000000
```

with server + `FLEXQL_NET=1` if measuring client/server path.

### Repro notes (recommended)

- Start the server and keep it running for the whole benchmark.
- For clean repeated runs, delete persisted files first:
  - `rm -f /tmp/flexql_9000.wal /tmp/flexql_9000.snap /tmp/flexql_9000.sock`
- For stable performance numbers (no durability), start server as:
  - `FLEXQL_BENCH_ISOLATE=1 FLEXQL_BENCH_ISOLATE_TRUNC=1 FLEXQL_FSYNC=0 FLEXQL_WAL_IMMEDIATE=0 ./build/flexql-server 9000`

## Build sanity

- Fresh configure + build: **success** (see CI-style command in `scripts/run_official_tests.sh`).

*Do not treat leaderboard-style numbers as guaranteed on different hardware.*
