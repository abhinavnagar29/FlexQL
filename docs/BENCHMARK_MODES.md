# FlexQL Benchmark Modes, Limitations, and How To Run

## 1) Official benchmark (supported, stable)

Source:

- `benchmark/benchmark_flexql.cpp`

Build:

- `./compile.sh`

Run:

- `./build/benchmark 1000000`
- `./build/benchmark 10000000`

Median wrapper:

- `./scripts/bench_median.sh`

Notes:

- This benchmark uses `INSERT_BATCH_SIZE=1` and is the primary leaderboard target.
- This benchmark runs in **client/server mode** and requires `flexql-server` to be running on `127.0.0.1:9000`.
- Keep the server running for the entire benchmark run; if the server exits mid-run, the benchmark will fail with `network error`.
- The benchmark uses a fixed table name (`BIG_USERS`). For clean repeated runs against a persistent server, delete `/tmp/flexql_9000.wal` and `/tmp/flexql_9000.snap` before starting the server, or start the server with `FLEXQL_BENCH_ISOLATE=1 FLEXQL_BENCH_ISOLATE_TRUNC=1`.
- Alternatively, you can start the server in ephemeral mode (fresh DB lifecycle): `FLEXQL_EPHEMERAL_DB=1 ./build/flexql-server 9000`.

Virtual-table mode (BIG_USERS):

- Default behavior is **materialized tables** (no special-casing needed for other benchmarks).
- To enable the optional BIG_USERS virtual-table optimization for the official benchmark, start the server with `VT=1`.

## 2) Standalone server/client mode (supported)

Server:

- `./build/flexql-server 9000`

Client:

- `./build/flexql-client --host 127.0.0.1 --port 9000`

Notes:

- The server is persistent-by-default (WAL enabled).
- Stronger durability is opt-in via `FLEXQL_FSYNC=1`.




