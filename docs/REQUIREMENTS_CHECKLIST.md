# FlexQL — requirement checklist

**Student:** Abhinav Nagar (**25CS60R71**)



| Requirement | Implementation | Notes |
|-------------|----------------|-------|
| C/C++ only, no external DB libs | `src/`, `include/` | Vendored `fast_float` header only. |
| Client API: `flexql_open`, `flexql_close`, `flexql_exec`, `flexql_free`, `FLEXQL_OK` / `FLEXQL_ERROR` | `include/flexql.h`, `src/client/flexql.cpp` | Extra: `flexql_exec_many`, `flexql_is_uds` (non-breaking). |
| Callback once per row; return 1 to stop | `emit_cb_*` in `flexql.cpp` | Default: full `argc`/`argv`/`azColName`. `FLEXQL_ROWSTRING=1`: single `argv[0]` pipe-row (benchmark/server default). |
| CREATE TABLE (DECIMAL, VARCHAR; optional INT wording) | `parse_create` | PRIMARY KEY / EXPIRES_AT detected; schema enforced on insert. |
| INSERT + batch VALUES | `parse_insert`, `parse_insert_big_users`, `flexql_exec_many` | Official benchmark multi-row `INSERT INTO ... VALUES (...),(...);` supported. |
| SELECT * / column list | `exec_select_single` | |
| WHERE: single predicate, ops `= > < >= <=` | `match_num` / `match_str`, scanner | No AND/OR. |
| INNER JOIN + optional WHERE | `exec_join` | Equality join uses hash on build side; non-equality uses nested-loop. ON clause supports same ops as WHERE. |
| Primary key index | `PkIndex` (flat + optional Swiss table) | |
| Caching | Client-side SELECT cache defaults ON (unset) and can be disabled via `FLEXQL_QUERY_CACHE=0`; server-side SELECT response cache defaults ON (unset) and can be disabled via `FLEXQL_SERVER_SELECT_CACHE=0` |  disable caches for strict evaluation by setting both env vars to `0`. |
| Multithreaded / concurrent clients | `flexql-server`: `poll` + per-connection state | The server supports multiple simultaneous clients via multiplexed I/O. The engine executes requests serially for correctness. |
| Thread-safe client handle (shared `FlexQL*`) | `FlexQL::mu` in `src/client/flexql.cpp` |  |
| **Persistence (disk)** | WAL append + optional snapshot checkpoint | **Default ON** unless `FLEXQL_PERSIST=0`. Files: `/tmp/flexql_<port>.wal`, `.snap` (override with `FLEXQL_WAL_PATH`, `FLEXQL_SNAPSHOT_PATH`). |
| Fault tolerance | checkpoint truncates WAL after snapshot | Crash between buffer and disk can lose last buffered writes unless fsync tuned. |
| Official benchmark source | `benchmark/benchmark_flexql.cpp` |  |



