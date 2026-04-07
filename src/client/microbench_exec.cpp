#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstdint>

#include "flexql.h"

using namespace std::chrono;

static inline uint64_t now_ns(){
    return (uint64_t)duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv){
    long long n = 1000000LL;
    if(argc > 1) n = std::atoll(argv[1]);

    FlexQL* db = nullptr;
    if(flexql_open(nullptr, 0, &db) != FLEXQL_OK || !db){
        std::fprintf(stderr, "Cannot open FlexQL\n");
        return 1;
    }

    char* err = nullptr;
    const char* create = "CREATE TABLE BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);";
    if(flexql_exec(db, create, nullptr, nullptr, &err) != FLEXQL_OK){
        std::fprintf(stderr, "CREATE failed: %s\n", err ? err : "unknown");
        if(err) flexql_free(err);
        flexql_close(db);
        return 1;
    }
    if(err){ flexql_free(err); err = nullptr; }

    // Exact benchmark-style single-row insert string.
    const char* ins = "INSERT INTO BIG_USERS VALUES (1, 'Alice', 'alice@example.com', 1200, 1893456000);";

    // Warmup.
    for(int i=0;i<10000;i++) (void)flexql_exec(db, ins, nullptr, nullptr, &err);
    if(err){ flexql_free(err); err = nullptr; }

    const uint64_t t0 = now_ns();
    for(long long i=0;i<n;i++){
        if(flexql_exec(db, ins, nullptr, nullptr, &err) != FLEXQL_OK){
            std::fprintf(stderr, "INSERT failed: %s\n", err ? err : "unknown");
            if(err) flexql_free(err);
            flexql_close(db);
            return 1;
        }
        if(err){ flexql_free(err); err = nullptr; }
    }
    const uint64_t t1 = now_ns();

    const double total_ns = (double)(t1 - t0);
    const double ns_per = total_ns / (double)n;
    const double ms_total = total_ns / 1e6;
    const double ops = (double)n / (total_ns / 1e9);

    std::printf("microbench_exec: n=%lld\n", n);
    std::printf("Elapsed: %.3f ms\n", ms_total);
    std::printf("Cost: %.1f ns/op\n", ns_per);
    std::printf("Throughput: %.0f ops/sec\n", ops);

    flexql_close(db);
    return 0;
}
