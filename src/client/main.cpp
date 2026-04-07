#include "../../include/flexql.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <string>
#include <ctime>
#include <cstdint>
 #include <iostream>

#include <unistd.h>

static inline void trim_leading_ws_and_dashdash(std::string& s){
    while(true){
        size_t i = 0;
        while(i < s.size() && (s[i]==' '||s[i]=='\t'||s[i]=='\n'||s[i]=='\r')) ++i;
        if(i >= s.size()){
            s.clear();
            return;
        }
        if(i + 1 < s.size() && s[i]=='-' && s[i+1]=='-'){
            i += 2;
            while(i < s.size() && s[i] != '\n') ++i;
            s.erase(0, i);
            continue;
        }
        if(i) s.erase(0, i);
        return;
    }
}

struct PrintCtx {
    bool pretty = false;
};

static int print_row(void* ctx, int argc, char** argv, char** colnames){
    PrintCtx* pc = (PrintCtx*)ctx;
    if(pc && pc->pretty){
        for(int i=0;i<argc;i++){
            const char* cn = (colnames && colnames[i]) ? colnames[i] : "";
            std::printf("%s = %s\n", cn, argv[i] ? argv[i] : "NULL");
        }
        std::printf("\n");
        return 0;
    }
    for(int i=0;i<argc;i++){
        if(i) std::printf("|");
        std::printf("%s", argv[i] ? argv[i] : "NULL");
    }
    std::printf("\n");
    return 0;
}

static inline uint64_t now_ns(){
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline bool pop_next_sql_stmt(std::string& buf, std::string& out){
    // Extracts next SQL statement ending in ';' from buf.
    // Splits on semicolons that are NOT inside single-quoted strings.
    // Supports SQL escaping of single quote via doubled quote: ''
    bool in_sq = false;
    for(size_t i = 0; i < buf.size(); ++i){
        const char c = buf[i];
        if(c == '\'' ){
            if(in_sq){
                if(i + 1 < buf.size() && buf[i + 1] == '\''){
                    ++i; // consume escaped quote
                } else {
                    in_sq = false;
                }
            } else {
                in_sq = true;
            }
            continue;
        }
        if(c == ';' && !in_sq){
            out.assign(buf.data(), i + 1);
            std::string rest;
            if(i + 1 < buf.size()) rest.assign(buf.data() + i + 1, buf.size() - (i + 1));
            buf.swap(rest);
            return true;
        }
    }
    return false;
}

int main(int argc, char** argv){
    const char* host = "127.0.0.1";
    int port = 9000;
    bool net = true;
    bool force_tcp = false;
    bool force_uds = false;
    bool quiet = false;
    bool timing = false;
    bool pretty = false;
    const char* sock_path = nullptr;
    const char* file_path = nullptr;

    for(int i=1;i<argc;i++){
        if(std::strcmp(argv[i], "--inproc")==0) net = false;
        else if(std::strcmp(argv[i], "--net")==0) net = true;
        else if(std::strcmp(argv[i], "--tcp")==0){ force_tcp = true; force_uds = false; }
        else if(std::strcmp(argv[i], "--uds")==0){ force_uds = true; force_tcp = false; }
        else if(std::strcmp(argv[i], "--file")==0 && i+1<argc) file_path = argv[++i];
        else if(std::strcmp(argv[i], "--quiet")==0) quiet = true;
        else if(std::strcmp(argv[i], "--time")==0) timing = true;
        else if(std::strcmp(argv[i], "--pretty")==0) pretty = true;
        else if(std::strcmp(argv[i], "--sock")==0 && i+1<argc) sock_path = argv[++i];
        else if(std::strcmp(argv[i], "--host")==0 && i+1<argc) host = argv[++i];
        else if(std::strcmp(argv[i], "--port")==0 && i+1<argc) port = std::atoi(argv[++i]);
        else {
            std::fprintf(stderr, "Usage: %s [--net|--inproc] [--tcp|--uds] [--sock PATH] [--host HOST] [--port PORT] [--file PATH] [--quiet] [--time] [--pretty]\n", argv[0]);
            return 2;
        }
    }

    if(!pretty){
        const char* pv = std::getenv("FLEXQL_PRETTY");
        if(pv && (pv[0]=='1' || strcasecmp(pv,"true")==0 || strcasecmp(pv,"yes")==0)) pretty = true;
    }

    // If input is not a terminal (pipe/redirect), disable prompts/banner by default.
    if(!quiet && !::isatty(STDIN_FILENO)) quiet = true;

    // With auto-detect in flexql_open, we only need to force in-process if --inproc.
    if(!net) ::setenv("FLEXQL_NET", "0", 1);
    if(force_tcp){
        if(host && (std::strcmp(host,"localhost")==0)) host = "127.0.0.1";
    }
    if(force_uds){
        host = "127.0.0.1";
    }

    if(sock_path){
        // Expected format: /tmp/flexql_<port>.sock
        const char* p = std::strrchr(sock_path, '_');
        const char* d = p ? p+1 : nullptr;
        int parsed_port = 0;
        if(d) parsed_port = std::atoi(d);
        if(parsed_port > 0) port = parsed_port;
        host = "127.0.0.1";
        force_uds = true;
    }

    FlexQL* db = nullptr;
    if(flexql_open(host, port, &db) != FLEXQL_OK){
        const int e = errno;
        std::fprintf(stderr, "flexql_open failed (errno=%d: %s)\n", e, std::strerror(e));
        return 1;
    }

    if(net && !quiet){
        std::printf("Connected via %s\n", flexql_is_uds(db) ? "UDS" : "TCP");
    }

    std::string line;
    line.reserve(4096);
    std::string stmt;
    stmt.reserve(1u<<20);

    PrintCtx pctx;
    pctx.pretty = pretty;

    auto exec_ready = [&](){
        while(true){
            std::string to_exec;
            if(!pop_next_sql_stmt(stmt, to_exec)) break;

            trim_leading_ws_and_dashdash(to_exec);
            if(to_exec.empty() || to_exec == ";"){
                trim_leading_ws_and_dashdash(stmt);
                continue;
            }

            char* err = nullptr;
            const uint64_t t0 = timing ? now_ns() : 0;
            int rc = flexql_exec(db, to_exec.c_str(), print_row, &pctx, &err);
            const uint64_t t1 = timing ? now_ns() : 0;
            if(rc != FLEXQL_OK){
                std::fprintf(stderr, "ERROR: %s\n", err ? err : "unknown error");
            }
            if(err) flexql_free(err);
            if(timing){
                double ms = (double)(t1 - t0) / 1e6;
                std::fprintf(stderr, "[time] %.3f ms\n", ms);
            }

            trim_leading_ws_and_dashdash(stmt);
        }
    };

    if(file_path){
        FILE* f = std::fopen(file_path, "rb");
        if(!f){
            std::fprintf(stderr, "failed to open file: %s\n", file_path);
            flexql_close(db);
            return 1;
        }
        char buf[1<<15];
        while(true){
            size_t n = std::fread(buf, 1, sizeof(buf), f);
            if(n) stmt.append(buf, n);
            if(n < sizeof(buf)) break;
        }
        std::fclose(f);
        // If the script doesn't end with ';' but has content, execute last statement.
        trim_leading_ws_and_dashdash(stmt);
        if(!stmt.empty() && stmt.find(';') == std::string::npos) stmt.push_back(';');
        exec_ready();
        flexql_close(db);
        return 0;
    }

    if(!quiet){
        std::printf("FlexQL client. Type SQL and press enter. Type .quit to exit.\n");
        std::fflush(stdout);
    }
    while(true){
        if(!quiet){
            std::printf("%s", stmt.empty() ? "flexql> " : ".....> ");
            std::fflush(stdout);
        }
        if(!std::getline(std::cin, line)) break;
        if(line == ".quit" || line == ".exit") break;
        if(line.empty()) continue;

        // Accumulate multi-line SQL until ';' is seen.
        if(!stmt.empty()) stmt.push_back('\n');
        stmt.append(line);

        // If statement not complete yet, keep reading.
        std::string tmp;
        if(!pop_next_sql_stmt(stmt, tmp)) continue;
        // Put back and let exec_ready drain all complete statements.
        stmt.insert(0, tmp);
        exec_ready();
    }

    flexql_close(db);
    return 0;
}
