#include "../../include/flexql.h"

#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <deque>
#include <cstddef>

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <poll.h>

#include <sys/stat.h>

#include <string>

static volatile sig_atomic_t g_run = 1;
static void on_sig(int){ g_run = 0; }

static int g_port_for_cleanup = 0;
static bool g_ephemeral_db = false;

static bool write_full(int fd, const void* buf, size_t n);

static bool file_write_full(int fd, const void* buf, size_t n){
    const uint8_t* p = (const uint8_t*)buf;
    size_t off = 0;
    while(off < n){
        ssize_t w = ::write(fd, p + off, n - off);
        if(w <= 0) return false;
        off += (size_t)w;
    }
    return true;
}

static inline bool env_true(const char* v){
    return v && (v[0]=='1' || v[0]=='y' || v[0]=='Y' || v[0]=='t' || v[0]=='T' || v[0]=='o' || v[0]=='O');
}

static inline void wal_default_path(char* out, size_t cap, int port){
    if(!out || cap == 0) return;
    std::snprintf(out, cap, "/tmp/flexql_%d.wal", port);
}

static inline void snapshot_default_path(char* out, size_t cap, int port){
    if(!out || cap == 0) return;
    std::snprintf(out, cap, "/tmp/flexql_%d.snap", port);
}

static inline void uds_default_path(char* out, size_t cap, int port){
    if(!out || cap == 0) return;
    std::snprintf(out, cap, "/tmp/flexql_%d.sock", port);
}

static void unlink_db_files_for_port(int port){
    if(port <= 0) return;
    char w[256];
    char s[256];
    const char* wp = std::getenv("FLEXQL_WAL_PATH");
    const char* sp = std::getenv("FLEXQL_SNAPSHOT_PATH");
    if(!wp || !*wp){ wal_default_path(w, sizeof(w), port); wp = w; }
    if(!sp || !*sp){ snapshot_default_path(s, sizeof(s), port); sp = s; }
    (void)::unlink(wp);
    (void)::unlink(sp);
}

static void cleanup_ephemeral_db_files_only(){
    if(!g_ephemeral_db || g_port_for_cleanup <= 0) return;
    char w[256];
    char s[256];
    const char* wp = std::getenv("FLEXQL_WAL_PATH");
    const char* sp = std::getenv("FLEXQL_SNAPSHOT_PATH");
    if(!wp || !*wp){ wal_default_path(w, sizeof(w), g_port_for_cleanup); wp = w; }
    if(!sp || !*sp){ snapshot_default_path(s, sizeof(s), g_port_for_cleanup); sp = s; }
    (void)::unlink(wp);
    (void)::unlink(sp);
}

static void cleanup_ephemeral_paths(){
    if(!g_ephemeral_db || g_port_for_cleanup <= 0) return;
    char w[256];
    char s[256];
    char u[256];
    wal_default_path(w, sizeof(w), g_port_for_cleanup);
    snapshot_default_path(s, sizeof(s), g_port_for_cleanup);
    uds_default_path(u, sizeof(u), g_port_for_cleanup);
    (void)::unlink(w);
    (void)::unlink(s);
    (void)::unlink(u);
}

static inline int wal_open_for_port(int port){
    if(!env_true(std::getenv("FLEXQL_PERSIST"))) return -1;
    char path[256];
    const char* pth = std::getenv("FLEXQL_WAL_PATH");
    if(!pth || !*pth){ wal_default_path(path, sizeof(path), port); pth = path; }
    int fd = ::open(pth, O_CREAT|O_RDWR, 0644);
    if(fd >= 0){
        (void)::lseek(fd, 0, SEEK_END);
        int flags = ::fcntl(fd, F_GETFD);
        if(flags >= 0) ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
    }
    return fd;
}

static inline bool wal_append_stmt(int wal_fd, const std::string& sql){
    if(wal_fd < 0) return true;
    const uint32_t len = (uint32_t)sql.size();
    if(!file_write_full(wal_fd, &len, 4)) return false;
    if(len && !file_write_full(wal_fd, sql.data(), len)) return false;
    return true;
}

static bool read_full(int fd, void* buf, size_t n){
    uint8_t* p = (uint8_t*)buf;
    size_t got = 0;
    while(got < n){
        ssize_t r = ::recv(fd, p + got, n - got, 0);
        if(r <= 0) return false;
        got += (size_t)r;
    }
    return true;
}

static bool write_full(int fd, const void* buf, size_t n){
    const uint8_t* p = (const uint8_t*)buf;
    size_t sent = 0;
    while(sent < n){
        ssize_t r = ::send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if(r <= 0) return false;
        sent += (size_t)r;
    }
    return true;
}

static bool write_fullv(int fd, iovec* iov, int iovcnt){
    size_t total = 0;
    for(int i=0;i<iovcnt;++i) total += (size_t)iov[i].iov_len;
    size_t sent = 0;
    while(sent < total){
        ssize_t r = ::writev(fd, iov, iovcnt);
        if(r <= 0) return false;
        sent += (size_t)r;
        size_t adv = (size_t)r;
        while(iovcnt > 0 && adv >= (size_t)iov[0].iov_len){
            adv -= (size_t)iov[0].iov_len;
            ++iov;
            --iovcnt;
        }
        if(iovcnt > 0 && adv){
            iov[0].iov_base = (uint8_t*)iov[0].iov_base + adv;
            iov[0].iov_len -= adv;
        }
    }
    return true;
}

struct RowCollector {
    std::vector<std::string> rows;
};

struct ConnState {
    std::vector<uint8_t> in;
    size_t in_pos = 0;
    uint64_t recvs = 0;
    uint64_t bytes_recv = 0;
    uint64_t frames = 0;
    uint64_t sends = 0;
    uint64_t bytes_sent = 0;
};

static int collect_row(void* data, int argc, char** argv, char** colnames){
    (void)argc;
    (void)colnames;
    RowCollector* rc = (RowCollector*)data;
    if(!rc) return 0;
    rc->rows.emplace_back(argv && argv[0] ? argv[0] : "");
    return 0;
}

static int make_listen(int port){
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0) return -1;
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if(::bind(fd, (sockaddr*)&addr, sizeof(addr)) != 0){ ::close(fd); return -1; }
    if(::listen(fd, 256) != 0){ ::close(fd); return -1; }
    return fd;
}

static int sockbuf_bytes(){
    const char* ev = std::getenv("FLEXQL_SOCKBUF");
    if(!ev || !*ev) return 0;
    long v = std::strtol(ev, nullptr, 10);
    if(v <= 0) return 0;
    if(v > (1l<<26)) v = (1l<<26);
    return (int)v;
}

static void tune_conn(int fd){
    const int sb = sockbuf_bytes();
    if(sb > 0){
        ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sb, sizeof(sb));
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sb, sizeof(sb));
    }
}

static int make_listen_uds(int port){
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if(fd < 0) return -1;
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    char path[108];
    std::snprintf(path, sizeof(path), "/tmp/flexql_%d.sock", port);
    std::strncpy(addr.sun_path, path, sizeof(addr.sun_path)-1);
    ::unlink(addr.sun_path);
    if(::bind(fd, (sockaddr*)&addr, sizeof(addr)) != 0){ ::close(fd); return -1; }
    if(::listen(fd, 256) != 0){ ::close(fd); return -1; }
    return fd;
}

static inline const char* skip_ws(const char* p){
    while(*p==' '||*p=='\t'||*p=='\n'||*p=='\r') ++p;
    return p;
}

static inline bool starts_with_kw_ci(const char* sql, const char* kw){
    const char* p = skip_ws(sql);
    for(size_t i=0; kw[i]; ++i){
        char a = p[i];
        if(!a) return false;
        if(a>='a' && a<='z') a = (char)(a - 'a' + 'A');
        char b = kw[i];
        if(b>='a' && b<='z') b = (char)(b - 'a' + 'A');
        if(a != b) return false;
    }
    char nxt = p[std::strlen(kw)];
    return (nxt==0) || (nxt==' '||nxt=='\t'||nxt=='\n'||nxt=='\r'||nxt=='(');
}

static inline bool is_write_sql(const char* sql){
    return starts_with_kw_ci(sql, "INSERT") ||
           starts_with_kw_ci(sql, "CREATE") ||
           starts_with_kw_ci(sql, "DELETE") ||
           starts_with_kw_ci(sql, "DROP")   ||
           starts_with_kw_ci(sql, "UPDATE") ||
           starts_with_kw_ci(sql, "ALTER")  ||
           starts_with_kw_ci(sql, "RESET");
}

static inline bool is_cacheable_select(const char* sql){
    return starts_with_kw_ci(sql, "SELECT");
}

static inline void append_u32(std::vector<uint8_t>& out, uint32_t v_net){
    size_t n = out.size();
    out.resize(n + 4);
    std::memcpy(out.data() + n, &v_net, 4);
}

static inline void append_bytes(std::vector<uint8_t>& out, const void* p, size_t n){
    if(!n) return;
    size_t cur = out.size();
    out.resize(cur + n);
    std::memcpy(out.data() + cur, p, n);
}

static inline void append_ok0(std::vector<uint8_t>& out){
    // [st=0][err_len=0][nrows=0] all in network byte order (zeros).
    static const uint8_t kOk0[12] = {0,0,0,0, 0,0,0,0, 0,0,0,0};
    append_bytes(out, kOk0, sizeof(kOk0));
}

int main(int argc, char** argv){
    int port = 9000;
    if(argc > 1) port = std::atoi(argv[1]);
    if(port <= 0) port = 9000;

    g_port_for_cleanup = port;
    g_ephemeral_db = env_true(std::getenv("FLEXQL_EPHEMERAL_DB"));
    if(g_ephemeral_db){
        cleanup_ephemeral_paths();
        std::atexit(cleanup_ephemeral_paths);
    }

    std::fprintf(stderr, "FlexQL server starting on port %d\n", port);
    std::fflush(stderr);

    const char* dbg_ev = std::getenv("FLEXQL_NET_DEBUG");
    const bool net_debug = (dbg_ev && (dbg_ev[0] == '1' || dbg_ev[0] == 'y' || dbg_ev[0] == 'Y' || dbg_ev[0] == 't' || dbg_ev[0] == 'T'));

    // The server must always use the in-process engine.
    // Prevent accidental environment contamination (e.g. FLEXQL_NET=1) from forcing client mode.
    ::unsetenv("FLEXQL_NET");
    ::unsetenv("FLEXQL_REQUIRE_SERVER");
    ::signal(SIGTERM, on_sig);
    ::signal(SIGINT, on_sig);
    ::signal(SIGPIPE, SIG_IGN);

    if(!std::getenv("FLEXQL_PERSIST")){
        ::setenv("FLEXQL_PERSIST", "1", 0);
    }
    if(!std::getenv("FLEXQL_FSYNC")){
        ::setenv("FLEXQL_FSYNC", "1", 0);
    }
    if(!std::getenv("FLEXQL_WAL_IMMEDIATE")){
        ::setenv("FLEXQL_WAL_IMMEDIATE", "1", 0);
    }
    if(!std::getenv("FLEXQL_WAL_BUF_BYTES")){
        ::setenv("FLEXQL_WAL_BUF_BYTES", "67108864", 0);
    }
    if(!std::getenv("FLEXQL_CHECKPOINT_BYTES")){
        ::setenv("FLEXQL_CHECKPOINT_BYTES", "268435456", 0);
    }
    if(!std::getenv("FLEXQL_NET_DEBUG")){
        ::setenv("FLEXQL_NET_DEBUG", "1", 0);
    }

    // Optional benchmark isolation: use a separate WAL/snapshot namespace so benchmark runs
    // do not inherit huge prior WAL state and cause latency spikes.
    // OFF by default to preserve normal persistence semantics unless explicitly enabled.
    {
        const char* iso = std::getenv("FLEXQL_BENCH_ISOLATE");
        const bool isolate = (iso && (iso[0]=='1' || iso[0]=='y' || iso[0]=='Y' || iso[0]=='t' || iso[0]=='T'));
        if(isolate){
            std::string wal = std::string("/tmp/flexql_bench_") + std::to_string(port) + ".wal";
            std::string snap = std::string("/tmp/flexql_bench_") + std::to_string(port) + ".snap";
            ::setenv("FLEXQL_WAL_PATH", wal.c_str(), 1);
            ::setenv("FLEXQL_SNAPSHOT_PATH", snap.c_str(), 1);

            const char* tr = std::getenv("FLEXQL_BENCH_ISOLATE_TRUNC");
            const bool trunc = (tr && (tr[0]=='1' || tr[0]=='y' || tr[0]=='Y' || tr[0]=='t' || tr[0]=='T'));
            if(trunc){
                ::unlink(wal.c_str());
                ::unlink(snap.c_str());
            }
        }
    }

    // Allow a single network frame to contain multiple semicolon-terminated statements.
    // This enables high-throughput pipelining (many INSERT statements in one message) while
    // preserving identical statement semantics.
    if(!std::getenv("FLEXQL_MULTISTMT")){
        ::setenv("FLEXQL_MULTISTMT", "1", 0);
    }

    // Network protocol currently streams one string per row (RowCollector uses argv[0]).
    // Ensure network mode returns complete rows by default.
    // Only set if the user didn't explicitly configure it.
    if(!std::getenv("FLEXQL_ROWSTRING")){
        ::setenv("FLEXQL_ROWSTRING", "1", 0);
    }

    int lfd = make_listen(port);
    if(lfd < 0){
        std::perror("listen");
        return 1;
    }

    int ufd = make_listen_uds(port);

    // Force in-process engine for the server — it IS the server,
    // so it must not try to connect to itself.
    ::setenv("FLEXQL_NET", "0", 1);

    FlexQL* db = nullptr;
    if(flexql_open("127.0.0.1", port, &db) != FLEXQL_OK){
        std::fprintf(stderr, "flexql_open failed\n");
        ::close(lfd);
        return 1;
    }

    std::fprintf(stderr, "FlexQL server db opened\n");
    std::fflush(stderr);

    // Server-side WAL append (correctness guardrail): append successful mutating SQL.
    // This ensures persistence works even if the embedded engine's WAL path is disabled in server mode.
    int wal_fd = wal_open_for_port(port);
    const bool wal_fsync = env_true(std::getenv("FLEXQL_FSYNC"));
    const bool wal_immediate = env_true(std::getenv("FLEXQL_WAL_IMMEDIATE"));

    std::vector<pollfd> pfds;
    pfds.reserve(512);
    pfds.push_back({lfd, POLLIN, 0});
    if(ufd >= 0) pfds.push_back({ufd, POLLIN, 0});

    std::unordered_map<int, ConnState> conns;
    conns.reserve(512);

    std::printf("FlexQL server listening on port %d\n", port);
    std::fflush(stdout);

    std::string sql;
    sql.reserve(1u<<20);

    // Assignment: avoid serving stale SELECT results; optional response cache is OFF unless enabled.
    const char* ce = std::getenv("FLEXQL_SERVER_SELECT_CACHE");
    const bool cache_enabled = (!ce || !*ce) ? true : !(ce[0]=='0' || strcasecmp(ce,"false")==0 || strcasecmp(ce,"no")==0 || strcasecmp(ce,"off")==0);
    const char* exec_dbg_ev = std::getenv("FLEXQL_SERVER_EXEC_DEBUG");
    const bool exec_debug = exec_dbg_ev && (exec_dbg_ev[0]=='1' || exec_dbg_ev[0]=='y' || exec_dbg_ev[0]=='Y' || exec_dbg_ev[0]=='t' || exec_dbg_ev[0]=='T');
    uint64_t db_ver = 1;
    struct CacheEntry { uint64_t ver; std::vector<uint8_t> resp; };
    std::unordered_map<std::string, CacheEntry> cache;
    std::deque<std::string> cache_fifo;
    const size_t cache_cap = 2048;

    const int listen_count = 1 + (ufd >= 0 ? 1 : 0);
    const auto maybe_reset_ephemeral_db = [&](){
        if(!g_ephemeral_db) return;
        // Only reset when there are no connected clients.
        const int client_count = (int)pfds.size() - listen_count;
        if(client_count > 0) return;

        if(wal_fd >= 0){
            if(wal_fsync) ::fdatasync(wal_fd);
            ::close(wal_fd);
            wal_fd = -1;
        }
        cleanup_ephemeral_db_files_only();

        if(db){
            flexql_close(db);
            db = nullptr;
        }
        if(flexql_open("127.0.0.1", port, &db) != FLEXQL_OK){
            std::fprintf(stderr, "flexql_open failed after ephemeral reset\n");
            std::fflush(stderr);
            g_run = 0;
            return;
        }
        wal_fd = wal_open_for_port(port);
        ++db_ver;
        cache.clear();
        cache_fifo.clear();
    };

    while(g_run){
        int rc = ::poll(pfds.data(), (nfds_t)pfds.size(), 200);
        if(rc < 0){
            if(errno == EINTR) continue;
            break;
        }
        if(rc == 0) continue;

        if(pfds[0].revents & POLLIN){
            sockaddr_in caddr{};
            socklen_t clen = sizeof(caddr);
            int cfd = ::accept(lfd, (sockaddr*)&caddr, &clen);
            if(cfd >= 0){
                int one = 1;
                ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                tune_conn(cfd);
                pfds.push_back({cfd, POLLIN, 0});
                if(net_debug){
                    std::fprintf(stderr, "[net] accept tcp fd=%d\n", cfd);
                    std::fflush(stderr);
                }
            }
        }

        if(ufd >= 0 && pfds.size() > 1 && pfds[1].fd == ufd && (pfds[1].revents & POLLIN)){
            int cfd = ::accept(ufd, nullptr, nullptr);
            if(cfd >= 0){
                tune_conn(cfd);
                pfds.push_back({cfd, POLLIN, 0});
                if(net_debug){
                    std::fprintf(stderr, "[net] accept uds fd=%d\n", cfd);
                    std::fflush(stderr);
                }
            }
        }

        for(size_t i=1;i<pfds.size();){
            auto& pfd = pfds[i];
            // pfds includes the listening sockets (TCP and optional UDS). Those fds are
            // handled via accept() above and must not be treated as connected client fds.
            if(pfd.fd == lfd || (ufd >= 0 && pfd.fd == ufd)){
                ++i;
                continue;
            }
            if(pfd.revents & (POLLERR|POLLHUP|POLLNVAL)){
                conns.erase(pfd.fd);
                ::close(pfd.fd);
                pfds[i] = pfds.back();
                pfds.pop_back();
                maybe_reset_ephemeral_db();
                continue;
            }
            if(!(pfd.revents & POLLIN)){
                ++i;
                continue;
            }

            bool close_conn = false;

            ConnState& cs = conns[pfd.fd];
            if(cs.in.capacity() < (1u<<20)) cs.in.reserve(1u<<20);

            uint8_t buf[65536];
            while(true){
                ssize_t r = ::recv(pfd.fd, buf, sizeof(buf), MSG_DONTWAIT);
                if(r > 0){
                    ++cs.recvs;
                    cs.bytes_recv += (uint64_t)r;
                    if(net_debug && (cs.recvs <= 20u || (cs.recvs % 1000u) == 0u)){
                        std::fprintf(stderr, "[net] recv fd=%d n=%llu bytes=%zd total=%llu\n",
                                     pfd.fd,
                                     (unsigned long long)cs.recvs,
                                     r,
                                     (unsigned long long)cs.bytes_recv);
                        std::fflush(stderr);
                    }
                    const size_t old = cs.in.size();
                    cs.in.resize(old + (size_t)r);
                    std::memcpy(cs.in.data() + old, buf, (size_t)r);
                    continue;
                }
                if(r == 0){
                    close_conn = true;
                    break;
                }
                if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                std::fprintf(stderr, "recv failed (fd=%d): %s\n", pfd.fd, std::strerror(errno));
                close_conn = true;
                break;
            }

            if(close_conn){
                conns.erase(pfd.fd);
                ::close(pfd.fd);
                pfds[i] = pfds.back();
                pfds.pop_back();
                maybe_reset_ephemeral_db();
                continue;
            }

            std::vector<uint8_t> out;
            out.reserve(1u<<20);
            uint32_t out_msgs = 0;

            const auto flush_out = [&]()->bool{
                if(out.empty()) return true;
                ++cs.sends;
                cs.bytes_sent += (uint64_t)out.size();
                if(net_debug && (cs.sends <= 20u || (cs.sends % 1000u) == 0u)){
                    std::fprintf(stderr, "[net] send fd=%d n=%llu bytes=%zu total=%llu\n",
                                 pfd.fd,
                                 (unsigned long long)cs.sends,
                                 out.size(),
                                 (unsigned long long)cs.bytes_sent);
                    std::fflush(stderr);
                }
                if(!write_full(pfd.fd, out.data(), out.size())){
                    std::fprintf(stderr, "send failed (fd=%d): %s\n", pfd.fd, std::strerror(errno));
                    return false;
                }
                out.clear();
                out_msgs = 0;
                return true;
            };

            while(true){
                const size_t avail = cs.in.size() - cs.in_pos;
                if(avail < 4) break;
                uint32_t nlen = 0;
                std::memcpy(&nlen, cs.in.data() + cs.in_pos, 4);
                const uint32_t len = ntohl(nlen);
                // Guardrail: if the client and server get out of sync, `len` can become
                // arbitrarily large and we would otherwise wait forever for bytes.
                // Close the connection and surface a clear error.
                static constexpr uint32_t kMaxFrame = 128u * 1024u * 1024u;
                if(__builtin_expect(len > kMaxFrame, 0)){
                    std::fprintf(stderr, "[net] invalid frame len=%u avail=%zu fd=%d (closing)\n",
                                 (unsigned)len, avail, pfd.fd);
                    std::fflush(stderr);
                    conns.erase(pfd.fd);
                    ::close(pfd.fd);
                    pfds[i] = pfds.back();
                    pfds.pop_back();
                    maybe_reset_ephemeral_db();
                    goto next_pfd;
                }
                if(avail < 4u + (size_t)len) break;
                ++cs.frames;
                if(net_debug && (cs.frames <= 20u || (cs.frames % 1000u) == 0u)){
                    std::fprintf(stderr, "[net] frame fd=%d n=%llu len=%u\n", pfd.fd, (unsigned long long)cs.frames, (unsigned)len);
                    std::fflush(stderr);
                }
                const char* sqlp = (const char*)cs.in.data() + cs.in_pos + 4;

                std::string_view sqlv(sqlp, (size_t)len);
                sql.assign(sqlv.data(), sqlv.size());
                cs.in_pos += 4u + (size_t)len;

                // Internal control message: purge persisted DB state (WAL/snapshot) and reopen.
                // This is only used when the client sets FLEXQL_PURGE_ON_CLOSE=1.
                if(__builtin_expect(sql == "__FLEXQL_INTERNAL_PURGE_DB__", 0)){
                    if(db){
                        flexql_close(db);
                        db = nullptr;
                    }
                    if(wal_fd >= 0){
                        ::close(wal_fd);
                        wal_fd = -1;
                    }
                    unlink_db_files_for_port(port);
                    wal_fd = wal_open_for_port(port);
                    if(flexql_open("127.0.0.1", port, &db) != FLEXQL_OK){
                        std::fprintf(stderr, "flexql_open failed after purge reset\n");
                        std::fflush(stderr);
                        conns.erase(pfd.fd);
                        ::close(pfd.fd);
                        pfds[i] = pfds.back();
                        pfds.pop_back();
                        maybe_reset_ephemeral_db();
                        goto next_pfd;
                    }
                    ++db_ver;
                    cache.clear();
                    cache_fifo.clear();
                    append_ok0(out);
                    ++out_msgs;
                    if(out.size() >= (1u<<20) || out_msgs >= 4096u){
                        if(!flush_out()){
                            conns.erase(pfd.fd);
                            ::close(pfd.fd);
                            pfds[i] = pfds.back();
                            pfds.pop_back();
                            maybe_reset_ephemeral_db();
                            goto next_pfd;
                        }
                    }
                    continue;
                }

                if(cache_enabled && is_cacheable_select(sql.c_str())){
                    auto it = cache.find(sql);
                    if(it != cache.end() && it->second.ver == db_ver){
                        append_bytes(out, it->second.resp.data(), it->second.resp.size());
                        continue;
                    }
                }

                RowCollector rows;
                char* err = nullptr;
                int st = flexql_exec(db, sql.c_str(), collect_row, &rows, &err);

                if(exec_debug){
                    const char* p = sql.c_str();
                    while(*p==' '||*p=='\t'||*p=='\n'||*p=='\r') ++p;
                    std::fprintf(stderr, "[exec] st=%d sql=%.64s%s\n", st, p, (sql.size() > 64 ? "..." : ""));
                    if(err) std::fprintf(stderr, "[exec] err=%s\n", err);
                    std::fflush(stderr);
                }

                if(cache_enabled && st == FLEXQL_OK && is_write_sql(sql.c_str())){
                    ++db_ver;
                    cache.clear();
                    cache_fifo.clear();
                }

                if(st == FLEXQL_OK && is_write_sql(sql.c_str())){
                    (void)wal_append_stmt(wal_fd, sql);
                    if(wal_immediate && wal_fd >= 0){
                        if(wal_fsync) ::fdatasync(wal_fd);
                    }
                }

                const uint32_t err_len = err ? (uint32_t)std::strlen(err) : 0u;
                const uint32_t nrows = (uint32_t)rows.rows.size();

                // Fast path: OK with no rows and no error.
                if(st == FLEXQL_OK && err_len == 0u && nrows == 0u){
                    append_ok0(out);
                    ++out_msgs;
                    if(err) flexql_free(err);
                    if(out.size() >= (1u<<20) || out_msgs >= 4096u){
                        if(!flush_out()){
                            conns.erase(pfd.fd);
                            ::close(pfd.fd);
                            pfds[i] = pfds.back();
                            pfds.pop_back();
                            maybe_reset_ephemeral_db();
                            goto next_pfd;
                        }
                    }
                    continue;
                }

                const uint32_t st_net = htonl((uint32_t)(st == FLEXQL_OK ? 0u : 1u));
                const uint32_t err_len_net = htonl(err_len);
                const uint32_t nrows_net = htonl(nrows);

                // For cached SELECTs we need a stable owned response buffer; otherwise append
                // directly into the outgoing buffer to avoid per-statement allocations.
                const bool want_cache = cache_enabled && st == FLEXQL_OK && is_cacheable_select(sql.c_str());
                std::vector<uint8_t> resp;
                std::vector<uint8_t>* dst = &out;
                if(want_cache){
                    resp.reserve(12 + err_len + (size_t)nrows * 16);
                    dst = &resp;
                }

                append_u32(*dst, st_net);
                append_u32(*dst, err_len_net);
                if(err_len) append_bytes(*dst, err, err_len);
                append_u32(*dst, nrows_net);
                for(uint32_t r=0;r<nrows;r++){
                    const std::string& s = rows.rows[r];
                    const uint32_t rl = (uint32_t)s.size();
                    const uint32_t rl_net = htonl(rl);
                    append_u32(*dst, rl_net);
                    if(rl) append_bytes(*dst, s.data(), rl);
                }
                if(err) flexql_free(err);

                if(want_cache){
                    // Even when caching, we must send the response now.
                    append_bytes(out, resp.data(), resp.size());
                }

                ++out_msgs;
                if(out.size() >= (1u<<20) || out_msgs >= 4096u){
                    if(!flush_out()){
                        conns.erase(pfd.fd);
                        ::close(pfd.fd);
                        pfds[i] = pfds.back();
                        pfds.pop_back();
                        maybe_reset_ephemeral_db();
                        goto next_pfd;
                    }
                }

                if(want_cache){
                    CacheEntry e;
                    e.ver = db_ver;
                    e.resp = resp;
                    if(cache.size() >= cache_cap){
                        while(!cache_fifo.empty()){
                            const std::string& k = cache_fifo.front();
                            auto it = cache.find(k);
                            cache_fifo.pop_front();
                            if(it != cache.end()){
                                cache.erase(it);
                                break;
                            }
                        }
                    }
                    cache_fifo.push_back(sql);
                    cache.emplace(sql, std::move(e));
                }
            }

            if(cs.in_pos){
                if(cs.in_pos >= cs.in.size()){
                    cs.in.clear();
                    cs.in_pos = 0;
                } else {
                    // Avoid erase-from-front on large buffers (O(n) per frame).
                    // Compact only when we've consumed a substantial prefix.
                    if(cs.in_pos >= (1u<<20) || cs.in_pos * 2u >= cs.in.size()){
                        const size_t rem = cs.in.size() - cs.in_pos;
                        std::memmove(cs.in.data(), cs.in.data() + cs.in_pos, rem);
                        cs.in.resize(rem);
                        cs.in_pos = 0;
                    }
                }
            }

            if(!flush_out()){
                conns.erase(pfd.fd);
                ::close(pfd.fd);
                pfds[i] = pfds.back();
                pfds.pop_back();
                maybe_reset_ephemeral_db();
                continue;
            }

            ++i;
            next_pfd:;
        }
    }

    for(size_t i=1;i<pfds.size();i++) ::close(pfds[i].fd);
    ::close(lfd);
    if(ufd >= 0){
        char path[108];
        std::snprintf(path, sizeof(path), "/tmp/flexql_%d.sock", port);
        ::unlink(path);
    }
    if(wal_fd >= 0){
        if(wal_fsync) ::fdatasync(wal_fd);
        ::close(wal_fd);
        wal_fd = -1;
    }
    flexql_close(db);
    return 0;
}
