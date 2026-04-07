#pragma once
#ifdef __cplusplus
extern "C" {
#endif

typedef struct FlexQL FlexQL;

#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

int  flexql_open(const char *host, int port, FlexQL **db);
int  flexql_close(FlexQL *db);
int  flexql_exec(FlexQL *db, const char *sql,
                 int (*callback)(void*, int, char**, char**),
                 void *arg, char **errmsg);
int  flexql_exec_many(FlexQL *db, const char *const* sqls, int n,
                      int (*callback)(void*, int, char**, char**),
                      void *arg, char **errmsg);
int  flexql_is_uds(FlexQL *db);
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif
