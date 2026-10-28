#ifndef AOF_H
#define AOF_H

#include <stddef.h>

enum { AOF_SYNC_NO = 0, AOF_SYNC_EVERYSEC = 1, AOF_SYNC_ALWAYS = 2 };

int aof_init(const char *path, int enabled, int sync_mode);
int aof_load(void);
void aof_start_background(void);
void aof_shutdown(void);
void aof_log(char **args, const size_t *lens, size_t count);

int aof_is_enabled(void);
const char *aof_sync_policy_name(void);
unsigned long long aof_sync_count(void);
long long aof_current_size(void);
int aof_last_status_ok(void);
const char *aof_path_string(void);

#endif
