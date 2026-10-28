#ifndef SERVER_H
#define SERVER_H

#include <signal.h>
#include "platform.h"

enum { IO_SELECT = 0, IO_EPOLL = 1 };

extern volatile sig_atomic_t g_stop;
extern int g_port;
extern int g_io_mode;
extern char g_dir[1024];
extern char g_dbfilename[256];
extern char g_aof_filename[256];
extern int g_appendonly;
extern int g_aof_sync;
extern long long g_repl_offset;
extern const char g_replid[];

#endif
