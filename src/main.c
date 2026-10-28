#include "aof.h"
#include "loop.h"
#include "platform.h"
#include "server.h"
#include "store.h"

volatile sig_atomic_t g_stop;
int g_port = 6379;
int g_io_mode = IO_SELECT;
char g_dir[1024] = ".";
char g_dbfilename[256] = "dump.rdb";
char g_aof_filename[256] = "appendonly.aof";
int g_appendonly;
int g_aof_sync = AOF_SYNC_EVERYSEC;
long long g_repl_offset;
const char g_replid[] = "8371b4fb1155b71f4a04d3e1bc3e18c8";

static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
}

#ifdef _WIN32
static BOOL WINAPI on_console(DWORD type) {
    (void)type;
    g_stop = 1;
    return TRUE;
}
#endif

static void usage(void) {
    fprintf(stderr,
            "usage: redis [--port N] [--io select|epoll] [--appendonly yes|no]\n"
            "             [--appendfilename name] [--appendfsync always|everysec|no]\n"
            "             [--dir path] [--dbfilename name]\n");
}

static int copy_arg(char *dst, size_t dst_len, const char *src) {
    size_t n = strlen(src);
    if (n >= dst_len) return -1;
    memcpy(dst, src, n + 1);
    return 0;
}

static int parse_args(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        const char *option = argv[i];
        const char *value = (i + 1 < argc) ? argv[i + 1] : NULL;
        if (strcmp(option, "--port") == 0 && value) {
            char *end = NULL;
            long port = strtol(value, &end, 10);
            if (!end || *end != '\0' || port <= 0 || port > 65535) {
                fprintf(stderr, "invalid port\n");
                return -1;
            }
            g_port = (int)port;
            i++;
        } else if (strcmp(option, "--io") == 0 && value) {
            if (strcmp(value, "select") == 0) g_io_mode = IO_SELECT;
            else if (strcmp(value, "epoll") == 0) g_io_mode = IO_EPOLL;
            else {
                fprintf(stderr, "invalid --io (expected select or epoll)\n");
                return -1;
            }
            i++;
        } else if (strcmp(option, "--appendonly") == 0 && value) {
            if (strcmp(value, "yes") == 0) g_appendonly = 1;
            else if (strcmp(value, "no") == 0) g_appendonly = 0;
            else {
                fprintf(stderr, "invalid --appendonly (expected yes or no)\n");
                return -1;
            }
            i++;
        } else if (strcmp(option, "--appendfsync") == 0 && value) {
            if (strcmp(value, "always") == 0) g_aof_sync = AOF_SYNC_ALWAYS;
            else if (strcmp(value, "everysec") == 0) g_aof_sync = AOF_SYNC_EVERYSEC;
            else if (strcmp(value, "no") == 0) g_aof_sync = AOF_SYNC_NO;
            else {
                fprintf(stderr, "invalid --appendfsync (expected always, everysec, or no)\n");
                return -1;
            }
            i++;
        } else if (strcmp(option, "--appendfilename") == 0 && value) {
            if (copy_arg(g_aof_filename, sizeof(g_aof_filename), value) != 0) return -1;
            i++;
        } else if (strcmp(option, "--dir") == 0 && value) {
            if (copy_arg(g_dir, sizeof(g_dir), value) != 0) return -1;
            i++;
        } else if (strcmp(option, "--dbfilename") == 0 && value) {
            if (copy_arg(g_dbfilename, sizeof(g_dbfilename), value) != 0) return -1;
            i++;
        } else if (strncmp(option, "--", 2) == 0) {
            fprintf(stderr, "ignoring unknown option %s\n", option);
            if (value && strncmp(value, "--", 2) != 0) i++;
        } else {
            fprintf(stderr, "unexpected argument %s\n", option);
            return -1;
        }
    }
    return 0;
}

int main(int argc, char **argv) {
    if (platform_init() != 0) {
        fprintf(stderr, "network startup failed\n");
        return 1;
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
#ifdef _WIN32
    SetConsoleCtrlHandler(on_console, TRUE);
#endif
    if (parse_args(argc, argv) != 0) {
        usage();
        platform_cleanup();
        return 2;
    }
#ifndef __linux__
    if (g_io_mode == IO_EPOLL) {
        fprintf(stderr, "epoll is only available on Linux\n");
        platform_cleanup();
        return 1;
    }
#endif
    char path[1400];
    int wrote = snprintf(path, sizeof(path), "%s/%s", g_dir, g_aof_filename);
    if (wrote < 0 || (size_t)wrote >= sizeof(path)) {
        fprintf(stderr, "AOF path is too long\n");
        platform_cleanup();
        return 1;
    }
    if (aof_init(path, g_appendonly, g_aof_sync) != 0) {
        aof_shutdown();
        platform_cleanup();
        return 1;
    }
    if (aof_load() != 0) {
        aof_shutdown();
        store_shutdown();
        platform_cleanup();
        return 1;
    }
    aof_start_background();
    socket_t listener = server_listen(g_port);
    if (listener == SOCK_INVALID) {
        aof_shutdown();
        store_shutdown();
        platform_cleanup();
        return 1;
    }
    setbuf(stdout, NULL);
    printf("Redis clone listening on port %d\n", g_port);
    fprintf(stderr, "io=%s appendonly=%s appendfsync=%s\n",
            g_io_mode == IO_EPOLL ? "epoll" : "select",
            g_appendonly ? "yes" : "no",
            aof_sync_policy_name());
    fflush(stderr);
    run_loop(listener);
    aof_shutdown();
    store_shutdown();
    socket_close(listener);
    platform_cleanup();
    return 0;
}
