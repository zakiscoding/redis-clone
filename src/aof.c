#include "aof.h"

#include "client.h"
#include "commands.h"
#include "platform.h"
#include "server.h"

#include <stdatomic.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef _WIN32
#include <io.h>
#include <process.h>
#else
#include <pthread.h>
#include <unistd.h>
#endif

static int enabled;
static int sync_mode = AOF_SYNC_EVERYSEC;
static int fd = -1;
static int loading;
static int last_ok = 1;
static int thread_started;
static int lock_ready;
static unsigned long long write_gen;
static unsigned long long synced_gen;
static unsigned long long sync_count;
static long long current_size;
static char path_storage[1400];
static atomic_int stop_thread;

#ifdef _WIN32
static CRITICAL_SECTION lock;
static HANDLE thread_handle;
#else
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t thread_handle;
#endif

static void aof_lock(void) {
    if (!lock_ready) return;
#ifdef _WIN32
    EnterCriticalSection(&lock);
#else
    pthread_mutex_lock(&lock);
#endif
}

static void aof_unlock(void) {
    if (!lock_ready) return;
#ifdef _WIN32
    LeaveCriticalSection(&lock);
#else
    pthread_mutex_unlock(&lock);
#endif
}

static int write_full(int file, const char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        size_t chunk = len - off;
        if (chunk > 1048576u) chunk = 1048576u;
#ifdef _WIN32
        int n = _write(file, buf + off, (unsigned int)chunk);
#else
        ssize_t n = write(file, buf + off, chunk);
#endif
        if (n < 0) {
#ifndef _WIN32
            if (errno == EINTR) continue;
#endif
            return -1;
        }
        if (n == 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

static int sync_file(void) {
    if (durable_datasync(fd) != 0) {
        last_ok = 0;
        return -1;
    }
    synced_gen = write_gen;
    sync_count++;
    last_ok = 1;
    return 0;
}

static void thread_loop(void) {
    while (!atomic_load(&stop_thread)) {
        for (int i = 0; i < 10 && !atomic_load(&stop_thread); i++) sleep_ms(100);
        if (atomic_load(&stop_thread)) break;
        aof_lock();
        if (fd >= 0 && write_gen != synced_gen) sync_file();
        aof_unlock();
    }
}

#ifdef _WIN32
static DWORD WINAPI thread_main(LPVOID arg) {
    (void)arg;
    thread_loop();
    return 0;
}
#else
static void *thread_main(void *arg) {
    (void)arg;
    thread_loop();
    return NULL;
}
#endif

const char *aof_sync_policy_name(void) {
    if (sync_mode == AOF_SYNC_ALWAYS) return "always";
    if (sync_mode == AOF_SYNC_NO) return "no";
    return "everysec";
}

int aof_is_enabled(void) { return enabled; }
unsigned long long aof_sync_count(void) { return sync_count; }
long long aof_current_size(void) { return current_size; }
int aof_last_status_ok(void) { return last_ok; }
const char *aof_path_string(void) { return path_storage; }

int aof_init(const char *path, int turn_on, int mode) {
    sync_mode = mode;
    enabled = turn_on ? 1 : 0;
    if (!enabled) return 0;
    if (!path || strlen(path) >= sizeof(path_storage)) {
        fprintf(stderr, "AOF path is missing or too long\n");
        return -1;
    }
    memcpy(path_storage, path, strlen(path) + 1);
#ifdef _WIN32
    InitializeCriticalSection(&lock);
    lock_ready = 1;
#else
    lock_ready = 1;
#endif
    fd = open(path_storage, O_RDWR | O_CREAT | O_APPEND | O_BINARY | O_CLOEXEC, 0644);
    if (fd < 0) {
        perror("open appendonly file");
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) == 0) current_size = (long long)st.st_size;
    aof_lock();
    sync_file();
    aof_unlock();
    return 0;
}

void aof_start_background(void) {
    if (!enabled || sync_mode != AOF_SYNC_EVERYSEC || thread_started) return;
    atomic_store(&stop_thread, 0);
#ifdef _WIN32
    thread_handle = CreateThread(NULL, 0, thread_main, NULL, 0, NULL);
    if (!thread_handle) {
        fprintf(stderr, "CreateThread for AOF sync failed\n");
        return;
    }
#else
    if (pthread_create(&thread_handle, NULL, thread_main, NULL) != 0) {
        perror("pthread_create");
        return;
    }
#endif
    thread_started = 1;
}

static char *encode_resp(char **args, const size_t *lens, size_t count, size_t *out_len) {
    size_t total = 24;
    for (size_t i = 0; i < count; i++) {
        size_t n = lens ? lens[i] : strlen(args[i]);
        if (total > SIZE_MAX - n - 32) return NULL;
        total += n + 32;
    }
    char *buf = malloc(total);
    if (!buf) return NULL;
    int header = snprintf(buf, total, "*%zu\r\n", count);
    if (header < 0 || (size_t)header >= total) {
        free(buf);
        return NULL;
    }
    size_t used = (size_t)header;
    for (size_t i = 0; i < count; i++) {
        size_t n = lens ? lens[i] : strlen(args[i]);
        int prefix = snprintf(buf + used, total - used, "$%zu\r\n", n);
        if (prefix < 0 || (size_t)prefix >= total - used) {
            free(buf);
            return NULL;
        }
        used += (size_t)prefix;
        if (n > 0) memcpy(buf + used, args[i], n);
        used += n;
        memcpy(buf + used, "\r\n", 2);
        used += 2;
    }
    *out_len = used;
    return buf;
}

void aof_log(char **args, const size_t *lens, size_t count) {
    if (!enabled || loading || fd < 0 || count == 0) return;
    size_t len = 0;
    char *buf = encode_resp(args, lens, count, &len);
    if (!buf) {
        last_ok = 0;
        fprintf(stderr, "AOF encode failed\n");
        return;
    }
    aof_lock();
    if (write_full(fd, buf, len) != 0) {
        last_ok = 0;
        fprintf(stderr, "AOF write failed\n");
        aof_unlock();
        free(buf);
        return;
    }
    current_size += (long long)len;
    write_gen++;
    if (sync_mode == AOF_SYNC_ALWAYS) sync_file();
    aof_unlock();
    free(buf);
}

static int truncate_to(long long size) {
#ifdef _WIN32
    if (_chsize_s(fd, size) != 0) return -1;
#else
    if (ftruncate(fd, (off_t)size) != 0) return -1;
#endif
    current_size = size;
    return 0;
}

int aof_load(void) {
    if (!enabled || fd < 0) return 0;
#ifdef _WIN32
    if (_lseeki64(fd, 0, SEEK_SET) < 0) {
#else
    if (lseek(fd, 0, SEEK_SET) < 0) {
#endif
        perror("lseek appendonly file");
        return -1;
    }
    size_t cap = 65536;
    char *buf = malloc(cap);
    if (!buf) return -1;
    size_t len = 0;
    long long base = 0;
    loading = 1;
    Client replay;
    memset(&replay, 0, sizeof(replay));
    replay.replay = 1;
    replay.active = 1;
    replay.fd = SOCK_INVALID;

    for (;;) {
        if (len == cap) {
            if (cap >= (size_t)MAX_VALUE * 2 + 65536) {
                fprintf(stderr, "AOF command is too large\n");
                loading = 0;
                free(buf);
                return -1;
            }
            size_t grown = cap * 2;
            char *next = realloc(buf, grown);
            if (!next) {
                loading = 0;
                free(buf);
                return -1;
            }
            buf = next;
            cap = grown;
        }
#ifdef _WIN32
        int n = _read(fd, buf + len, (unsigned int)(cap - len > 1048576 ? 1048576 : cap - len));
#else
        ssize_t n = read(fd, buf + len, cap - len > 1048576 ? 1048576 : cap - len);
        if (n < 0 && errno == EINTR) continue;
#endif
        if (n < 0) {
            perror("read appendonly file");
            loading = 0;
            free(buf);
            return -1;
        }
        if (n == 0) break;
        len += (size_t)n;
        for (;;) {
            char **args = NULL;
            size_t *lens = NULL;
            size_t count = 0;
            size_t consumed = 0;
            int status = resp_parse(buf, len, 1, &args, &lens, &count, &consumed);
            if (status == RESP_NEED) break;
            if (status != RESP_OK) {
                fprintf(stderr, "AOF protocol error at offset %lld\n", base);
                free_command(args, lens, count);
                loading = 0;
                client_free_transaction_state(&replay);
                client_free_buffers(&replay);
                free(buf);
                return -1;
            }
            command(&replay, args, lens, count);
            free_command(args, lens, count);
            memmove(buf, buf + consumed, len - consumed);
            len -= consumed;
            base += (long long)consumed;
        }
    }

    int rc = 0;
    if (len > 0) {
        size_t consumed = 0;
        int status = resp_parse(buf, len, 0, NULL, NULL, NULL, &consumed);
        if (status == RESP_NEED) {
            fprintf(stderr, "warning: truncated %zu-byte incomplete AOF tail at offset %lld\n", len, base);
            if (truncate_to(base) != 0) {
                perror("truncate appendonly file");
                rc = -1;
            }
        } else {
            fprintf(stderr, "AOF protocol error at offset %lld\n", base);
            rc = -1;
        }
    }
    loading = 0;
    client_free_transaction_state(&replay);
    client_free_buffers(&replay);
    free(buf);
    return rc;
}

void aof_shutdown(void) {
    if (!enabled && fd < 0) return;
    atomic_store(&stop_thread, 1);
    if (thread_started) {
#ifdef _WIN32
        WaitForSingleObject(thread_handle, INFINITE);
        CloseHandle(thread_handle);
#else
        pthread_join(thread_handle, NULL);
#endif
        thread_started = 0;
    }
    if (fd >= 0) {
        aof_lock();
        sync_file();
        aof_unlock();
#ifdef _WIN32
        _close(fd);
#else
        close(fd);
#endif
        fd = -1;
    }
#ifdef _WIN32
    if (lock_ready) DeleteCriticalSection(&lock);
#endif
    lock_ready = 0;
    enabled = 0;
}
