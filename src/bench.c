#include "platform.h"

#include <stdatomic.h>

#ifdef _WIN32
#include <process.h>
#include <psapi.h>
#else
#include <pthread.h>
#endif

typedef struct {
    double cpu_sec;
    unsigned long long rss_bytes;
} ProcSample;

typedef struct {
    int id;
    int port;
    int ops;
    int pipeline;
    int datasize;
    int mode_get;
    const char *value;
    uint32_t *samples;
} Worker;

static atomic_int ready_threads;
static atomic_int start_flag;

static int sample_process(unsigned long pid, ProcSample *out) {
    if (pid == 0) return -1;
#ifdef _WIN32
    HANDLE process = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, (DWORD)pid);
    if (!process) return -1;
    FILETIME created, exited, kernel, user;
    if (!GetProcessTimes(process, &created, &exited, &kernel, &user)) {
        CloseHandle(process);
        return -1;
    }
    ULARGE_INTEGER kernel_time, user_time;
    kernel_time.LowPart = kernel.dwLowDateTime;
    kernel_time.HighPart = kernel.dwHighDateTime;
    user_time.LowPart = user.dwLowDateTime;
    user_time.HighPart = user.dwHighDateTime;
    out->cpu_sec = (double)(kernel_time.QuadPart + user_time.QuadPart) / 10000000.0;
    PROCESS_MEMORY_COUNTERS counters;
    memset(&counters, 0, sizeof(counters));
    counters.cb = sizeof(counters);
    if (!GetProcessMemoryInfo(process, &counters, sizeof(counters))) {
        CloseHandle(process);
        return -1;
    }
    out->rss_bytes = counters.WorkingSetSize;
    CloseHandle(process);
    return 0;
#else
    char path[64];
    snprintf(path, sizeof(path), "/proc/%lu/stat", pid);
    FILE *stat_file = fopen(path, "r");
    if (!stat_file) return -1;
    char stat_buf[1024];
    if (!fgets(stat_buf, sizeof(stat_buf), stat_file)) {
        fclose(stat_file);
        return -1;
    }
    fclose(stat_file);
    char *rest = strrchr(stat_buf, ')');
    if (!rest || rest[1] == '\0') return -1;
    unsigned long utime = 0, stime = 0;
    int scanned = sscanf(rest + 2,
                         "%*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu",
                         &utime, &stime);
    if (scanned != 2) return -1;
    long ticks = sysconf(_SC_CLK_TCK);
    if (ticks <= 0) ticks = 100;
    out->cpu_sec = (double)(utime + stime) / (double)ticks;
    snprintf(path, sizeof(path), "/proc/%lu/status", pid);
    FILE *status = fopen(path, "r");
    if (!status) return -1;
    char line[256];
    int found = 0;
    while (fgets(line, sizeof(line), status)) {
        unsigned long rss_kb = 0;
        if (sscanf(line, "VmRSS: %lu", &rss_kb) == 1) {
            out->rss_bytes = rss_kb * 1024ULL;
            found = 1;
            break;
        }
    }
    fclose(status);
    return found ? 0 : -1;
#endif
}

static void fail(const char *message) {
    fprintf(stderr, "bench: %s\n", message);
    exit(1);
}

static socket_t connect_server(int port) {
    socket_t fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == SOCK_INVALID) fail("socket");
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    address.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) < 0) fail("connect");
    socket_set_nodelay(fd);
#ifdef _WIN32
    DWORD timeout = 30000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&timeout, sizeof(timeout));
#else
    struct timeval timeout;
    memset(&timeout, 0, sizeof(timeout));
    timeout.tv_sec = 30;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif
    return fd;
}

static void send_full(socket_t fd, const char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        int n = socket_send(fd, buf + off, len - off);
        if (n > 0) {
            off += (size_t)n;
            continue;
        }
        int err = socket_last_error();
        if (n < 0 && socket_is_intr(err)) continue;
        fail("send");
    }
}

static void recv_full(socket_t fd, char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        int n = socket_recv(fd, buf + off, len - off);
        if (n > 0) {
            off += (size_t)n;
            continue;
        }
        int err = socket_last_error();
        if (n < 0 && socket_is_intr(err)) continue;
        fail("recv");
    }
}

static size_t append_bulk(char *dst, size_t used, size_t cap, const char *data, size_t len) {
    int prefix = snprintf(dst + used, cap - used, "$%zu\r\n", len);
    if (prefix < 0 || (size_t)prefix >= cap - used) return 0;
    used += (size_t)prefix;
    if (used + len + 2 > cap) return 0;
    if (len > 0) memcpy(dst + used, data, len);
    used += len;
    memcpy(dst + used, "\r\n", 2);
    return used + 2;
}

static size_t build_command(char *dst, size_t cap, int mode_get, const char *key, size_t key_len,
                            const char *value, size_t value_len) {
    const char *name = mode_get ? "GET" : "SET";
    int argc = mode_get ? 2 : 3;
    int header = snprintf(dst, cap, "*%d\r\n$%zu\r\n%s\r\n", argc, strlen(name), name);
    if (header < 0 || (size_t)header >= cap) return 0;
    size_t used = append_bulk(dst, (size_t)header, cap, key, key_len);
    if (used == 0) return 0;
    if (!mode_get) used = append_bulk(dst, used, cap, value, value_len);
    return used;
}

static void check_replies(const char *buf, size_t len, int mode_get, int count, int datasize) {
    if (mode_get) {
        char header[32];
        int header_len = snprintf(header, sizeof(header), "$%d\r\n", datasize);
        size_t one = (size_t)header_len + (size_t)datasize + 2;
        if ((size_t)header_len <= 0 || one * (size_t)count != len) fail("unexpected GET reply size");
        for (int i = 0; i < count; i++) {
            if (memcmp(buf + one * (size_t)i, header, (size_t)header_len) != 0) fail("unexpected GET reply");
        }
        return;
    }
    if (len != (size_t)count * 5) fail("unexpected SET reply size");
    for (int i = 0; i < count; i++) {
        if (memcmp(buf + (size_t)i * 5, "+OK\r\n", 5) != 0) fail("unexpected SET reply");
    }
}

static size_t reply_bytes(int mode_get, int count, int datasize) {
    if (!mode_get) return (size_t)count * 5;
    char header[32];
    int header_len = snprintf(header, sizeof(header), "$%d\r\n", datasize);
    return ((size_t)header_len + (size_t)datasize + 2) * (size_t)count;
}

static void run_ops(socket_t fd, Worker *worker, int ops, int record) {
    size_t command_cap = (size_t)worker->pipeline * (64u + 16u + (size_t)worker->datasize + 32u);
    char *command = malloc(command_cap);
    size_t reply_cap = reply_bytes(worker->mode_get, worker->pipeline, worker->datasize);
    char *reply = malloc(reply_cap);
    if (!command || !reply) fail("out of memory");
    for (int done = 0; done < ops;) {
        int batch = worker->pipeline;
        if (done + batch > ops) batch = ops - done;
        size_t used = 0;
        for (int i = 0; i < batch; i++) {
            char key[16];
            int key_len = snprintf(key, sizeof(key), "k%04d", (done + i + worker->id * 17) % 256);
            size_t wrote = build_command(command + used, command_cap - used, worker->mode_get, key,
                                         (size_t)key_len, worker->value, (size_t)worker->datasize);
            if (wrote == 0) fail("command buffer");
            used += wrote;
        }
        uint64_t started = mono_ns();
        send_full(fd, command, used);
        size_t expect = reply_bytes(worker->mode_get, batch, worker->datasize);
        recv_full(fd, reply, expect);
        check_replies(reply, expect, worker->mode_get, batch, worker->datasize);
        uint64_t each = (mono_ns() - started) / 1000u / (uint64_t)batch;
        if (each > UINT32_MAX) each = UINT32_MAX;
        if (record) {
            for (int i = 0; i < batch; i++) worker->samples[done + i] = (uint32_t)each;
        }
        done += batch;
    }
    free(command);
    free(reply);
}

static void worker_main(Worker *worker) {
    socket_t fd = connect_server(worker->port);
    run_ops(fd, worker, 8, 0);
    atomic_fetch_add(&ready_threads, 1);
    while (atomic_load(&start_flag) == 0) sleep_ms(1);
    run_ops(fd, worker, worker->ops, 1);
    socket_close(fd);
}

#ifdef _WIN32
static unsigned __stdcall thread_start(void *arg) {
    worker_main(arg);
    return 0;
}
#else
static void *thread_start(void *arg) {
    worker_main(arg);
    return NULL;
}
#endif

static int cmp_u32(const void *left, const void *right) {
    uint32_t a = *(const uint32_t *)left;
    uint32_t b = *(const uint32_t *)right;
    return (a > b) - (a < b);
}

static uint32_t percentile(uint32_t *samples, size_t count, int percent) {
    if (count == 0) return 0;
    size_t index = (count * (size_t)percent + 99) / 100;
    if (index == 0) index = 1;
    index--;
    if (index >= count) index = count - 1;
    return samples[index];
}

static void preload(int port, const char *value, int datasize) {
    socket_t fd = connect_server(port);
    char command[128 + 4096];
    char reply[64];
    for (int i = 0; i < 256; i++) {
        char key[16];
        int key_len = snprintf(key, sizeof(key), "k%04d", i);
        size_t used = build_command(command, sizeof(command), 0, key, (size_t)key_len, value, (size_t)datasize);
        if (used == 0) fail("preload command");
        send_full(fd, command, used);
        recv_full(fd, reply, 5);
        if (memcmp(reply, "+OK\r\n", 5) != 0) fail("preload");
    }
    socket_close(fd);
}

static void usage(void) {
    fprintf(stderr,
            "usage: redis-bench --port N [--clients N] [--requests N] [--pipeline N]\n"
            "                   [--datasize N] [--mode set|get] [--server-pid N]\n");
}

int main(int argc, char **argv) {
    int port = 6379;
    int clients = 1;
    int requests = 1000;
    int pipeline = 1;
    int datasize = 32;
    int mode_get = 0;
    unsigned long pid = 0;
    for (int i = 1; i + 1 < argc; i += 2) {
        const char *value = argv[i + 1];
        if (strcmp(argv[i], "--port") == 0) port = atoi(value);
        else if (strcmp(argv[i], "--clients") == 0) clients = atoi(value);
        else if (strcmp(argv[i], "--requests") == 0) requests = atoi(value);
        else if (strcmp(argv[i], "--pipeline") == 0) pipeline = atoi(value);
        else if (strcmp(argv[i], "--datasize") == 0) datasize = atoi(value);
        else if (strcmp(argv[i], "--server-pid") == 0) pid = strtoul(value, NULL, 10);
        else if (strcmp(argv[i], "--mode") == 0) {
            if (strcmp(value, "get") == 0) mode_get = 1;
            else if (strcmp(value, "set") == 0) mode_get = 0;
            else {
                usage();
                return 2;
            }
        } else {
            usage();
            return 2;
        }
    }
    if (port <= 0 || clients < 1 || clients > 2000 || requests < 1 || pipeline < 1 || pipeline > 256 ||
        datasize < 1 || datasize > 4096) {
        usage();
        return 2;
    }
    if (platform_init() != 0) fail("network startup");
    int per_client = requests / clients;
    if (per_client < 1) per_client = 1;
    char *value = malloc((size_t)datasize);
    if (!value) fail("out of memory");
    memset(value, 'x', (size_t)datasize);
    if (mode_get) preload(port, value, datasize);

    Worker *workers = calloc((size_t)clients, sizeof(*workers));
    if (!workers) fail("out of memory");
#ifdef _WIN32
    uintptr_t *threads = calloc((size_t)clients, sizeof(*threads));
#else
    pthread_t *threads = calloc((size_t)clients, sizeof(*threads));
#endif
    if (!threads) fail("out of memory");
    for (int i = 0; i < clients; i++) {
        workers[i].id = i;
        workers[i].port = port;
        workers[i].ops = per_client;
        workers[i].pipeline = pipeline;
        workers[i].datasize = datasize;
        workers[i].mode_get = mode_get;
        workers[i].value = value;
        workers[i].samples = calloc((size_t)per_client, sizeof(uint32_t));
        if (!workers[i].samples) fail("out of memory");
#ifdef _WIN32
        threads[i] = _beginthreadex(NULL, 0, thread_start, &workers[i], 0, NULL);
        if (threads[i] == 0) fail("thread");
#else
        if (pthread_create(&threads[i], NULL, thread_start, &workers[i]) != 0) fail("thread");
#endif
    }
    while (atomic_load(&ready_threads) < clients) sleep_ms(1);
    ProcSample before = {0}, after = {0};
    int sampled = sample_process(pid, &before) == 0;
    uint64_t started = mono_ns();
    atomic_store(&start_flag, 1);
    for (int i = 0; i < clients; i++) {
#ifdef _WIN32
        WaitForSingleObject((HANDLE)threads[i], INFINITE);
        CloseHandle((HANDLE)threads[i]);
#else
        pthread_join(threads[i], NULL);
#endif
    }
    double elapsed = (double)(mono_ns() - started) / 1e9;
    if (sampled) sample_process(pid, &after);
    size_t total = (size_t)per_client * (size_t)clients;
    uint32_t *all = malloc(total * sizeof(uint32_t));
    if (!all) fail("out of memory");
    size_t cursor = 0;
    for (int i = 0; i < clients; i++) {
        memcpy(all + cursor, workers[i].samples, (size_t)per_client * sizeof(uint32_t));
        cursor += (size_t)per_client;
        free(workers[i].samples);
    }
    qsort(all, total, sizeof(uint32_t), cmp_u32);
    double ops = elapsed > 0 ? (double)total / elapsed : 0;
    printf("result clients=%d pipeline=%d requests=%zu elapsed_s=%.4f ops_per_s=%.1f p50_us=%u p99_us=%u ",
           clients, pipeline, total, elapsed, ops, percentile(all, total, 50), percentile(all, total, 99));
    if (sampled && elapsed > 0) {
        double cpu = (after.cpu_sec - before.cpu_sec) / elapsed * 100.0;
        if (cpu < 0) cpu = 0;
        printf("rss_kb=%llu cpu_pct=%.1f\n", after.rss_bytes / 1024ULL, cpu);
    } else {
        printf("rss_kb=na cpu_pct=na\n");
    }
    free(all);
    free(workers);
    free(threads);
    free(value);
    platform_cleanup();
    return 0;
}
