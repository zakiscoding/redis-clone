#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define PORT 6379
#define MAX_CLIENTS FD_SETSIZE
#define MAX_KEYS 1024
#define MAX_ARGS 64
#define MAX_VALUE 1024 * 1024

typedef enum { VALUE_STRING, VALUE_LIST } ValueType;

typedef struct {
    char *data;
    size_t length;
} Item;

typedef struct {
    char *key;
    ValueType type;
    char *string;
    Item *items;
    size_t count;
    size_t capacity;
    int64_t expires_at_ms;
    int used;
} Entry;

static Entry store[MAX_KEYS];

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void free_entry(Entry *entry) {
    if (!entry->used) return;
    free(entry->key);
    free(entry->string);
    for (size_t i = 0; i < entry->count; i++) free(entry->items[i].data);
    free(entry->items);
    memset(entry, 0, sizeof(*entry));
}

static Entry *find_entry(const char *key) {
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (!store[i].used || strcmp(store[i].key, key) != 0) continue;
        if (store[i].expires_at_ms > 0 && store[i].expires_at_ms <= now_ms()) {
            free_entry(&store[i]);
            return NULL;
        }
        return &store[i];
    }
    return NULL;
}

static Entry *get_entry(const char *key, int create, ValueType type) {
    Entry *entry = find_entry(key);
    if (entry || !create) return entry;
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (store[i].used) continue;
        store[i].used = 1;
        store[i].key = strdup(key);
        store[i].type = type;
        store[i].expires_at_ms = 0;
        return &store[i];
    }
    return NULL;
}

static void send_all(int fd, const char *data, size_t length) {
    while (length > 0) {
        ssize_t sent = send(fd, data, length, 0);
        if (sent <= 0) return;
        data += sent;
        length -= (size_t)sent;
    }
}

static void reply_simple(int fd, const char *value) {
    char response[256];
    int length = snprintf(response, sizeof(response), "+%s\r\n", value);
    send_all(fd, response, (size_t)length);
}

static void reply_error(int fd, const char *value) {
    char response[512];
    int length = snprintf(response, sizeof(response), "-ERR %s\r\n", value);
    send_all(fd, response, (size_t)length);
}

static void reply_integer(int fd, long long value) {
    char response[128];
    int length = snprintf(response, sizeof(response), ":%lld\r\n", value);
    send_all(fd, response, (size_t)length);
}

static void reply_bulk(int fd, const char *value, size_t length) {
    char header[64];
    int header_length = snprintf(header, sizeof(header), "$%zu\r\n", length);
    send_all(fd, header, (size_t)header_length);
    send_all(fd, value, length);
    send_all(fd, "\r\n", 2);
}

static void reply_null(int fd) {
    send_all(fd, "$-1\r\n", 5);
}

static void reply_array_header(int fd, long long count) {
    char response[64];
    int length = snprintf(response, sizeof(response), "*%lld\r\n", count);
    send_all(fd, response, (size_t)length);
}

static int read_line(int fd, char *line, size_t capacity) {
    size_t length = 0;
    while (length + 1 < capacity) {
        char c;
        ssize_t result = recv(fd, &c, 1, 0);
        if (result <= 0) return 0;
        if (c == '\r') {
            char next;
            if (recv(fd, &next, 1, 0) <= 0 || next != '\n') return 0;
            line[length] = '\0';
            return 1;
        }
        line[length++] = c;
    }
    return 0;
}

static int read_command(int fd, char **args, size_t *argument_count) {
    char line[128];
    if (!read_line(fd, line, sizeof(line)) || line[0] != '*') return 0;
    char *end;
    long count = strtol(line + 1, &end, 10);
    if (*end != '\0' || count < 1 || count > MAX_ARGS) return 0;
    for (long i = 0; i < count; i++) {
        if (!read_line(fd, line, sizeof(line)) || line[0] != '$') return 0;
        long length = strtol(line + 1, &end, 10);
        if (*end != '\0' || length < 0 || length > MAX_VALUE) return 0;
        args[i] = malloc((size_t)length + 1);
        if (!args[i]) return 0;
        size_t received = 0;
        while (received < (size_t)length) {
            ssize_t result = recv(fd, args[i] + received, (size_t)length - received, 0);
            if (result <= 0) return 0;
            received += (size_t)result;
        }
        args[i][length] = '\0';
        char terminator[2];
        if (recv(fd, terminator, 2, MSG_WAITALL) != 2 ||
            terminator[0] != '\r' || terminator[1] != '\n') return 0;
    }
    *argument_count = (size_t)count;
    return 1;
}

static void free_args(char **args, size_t count) {
    for (size_t i = 0; i < count; i++) free(args[i]);
}

static int equals(const char *left, const char *right) {
    while (*left && *right) {
        char a = *left++;
        char b = *right++;
        if (a >= 'a' && a <= 'z') a -= 'a' - 'A';
        if (b >= 'a' && b <= 'z') b -= 'a' - 'A';
        if (a != b) return 0;
    }
    return *left == '\0' && *right == '\0';
}

static void command(int fd, char **args, size_t count) {
    if (equals(args[0], "PING")) {
        if (count == 1) reply_simple(fd, "PONG");
        else if (count == 2) reply_bulk(fd, args[1], strlen(args[1]));
        else reply_error(fd, "wrong number of arguments for 'ping' command");
        return;
    }
    if (equals(args[0], "ECHO") && count == 2) {
        reply_bulk(fd, args[1], strlen(args[1]));
        return;
    }
    if (equals(args[0], "SET") && count >= 3) {
        Entry *entry = get_entry(args[1], 1, VALUE_STRING);
        if (!entry) { reply_error(fd, "maximum key limit reached"); return; }
        if (entry->type != VALUE_STRING) { reply_error(fd, "wrong type"); return; }
        free(entry->string);
        entry->string = strdup(args[2]);
        entry->expires_at_ms = 0;
        for (size_t i = 3; i + 1 < count; i += 2) {
            if (equals(args[i], "EX") || equals(args[i], "PX")) {
                long long duration = strtoll(args[i + 1], NULL, 10);
                entry->expires_at_ms = now_ms() + duration * (equals(args[i], "EX") ? 1000 : 1);
            }
        }
        reply_simple(fd, "OK");
        return;
    }
    if (equals(args[0], "GET") && count == 2) {
        Entry *entry = find_entry(args[1]);
        if (!entry) reply_null(fd);
        else if (entry->type != VALUE_STRING) reply_error(fd, "wrong type");
        else reply_bulk(fd, entry->string, strlen(entry->string));
        return;
    }
    if (equals(args[0], "DEL") && count >= 2) {
        long removed = 0;
        for (size_t i = 1; i < count; i++) {
            Entry *entry = find_entry(args[i]);
            if (entry) { free_entry(entry); removed++; }
        }
        reply_integer(fd, removed);
        return;
    }
    if (equals(args[0], "EXISTS") && count >= 2) {
        long present = 0;
        for (size_t i = 1; i < count; i++) if (find_entry(args[i])) present++;
        reply_integer(fd, present);
        return;
    }
    if (equals(args[0], "TYPE") && count == 2) {
        Entry *entry = find_entry(args[1]);
        reply_simple(fd, entry ? (entry->type == VALUE_LIST ? "list" : "string") : "none");
        return;
    }
    if ((equals(args[0], "INCR") || equals(args[0], "DECR")) && count == 2) {
        Entry *entry = get_entry(args[1], 1, VALUE_STRING);
        if (!entry || entry->type != VALUE_STRING) { reply_error(fd, "value is not an integer"); return; }
        char *end;
        long long value = strtoll(entry->string ? entry->string : "0", &end, 10);
        if (*end != '\0') { reply_error(fd, "value is not an integer"); return; }
        value += equals(args[0], "INCR") ? 1 : -1;
        char number[64];
        snprintf(number, sizeof(number), "%lld", value);
        free(entry->string);
        entry->string = strdup(number);
        reply_integer(fd, value);
        return;
    }
    if ((equals(args[0], "LPUSH") || equals(args[0], "RPUSH")) && count >= 3) {
        int left = equals(args[0], "LPUSH");
        Entry *entry = get_entry(args[1], 1, VALUE_LIST);
        if (!entry || entry->type != VALUE_LIST) { reply_error(fd, "wrong type"); return; }
        size_t added = count - 2;
        Item *new_items = realloc(entry->items, (entry->count + added) * sizeof(*new_items));
        if (!new_items) { reply_error(fd, "out of memory"); return; }
        entry->items = new_items;
        if (left) {
            memmove(entry->items + added, entry->items, entry->count * sizeof(*entry->items));
            for (size_t i = 0; i < added; i++) {
                size_t source = count - 1 - i;
                entry->items[i] = (Item){strdup(args[source]), strlen(args[source])};
            }
        } else {
            for (size_t i = 0; i < added; i++) entry->items[entry->count + i] = (Item){strdup(args[i + 2]), strlen(args[i + 2])};
        }
        entry->count += added;
        reply_integer(fd, (long long)entry->count);
        return;
    }
    if ((equals(args[0], "LLEN") || equals(args[0], "LRANGE")) && count >= 2) {
        Entry *entry = find_entry(args[1]);
        if (equals(args[0], "LLEN")) {
            reply_integer(fd, entry && entry->type == VALUE_LIST ? (long long)entry->count : 0);
            return;
        }
        if (count != 4) { reply_error(fd, "wrong number of arguments"); return; }
        long start = strtol(args[2], NULL, 10), stop = strtol(args[3], NULL, 10);
        if (!entry || entry->type != VALUE_LIST || entry->count == 0) { reply_array_header(fd, 0); return; }
        if (start < 0) start = (long)entry->count + start;
        if (stop < 0) stop = (long)entry->count + stop;
        if (start < 0) start = 0;
        if (stop >= (long)entry->count) stop = (long)entry->count - 1;
        if (start > stop || start >= (long)entry->count) { reply_array_header(fd, 0); return; }
        reply_array_header(fd, stop - start + 1);
        for (long i = start; i <= stop; i++) reply_bulk(fd, entry->items[i].data, entry->items[i].length);
        return;
    }
    if ((equals(args[0], "LPOP") || equals(args[0], "RPOP")) && count >= 2 && count <= 3) {
        Entry *entry = find_entry(args[1]);
        if (!entry || entry->type != VALUE_LIST || entry->count == 0) { reply_null(fd); return; }
        long amount = count == 3 ? strtol(args[2], NULL, 10) : 1;
        if (amount < 1) { reply_array_header(fd, 0); return; }
        if (amount > (long)entry->count) amount = (long)entry->count;
        int right = equals(args[0], "RPOP");
        if (count == 2) {
            size_t index = right ? entry->count - 1 : 0;
            reply_bulk(fd, entry->items[index].data, entry->items[index].length);
        } else {
            reply_array_header(fd, amount);
            for (long i = 0; i < amount; i++) {
                size_t index = right ? entry->count - 1 - (size_t)i : (size_t)i;
                reply_bulk(fd, entry->items[index].data, entry->items[index].length);
            }
        }
        if (right) {
            for (long i = 0; i < amount; i++) free(entry->items[entry->count - 1 - (size_t)i].data);
        } else {
            for (long i = 0; i < amount; i++) free(entry->items[i].data);
            memmove(entry->items, entry->items + amount, (entry->count - (size_t)amount) * sizeof(*entry->items));
        }
        entry->count -= (size_t)amount;
        if (entry->count == 0) free_entry(entry);
        return;
    }
    reply_error(fd, "unknown command");
}

static int create_server(void) {
    int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server < 0) { perror("socket"); exit(1); }
    int reuse = 1;
    if (setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) { perror("setsockopt"); exit(1); }
    struct sockaddr_in address = {.sin_family = AF_INET, .sin_port = htons(PORT), .sin_addr.s_addr = htonl(INADDR_ANY)};
    if (bind(server, (struct sockaddr *)&address, sizeof(address)) < 0) { perror("bind"); exit(1); }
    if (listen(server, 16) < 0) { perror("listen"); exit(1); }
    return server;
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    setbuf(stdout, NULL);
    int server = create_server();
    int clients[MAX_CLIENTS];
    for (size_t i = 0; i < MAX_CLIENTS; i++) clients[i] = -1;
    printf("Redis clone listening on port %d\n", PORT);
    for (;;) {
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(server, &readable);
        int highest = server;
        for (size_t i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i] < 0) continue;
            FD_SET(clients[i], &readable);
            if (clients[i] > highest) highest = clients[i];
        }
        if (select(highest + 1, &readable, NULL, NULL, NULL) < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }
        if (FD_ISSET(server, &readable)) {
            int client = accept(server, NULL, NULL);
            if (client >= 0) {
                for (size_t i = 0; i < MAX_CLIENTS; i++) {
                    if (clients[i] < 0) { clients[i] = client; client = -1; break; }
                }
                if (client >= 0) close(client);
            }
        }
        for (size_t i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i] < 0 || !FD_ISSET(clients[i], &readable)) continue;
            char *args[MAX_ARGS] = {0};
            size_t count = 0;
            if (!read_command(clients[i], args, &count)) {
                close(clients[i]);
                clients[i] = -1;
                free_args(args, count);
                continue;
            }
            command(clients[i], args, count);
            free_args(args, count);
        }
    }
    close(server);
    return 0;
}
