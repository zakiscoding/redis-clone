#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
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

typedef enum { VALUE_STRING, VALUE_LIST, VALUE_STREAM } ValueType;

typedef struct {
    char *data;
    size_t length;
} Item;

typedef struct {
    long long milliseconds;
    long long sequence;
    Item *fields;
    size_t field_count;
} StreamRecord;

typedef struct {
    char *key;
    ValueType type;
    char *string;
    Item *items;
    size_t count;
    size_t capacity;
    StreamRecord *records;
    size_t record_count;
    size_t record_capacity;
    long long last_stream_ms;
    long long last_stream_sequence;
    int64_t expires_at_ms;
    int used;
    unsigned long long version;
} Entry;

typedef struct {
    char **args;
    size_t count;
} QueuedCommand;

typedef struct {
    char *key;
    unsigned long long version;
} WatchedKey;

typedef struct {
    int fd;
    int in_multi;
    int executing;
    QueuedCommand *queue;
    size_t queue_count;
    size_t queue_capacity;
    WatchedKey *watched;
    size_t watched_count;
    size_t watched_capacity;
} ClientState;

static Entry store[MAX_KEYS];
static struct {
    char *key;
    unsigned long long version;
} key_versions[MAX_KEYS];
static int configured_port = PORT;
static long long replication_offset = 0;
static const char *replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c8";

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
    for (size_t i = 0; i < entry->record_count; i++) {
        for (size_t j = 0; j < entry->records[i].field_count; j++) free(entry->records[i].fields[j].data);
        free(entry->records[i].fields);
    }
    free(entry->records);
    memset(entry, 0, sizeof(*entry));
}

static unsigned long long key_version(const char *key) {
    for (size_t i = 0; i < MAX_KEYS; i++)
        if (key_versions[i].key && strcmp(key_versions[i].key, key) == 0) return key_versions[i].version;
    return 0;
}

static void bump_version(const char *key) {
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (key_versions[i].key && strcmp(key_versions[i].key, key) == 0) {
            key_versions[i].version++;
            return;
        }
    }
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (!key_versions[i].key) {
            key_versions[i].key = strdup(key);
            key_versions[i].version = 1;
            return;
        }
    }
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

static void reply_null_array(int fd) {
    send_all(fd, "*-1\r\n", 5);
}

static void reply_stream_id(int fd, long long milliseconds, long long sequence) {
    char id[64];
    int length = snprintf(id, sizeof(id), "%lld-%lld", milliseconds, sequence);
    reply_bulk(fd, id, (size_t)length);
}

static int parse_stream_id(const char *text, long long *milliseconds, long long *sequence) {
    char *separator = strchr(text, '-');
    if (!separator) return 0;
    char *end;
    *milliseconds = strtoll(text, &end, 10);
    if (end != separator) return 0;
    *sequence = strtoll(separator + 1, &end, 10);
    return *separator != '\0' && *end == '\0';
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

static void clear_queue(ClientState *client) {
    for (size_t i = 0; i < client->queue_count; i++) {
        free_args(client->queue[i].args, client->queue[i].count);
        free(client->queue[i].args);
    }
    free(client->queue);
    client->queue = NULL;
    client->queue_count = 0;
    client->queue_capacity = 0;
}

static void clear_watches(ClientState *client) {
    for (size_t i = 0; i < client->watched_count; i++) free(client->watched[i].key);
    free(client->watched);
    client->watched = NULL;
    client->watched_count = 0;
    client->watched_capacity = 0;
}

static int watches_changed(const ClientState *client) {
    for (size_t i = 0; i < client->watched_count; i++)
        if (key_version(client->watched[i].key) != client->watched[i].version) return 1;
    return 0;
}

static int queue_command(ClientState *client, char **args, size_t count) {
    if (client->queue_count == client->queue_capacity) {
        size_t capacity = client->queue_capacity ? client->queue_capacity * 2 : 8;
        QueuedCommand *queue = realloc(client->queue, capacity * sizeof(*queue));
        if (!queue) return 0;
        client->queue = queue;
        client->queue_capacity = capacity;
    }
    char **copy = calloc(count, sizeof(*copy));
    if (!copy) return 0;
    for (size_t i = 0; i < count; i++) {
        copy[i] = strdup(args[i]);
        if (!copy[i]) {
            free_args(copy, i);
            free(copy);
            return 0;
        }
    }
    client->queue[client->queue_count++] = (QueuedCommand){copy, count};
    return 1;
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

static int parse_integer(const char *text, long long *value) {
    char *end;
    errno = 0;
    *value = strtoll(text, &end, 10);
    return errno == 0 && *text != '\0' && *end == '\0';
}

static void command(ClientState *client, char **args, size_t count);

static void command(ClientState *client, char **args, size_t count) {
    int fd = client->fd;
    if (client->in_multi && !client->executing &&
        !equals(args[0], "MULTI") && !equals(args[0], "EXEC") &&
        !equals(args[0], "DISCARD")) {
        if (queue_command(client, args, count)) reply_simple(fd, "QUEUED");
        else reply_error(fd, "transaction queue is full");
        return;
    }
    if (equals(args[0], "MULTI") && count == 1) {
        if (client->in_multi) reply_error(fd, "MULTI calls can not be nested");
        else {
            client->in_multi = 1;
            reply_simple(fd, "OK");
        }
        return;
    }
    if (equals(args[0], "DISCARD") && count == 1) {
        if (!client->in_multi) reply_error(fd, "DISCARD without MULTI");
        else {
            client->in_multi = 0;
            clear_queue(client);
            clear_watches(client);
            reply_simple(fd, "OK");
        }
        return;
    }
    if (equals(args[0], "EXEC") && count == 1) {
        if (!client->in_multi) {
            reply_error(fd, "EXEC without MULTI");
            return;
        }
        if (watches_changed(client)) {
            client->in_multi = 0;
            clear_queue(client);
            clear_watches(client);
            reply_null_array(fd);
            return;
        }
        reply_array_header(fd, (long long)client->queue_count);
        client->executing = 1;
        for (size_t i = 0; i < client->queue_count; i++) {
            command(client, client->queue[i].args, client->queue[i].count);
        }
        client->executing = 0;
        client->in_multi = 0;
        clear_queue(client);
        clear_watches(client);
        return;
    }
    if (equals(args[0], "WATCH") && count >= 2) {
        if (client->in_multi) {
            reply_error(fd, "WATCH inside MULTI is not allowed");
            return;
        }
        for (size_t i = 1; i < count; i++) {
            int already_watched = 0;
            for (size_t j = 0; j < client->watched_count; j++) {
                if (strcmp(client->watched[j].key, args[i]) == 0) {
                    client->watched[j].version = key_version(args[i]);
                    already_watched = 1;
                    break;
                }
            }
            if (already_watched) continue;
            if (client->watched_count == client->watched_capacity) {
                size_t capacity = client->watched_capacity ? client->watched_capacity * 2 : 8;
                WatchedKey *watched = realloc(client->watched, capacity * sizeof(*watched));
                if (!watched) { reply_error(fd, "out of memory"); return; }
                client->watched = watched;
                client->watched_capacity = capacity;
            }
            client->watched[client->watched_count++] = (WatchedKey){strdup(args[i]), key_version(args[i])};
        }
        reply_simple(fd, "OK");
        return;
    }
    if (equals(args[0], "UNWATCH") && count == 1) {
        clear_watches(client);
        reply_simple(fd, "OK");
        return;
    }
    if (equals(args[0], "PING")) {
        if (count == 1) reply_simple(fd, "PONG");
        else if (count == 2) reply_bulk(fd, args[1], strlen(args[1]));
        else reply_error(fd, "wrong number of arguments for 'ping' command");
        return;
    }
    if (equals(args[0], "INFO") && (count == 1 || count == 2)) {
        char info[512];
        const char *section = count == 2 ? args[1] : "default";
        int length = snprintf(info, sizeof(info),
            "# Server\r\nredis_version:7.2.0\r\nredis_mode:standalone\r\n"
            "# Replication\r\nrole:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%lld\r\n"
            "connected_slaves:0\r\n",
            replication_id, replication_offset);
        if (equals(section, "replication") || equals(section, "default") || equals(section, "server"))
            reply_bulk(fd, info, (size_t)length);
        else reply_bulk(fd, "", 0);
        return;
    }
    if (equals(args[0], "ROLE") && count == 1) {
        reply_array_header(fd, 3);
        reply_bulk(fd, "master", 6);
        reply_integer(fd, 0);
        reply_array_header(fd, 0);
        return;
    }
    if (equals(args[0], "REPLCONF") && count >= 2) {
        if (equals(args[1], "GETACK")) {
            reply_array_header(fd, 2);
            reply_bulk(fd, "REPLCONF", 8);
            char offset[64];
            int length = snprintf(offset, sizeof(offset), "%lld", replication_offset);
            reply_bulk(fd, offset, (size_t)length);
        } else {
            reply_simple(fd, "OK");
        }
        return;
    }
    if (equals(args[0], "WAIT") && count == 3) {
        reply_integer(fd, 0);
        return;
    }
    if (equals(args[0], "CONFIG") && count >= 2 && equals(args[1], "GET")) {
        reply_array_header(fd, 2);
        reply_bulk(fd, "dir", 3);
        reply_bulk(fd, ".", 1);
        return;
    }
    if (equals(args[0], "ECHO") && count == 2) {
        reply_bulk(fd, args[1], strlen(args[1]));
        return;
    }
    if (equals(args[0], "SET") && count >= 3) {
        int only_if_missing = 0;
        int only_if_present = 0;
        int64_t expiry = 0;
        for (size_t i = 3; i < count; i++) {
            if (equals(args[i], "NX")) only_if_missing = 1;
            else if (equals(args[i], "XX")) only_if_present = 1;
            else if ((equals(args[i], "EX") || equals(args[i], "PX")) && i + 1 < count) {
                long long duration;
                if (!parse_integer(args[++i], &duration) || duration <= 0) {
                    reply_error(fd, "invalid expire time");
                    return;
                }
                expiry = now_ms() + duration * (equals(args[i - 1], "EX") ? 1000 : 1);
            } else {
                reply_error(fd, "syntax error");
                return;
            }
        }
        Entry *existing = find_entry(args[1]);
        if ((only_if_missing && existing) || (only_if_present && !existing)) {
            reply_null(fd);
            return;
        }
        Entry *entry = get_entry(args[1], 1, VALUE_STRING);
        if (!entry) { reply_error(fd, "maximum key limit reached"); return; }
        if (entry->type != VALUE_STRING) { reply_error(fd, "wrong type"); return; }
        free(entry->string);
        entry->string = strdup(args[2]);
        entry->expires_at_ms = expiry;
        bump_version(args[1]);
        replication_offset += (long long)count;
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
    if (equals(args[0], "MGET") && count >= 2) {
        reply_array_header(fd, (long long)count - 1);
        for (size_t i = 1; i < count; i++) {
            Entry *entry = find_entry(args[i]);
            if (!entry || entry->type != VALUE_STRING) reply_null(fd);
            else reply_bulk(fd, entry->string, strlen(entry->string));
        }
        return;
    }
    if (equals(args[0], "MSET") && count >= 3 && (count % 2) == 1) {
        for (size_t i = 1; i < count; i += 2) {
            Entry *entry = get_entry(args[i], 1, VALUE_STRING);
            if (!entry || entry->type != VALUE_STRING) {
                reply_error(fd, "wrong type");
                return;
            }
            free(entry->string);
            entry->string = strdup(args[i + 1]);
            entry->expires_at_ms = 0;
            bump_version(args[i]);
        }
        reply_simple(fd, "OK");
        return;
    }
    if (equals(args[0], "DEL") && count >= 2) {
        long removed = 0;
        for (size_t i = 1; i < count; i++) {
            Entry *entry = find_entry(args[i]);
            if (entry) { bump_version(args[i]); free_entry(entry); removed++; }
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
        if (!entry) reply_simple(fd, "none");
        else if (entry->type == VALUE_LIST) reply_simple(fd, "list");
        else if (entry->type == VALUE_STREAM) reply_simple(fd, "stream");
        else reply_simple(fd, "string");
        return;
    }
    if (equals(args[0], "XADD") && count >= 5 && (count % 2) == 1) {
        Entry *entry = get_entry(args[1], 1, VALUE_STREAM);
        if (!entry || entry->type != VALUE_STREAM) { reply_error(fd, "wrong type"); return; }
        long long milliseconds, sequence;
        if (strcmp(args[2], "*") == 0) {
            milliseconds = now_ms();
            sequence = milliseconds == entry->last_stream_ms ? entry->last_stream_sequence + 1 : 0;
        } else if (!parse_stream_id(args[2], &milliseconds, &sequence)) {
            reply_error(fd, "invalid stream ID");
            return;
        } else if (milliseconds < entry->last_stream_ms ||
                   (milliseconds == entry->last_stream_ms && sequence <= entry->last_stream_sequence)) {
            reply_error(fd, "ID specified in XADD is equal or smaller than the target stream top item");
            return;
        }
        if (milliseconds == 0 && sequence == 0) { reply_error(fd, "The ID specified in XADD must be greater than 0-0"); return; }
        if (entry->record_count == entry->record_capacity) {
            size_t capacity = entry->record_capacity ? entry->record_capacity * 2 : 8;
            StreamRecord *records = realloc(entry->records, capacity * sizeof(*records));
            if (!records) { reply_error(fd, "out of memory"); return; }
            entry->records = records;
            entry->record_capacity = capacity;
        }
        StreamRecord *record = &entry->records[entry->record_count++];
        record->milliseconds = milliseconds;
        record->sequence = sequence;
        record->field_count = count - 3;
        record->fields = calloc(record->field_count, sizeof(*record->fields));
        if (!record->fields) { entry->record_count--; reply_error(fd, "out of memory"); return; }
        for (size_t i = 0; i < record->field_count; i++) {
            record->fields[i].data = strdup(args[i + 3]);
            record->fields[i].length = strlen(args[i + 3]);
        }
        entry->last_stream_ms = milliseconds;
        entry->last_stream_sequence = sequence;
        bump_version(args[1]);
        reply_stream_id(fd, milliseconds, sequence);
        return;
    }
    if (equals(args[0], "XRANGE") && count == 4) {
        Entry *entry = find_entry(args[1]);
        if (!entry || entry->type != VALUE_STREAM) { reply_array_header(fd, 0); return; }
        long long start_ms = 0, start_seq = 0, end_ms = LLONG_MAX, end_seq = LLONG_MAX;
        if (strcmp(args[2], "-") != 0 && !parse_stream_id(args[2], &start_ms, &start_seq)) {
            reply_error(fd, "invalid stream ID"); return;
        }
        if (strcmp(args[3], "+") != 0 && !parse_stream_id(args[3], &end_ms, &end_seq)) {
            reply_error(fd, "invalid stream ID"); return;
        }
        size_t matches = 0;
        for (size_t i = 0; i < entry->record_count; i++) {
            StreamRecord *record = &entry->records[i];
            int after_start = record->milliseconds > start_ms ||
                (record->milliseconds == start_ms && record->sequence >= start_seq);
            int before_end = record->milliseconds < end_ms ||
                (record->milliseconds == end_ms && record->sequence <= end_seq);
            if (after_start && before_end) matches++;
        }
        reply_array_header(fd, (long long)matches);
        for (size_t i = 0; i < entry->record_count; i++) {
            StreamRecord *record = &entry->records[i];
            int after_start = record->milliseconds > start_ms ||
                (record->milliseconds == start_ms && record->sequence >= start_seq);
            int before_end = record->milliseconds < end_ms ||
                (record->milliseconds == end_ms && record->sequence <= end_seq);
            if (!after_start || !before_end) continue;
            reply_array_header(fd, 2);
            reply_stream_id(fd, record->milliseconds, record->sequence);
            reply_array_header(fd, (long long)record->field_count);
            for (size_t j = 0; j < record->field_count; j++)
                reply_bulk(fd, record->fields[j].data, record->fields[j].length);
        }
        return;
    }
    if (equals(args[0], "XREAD") && count == 4 && equals(args[1], "STREAMS")) {
        Entry *entry = find_entry(args[2]);
        long long after_ms, after_seq;
        if (!parse_stream_id(args[3], &after_ms, &after_seq)) { reply_error(fd, "invalid stream ID"); return; }
        if (!entry || entry->type != VALUE_STREAM) { reply_null_array(fd); return; }
        size_t matches = 0;
        for (size_t i = 0; i < entry->record_count; i++) {
            StreamRecord *record = &entry->records[i];
            if (record->milliseconds > after_ms ||
                (record->milliseconds == after_ms && record->sequence > after_seq)) matches++;
        }
        if (matches == 0) { reply_null_array(fd); return; }
        reply_array_header(fd, 1);
        reply_array_header(fd, 2);
        reply_bulk(fd, args[2], strlen(args[2]));
        reply_array_header(fd, (long long)matches);
        for (size_t i = 0; i < entry->record_count; i++) {
            StreamRecord *record = &entry->records[i];
            if (record->milliseconds < after_ms ||
                (record->milliseconds == after_ms && record->sequence <= after_seq)) continue;
            reply_array_header(fd, 2);
            reply_stream_id(fd, record->milliseconds, record->sequence);
            reply_array_header(fd, (long long)record->field_count);
            for (size_t j = 0; j < record->field_count; j++)
                reply_bulk(fd, record->fields[j].data, record->fields[j].length);
        }
        return;
    }
    if ((equals(args[0], "INCR") || equals(args[0], "DECR")) && count == 2) {
        Entry *entry = get_entry(args[1], 1, VALUE_STRING);
        if (!entry || entry->type != VALUE_STRING) { reply_error(fd, "value is not an integer"); return; }
        long long value;
        if (!parse_integer(entry->string ? entry->string : "0", &value)) {
            reply_error(fd, "value is not an integer");
            return;
        }
        value += equals(args[0], "INCR") ? 1 : -1;
        char number[64];
        snprintf(number, sizeof(number), "%lld", value);
        free(entry->string);
        entry->string = strdup(number);
        bump_version(args[1]);
        reply_integer(fd, value);
        return;
    }
    if ((equals(args[0], "INCRBY") || equals(args[0], "DECRBY")) && count == 3) {
        long long delta;
        if (!parse_integer(args[2], &delta)) { reply_error(fd, "value is not an integer"); return; }
        Entry *entry = get_entry(args[1], 1, VALUE_STRING);
        if (!entry || entry->type != VALUE_STRING) { reply_error(fd, "value is not an integer"); return; }
        long long value;
        if (!parse_integer(entry->string ? entry->string : "0", &value)) {
            reply_error(fd, "value is not an integer");
            return;
        }
        value += equals(args[0], "INCRBY") ? delta : -delta;
        char number[64];
        snprintf(number, sizeof(number), "%lld", value);
        free(entry->string);
        entry->string = strdup(number);
        bump_version(args[1]);
        reply_integer(fd, value);
        return;
    }
    if (equals(args[0], "APPEND") && count == 3) {
        Entry *entry = get_entry(args[1], 1, VALUE_STRING);
        if (!entry || entry->type != VALUE_STRING) { reply_error(fd, "wrong type"); return; }
        size_t old_length = entry->string ? strlen(entry->string) : 0;
        size_t added_length = strlen(args[2]);
        char *value = realloc(entry->string, old_length + added_length + 1);
        if (!value) { reply_error(fd, "out of memory"); return; }
        memcpy(value + old_length, args[2], added_length + 1);
        entry->string = value;
        bump_version(args[1]);
        reply_integer(fd, (long long)(old_length + added_length));
        return;
    }
    if (equals(args[0], "STRLEN") && count == 2) {
        Entry *entry = find_entry(args[1]);
        if (!entry) reply_integer(fd, 0);
        else if (entry->type != VALUE_STRING) reply_error(fd, "wrong type");
        else reply_integer(fd, (long long)strlen(entry->string));
        return;
    }
    if ((equals(args[0], "EXPIRE") || equals(args[0], "PEXPIRE")) && count == 3) {
        long long duration;
        if (!parse_integer(args[2], &duration)) { reply_error(fd, "invalid expire time"); return; }
        Entry *entry = find_entry(args[1]);
        if (!entry) reply_integer(fd, 0);
        else {
            entry->expires_at_ms = now_ms() + duration * (equals(args[0], "EXPIRE") ? 1000 : 1);
            bump_version(args[1]);
            if (duration <= 0) free_entry(entry);
            reply_integer(fd, 1);
        }
        return;
    }
    if ((equals(args[0], "TTL") || equals(args[0], "PTTL")) && count == 2) {
        Entry *entry = find_entry(args[1]);
        if (!entry) reply_integer(fd, -2);
        else if (entry->expires_at_ms == 0) reply_integer(fd, -1);
        else {
            int64_t remaining = entry->expires_at_ms - now_ms();
            if (remaining <= 0) { bump_version(args[1]); free_entry(entry); reply_integer(fd, -2); }
            else reply_integer(fd, equals(args[0], "TTL") ? (remaining + 999) / 1000 : remaining);
        }
        return;
    }
    if (equals(args[0], "PERSIST") && count == 2) {
        Entry *entry = find_entry(args[1]);
        if (!entry || entry->expires_at_ms == 0) reply_integer(fd, 0);
        else { entry->expires_at_ms = 0; reply_integer(fd, 1); }
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
        bump_version(args[1]);
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
        bump_version(args[1]);
        if (entry->count == 0) free_entry(entry);
        return;
    }
    if (equals(args[0], "FLUSHALL") && count == 1) {
        for (size_t i = 0; i < MAX_KEYS; i++) {
            if (store[i].used) bump_version(store[i].key);
            free_entry(&store[i]);
        }
        reply_simple(fd, "OK");
        return;
    }
    if (equals(args[0], "QUIT") && count == 1) {
        reply_simple(fd, "OK");
        shutdown(fd, SHUT_RDWR);
        return;
    }
    reply_error(fd, "unknown command");
}

static int create_server(int port) {
    int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server < 0) { perror("socket"); exit(1); }
    int reuse = 1;
    if (setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) { perror("setsockopt"); exit(1); }
    struct sockaddr_in address = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port), .sin_addr.s_addr = htonl(INADDR_ANY)};
    if (bind(server, (struct sockaddr *)&address, sizeof(address)) < 0) { perror("bind"); exit(1); }
    if (listen(server, 16) < 0) { perror("listen"); exit(1); }
    return server;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    setbuf(stdout, NULL);
    int port = PORT;
    for (int i = 1; i + 1 < argc; i++) {
        if (strcmp(argv[i], "--port") == 0) port = atoi(argv[++i]);
    }
    configured_port = port;
    int server = create_server(port);
    ClientState clients[MAX_CLIENTS] = {0};
    for (size_t i = 0; i < MAX_CLIENTS; i++) clients[i].fd = -1;
    printf("Redis clone listening on port %d\n", port);
    for (;;) {
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(server, &readable);
        int highest = server;
        for (size_t i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].fd < 0) continue;
            FD_SET(clients[i].fd, &readable);
            if (clients[i].fd > highest) highest = clients[i].fd;
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
                    if (clients[i].fd < 0) {
                        clients[i].fd = client;
                        clients[i].in_multi = 0;
                        clients[i].executing = 0;
                        client = -1;
                        break;
                    }
                }
                if (client >= 0) close(client);
            }
        }
        for (size_t i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].fd < 0 || !FD_ISSET(clients[i].fd, &readable)) continue;
            char *args[MAX_ARGS] = {0};
            size_t count = 0;
            if (!read_command(clients[i].fd, args, &count)) {
                close(clients[i].fd);
                clear_queue(&clients[i]);
                clear_watches(&clients[i]);
                clients[i].fd = -1;
                free_args(args, count);
                continue;
            }
            command(&clients[i], args, count);
            free_args(args, count);
        }
    }
    close(server);
    return 0;
}
