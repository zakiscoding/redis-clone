#include "commands.h"

#include "aof.h"
#include "server.h"
#include "store.h"

#include <limits.h>

static int arg_eq(const char *arg, size_t len, const char *lit) {
    size_t n = strlen(lit);
    if (n != len) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char a = (unsigned char)arg[i];
        unsigned char b = (unsigned char)lit[i];
        if (a >= 'a' && a <= 'z') a = (unsigned char)(a - 'a' + 'A');
        if (b >= 'a' && b <= 'z') b = (unsigned char)(b - 'a' + 'A');
        if (a != b) return 0;
    }
    return 1;
}

static int parse_integer(const char *text, long long *value) {
    char *end;
    errno = 0;
    *value = strtoll(text, &end, 10);
    return errno == 0 && text[0] != '\0' && *end == '\0';
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

static void reply_stream_id(Client *client, long long milliseconds, long long sequence) {
    char id[64];
    int length = snprintf(id, sizeof(id), "%lld-%lld", milliseconds, sequence);
    if (length > 0 && (size_t)length < sizeof(id)) reply_bulk(client, id, (size_t)length);
}

static void clear_queue(Client *client) {
    for (size_t i = 0; i < client->queue_count; i++) {
        free_command(client->queue[i].args, client->queue[i].lens, client->queue[i].count);
    }
    free(client->queue);
    client->queue = NULL;
    client->queue_count = 0;
    client->queue_capacity = 0;
}

static void clear_watches(Client *client) {
    for (size_t i = 0; i < client->watched_count; i++) free(client->watched[i].key);
    free(client->watched);
    client->watched = NULL;
    client->watched_count = 0;
    client->watched_capacity = 0;
}

void client_free_transaction_state(Client *client) {
    clear_queue(client);
    clear_watches(client);
    client->in_multi = 0;
    client->executing = 0;
}

static int watches_changed(const Client *client) {
    for (size_t i = 0; i < client->watched_count; i++) {
        if (key_version(client->watched[i].key, client->watched[i].key_len) != client->watched[i].version) return 1;
    }
    return 0;
}

static int queue_command(Client *client, char **args, const size_t *lens, size_t count) {
    if (client->queue_count == client->queue_capacity) {
        size_t capacity = client->queue_capacity ? client->queue_capacity * 2 : 8;
        QueuedCommand *queue = realloc(client->queue, capacity * sizeof(*queue));
        if (!queue) return 0;
        client->queue = queue;
        client->queue_capacity = capacity;
    }
    char **copy = calloc(count, sizeof(*copy));
    size_t *copy_lens = calloc(count, sizeof(*copy_lens));
    if (!copy || !copy_lens) {
        free(copy);
        free(copy_lens);
        return 0;
    }
    for (size_t i = 0; i < count; i++) {
        copy[i] = malloc(lens[i] + 1);
        if (!copy[i]) {
            free_command(copy, copy_lens, i);
            return 0;
        }
        if (lens[i] > 0) memcpy(copy[i], args[i], lens[i]);
        copy[i][lens[i]] = '\0';
        copy_lens[i] = lens[i];
    }
    client->queue[client->queue_count].args = copy;
    client->queue[client->queue_count].lens = copy_lens;
    client->queue[client->queue_count].count = count;
    client->queue_count++;
    return 1;
}

static void log_set(char *key, size_t key_len, char *value, size_t value_len, int has_expiry, int64_t expiry) {
    if (has_expiry) {
        char ts[32];
        int n = snprintf(ts, sizeof(ts), "%lld", (long long)expiry);
        if (n <= 0) return;
        char *logged[] = {"SET", key, value, "PXAT", ts};
        size_t logged_lens[] = {3, key_len, value_len, 4, (size_t)n};
        aof_log(logged, logged_lens, 5);
    } else {
        char *logged[] = {"SET", key, value};
        size_t logged_lens[] = {3, key_len, value_len};
        aof_log(logged, logged_lens, 3);
    }
}

static void log_del(char *key, size_t key_len) {
    char *logged[] = {"DEL", key};
    size_t logged_lens[] = {3, key_len};
    aof_log(logged, logged_lens, 2);
}

static void log_expire_at(char *key, size_t key_len, int64_t when) {
    char ts[32];
    int n = snprintf(ts, sizeof(ts), "%lld", (long long)when);
    if (n <= 0) return;
    char *logged[] = {"PEXPIREAT", key, ts};
    size_t logged_lens[] = {9, key_len, (size_t)n};
    aof_log(logged, logged_lens, 3);
}

static int add_overflow(long long value, long long delta) {
    if (delta > 0 && value > LLONG_MAX - delta) return 1;
    if (delta < 0 && value < LLONG_MIN - delta) return 1;
    return 0;
}

static void drop_empty_entry(Entry *entry, int created) {
    if (created && entry && entry->record_count == 0 && entry->count == 0 && !entry->string) free_entry(entry);
}

void command(Client *client, char **args, const size_t *lens, size_t count) {
    if (count == 0) {
        reply_error(client, "protocol error");
        return;
    }
    if (client->in_multi && !client->executing &&
        !arg_eq(args[0], lens[0], "MULTI") && !arg_eq(args[0], lens[0], "EXEC") &&
        !arg_eq(args[0], lens[0], "DISCARD")) {
        if (queue_command(client, args, lens, count)) reply_simple(client, "QUEUED");
        else reply_error(client, "transaction queue is full");
        return;
    }
    if (arg_eq(args[0], lens[0], "MULTI") && count == 1) {
        if (client->in_multi) reply_error(client, "MULTI calls can not be nested");
        else {
            client->in_multi = 1;
            reply_simple(client, "OK");
        }
        return;
    }
    if (arg_eq(args[0], lens[0], "DISCARD") && count == 1) {
        if (!client->in_multi) reply_error(client, "DISCARD without MULTI");
        else {
            client->in_multi = 0;
            clear_queue(client);
            clear_watches(client);
            reply_simple(client, "OK");
        }
        return;
    }
    if (arg_eq(args[0], lens[0], "EXEC") && count == 1) {
        if (!client->in_multi) {
            reply_error(client, "EXEC without MULTI");
            return;
        }
        if (watches_changed(client)) {
            client->in_multi = 0;
            clear_queue(client);
            clear_watches(client);
            reply_null_array(client);
            return;
        }
        reply_array_header(client, (long long)client->queue_count);
        client->executing = 1;
        size_t queued = client->queue_count;
        for (size_t i = 0; i < queued; i++) {
            command(client, client->queue[i].args, client->queue[i].lens, client->queue[i].count);
        }
        client->executing = 0;
        client->in_multi = 0;
        clear_queue(client);
        clear_watches(client);
        return;
    }
    if (arg_eq(args[0], lens[0], "WATCH") && count >= 2) {
        if (client->in_multi) {
            reply_error(client, "WATCH inside MULTI is not allowed");
            return;
        }
        for (size_t i = 1; i < count; i++) {
            int already = 0;
            for (size_t j = 0; j < client->watched_count; j++) {
                if (client->watched[j].key_len == lens[i] &&
                    memcmp(client->watched[j].key, args[i], lens[i]) == 0) {
                    client->watched[j].version = key_version(args[i], lens[i]);
                    already = 1;
                    break;
                }
            }
            if (already) continue;
            if (client->watched_count == client->watched_capacity) {
                size_t capacity = client->watched_capacity ? client->watched_capacity * 2 : 8;
                WatchedKey *watched = realloc(client->watched, capacity * sizeof(*watched));
                if (!watched) {
                    reply_error(client, "out of memory");
                    return;
                }
                client->watched = watched;
                client->watched_capacity = capacity;
            }
            char *key = malloc(lens[i] + 1);
            if (!key) {
                reply_error(client, "out of memory");
                return;
            }
            if (lens[i] > 0) memcpy(key, args[i], lens[i]);
            key[lens[i]] = '\0';
            client->watched[client->watched_count].key = key;
            client->watched[client->watched_count].key_len = lens[i];
            client->watched[client->watched_count].version = key_version(args[i], lens[i]);
            client->watched_count++;
        }
        reply_simple(client, "OK");
        return;
    }
    if (arg_eq(args[0], lens[0], "UNWATCH") && count == 1) {
        clear_watches(client);
        reply_simple(client, "OK");
        return;
    }
    if (arg_eq(args[0], lens[0], "PING")) {
        if (count == 1) reply_simple(client, "PONG");
        else if (count == 2) reply_bulk(client, args[1], lens[1]);
        else reply_error(client, "wrong number of arguments for 'ping' command");
        return;
    }
    if (arg_eq(args[0], lens[0], "INFO") && (count == 1 || count == 2)) {
        char info[2048];
        const char *section = count == 2 ? args[1] : "default";
        size_t section_len = count == 2 ? lens[1] : 7;
        int length = snprintf(info, sizeof(info),
            "# Server\r\nredis_version:7.2.0\r\nredis_mode:standalone\r\nio:%s\r\nkeys:%zu\r\n"
            "# Persistence\r\naof_enabled:%d\r\naof_fsync:%s\r\naof_sync_count:%llu\r\n"
            "aof_current_size:%lld\r\naof_last_write_status:%s\r\n"
            "# Replication\r\nrole:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%lld\r\n"
            "connected_slaves:0\r\n",
            g_io_mode == IO_EPOLL ? "epoll" : "select",
            store_key_count(),
            aof_is_enabled(),
            aof_sync_policy_name(),
            aof_sync_count(),
            aof_current_size(),
            aof_last_status_ok() ? "ok" : "error",
            g_replid,
            g_repl_offset);
        if (length < 0) length = 0;
        if ((size_t)length >= sizeof(info)) length = (int)strlen(info);
        if (arg_eq(section, section_len, "replication") || arg_eq(section, section_len, "default") ||
            arg_eq(section, section_len, "server") || arg_eq(section, section_len, "persistence") ||
            arg_eq(section, section_len, "all")) {
            reply_bulk(client, info, (size_t)length);
        } else {
            reply_bulk(client, "", 0);
        }
        return;
    }
    if (arg_eq(args[0], lens[0], "ROLE") && count == 1) {
        reply_array_header(client, 3);
        reply_bulk(client, "master", 6);
        reply_integer(client, 0);
        reply_array_header(client, 0);
        return;
    }
    if (arg_eq(args[0], lens[0], "REPLCONF") && count >= 2) {
        if (arg_eq(args[1], lens[1], "GETACK")) {
            reply_array_header(client, 2);
            reply_bulk(client, "REPLCONF", 8);
            char offset[64];
            int length = snprintf(offset, sizeof(offset), "%lld", g_repl_offset);
            if (length < 0) length = 0;
            reply_bulk(client, offset, (size_t)length);
        } else {
            reply_simple(client, "OK");
        }
        return;
    }
    if (arg_eq(args[0], lens[0], "WAIT") && count == 3) {
        reply_integer(client, 0);
        return;
    }
    if (arg_eq(args[0], lens[0], "CONFIG") && count == 3 && arg_eq(args[1], lens[1], "GET")) {
        const char *value = NULL;
        if (arg_eq(args[2], lens[2], "dir")) value = g_dir;
        else if (arg_eq(args[2], lens[2], "dbfilename")) value = g_dbfilename;
        else if (arg_eq(args[2], lens[2], "appendonly")) value = aof_is_enabled() ? "yes" : "no";
        else if (arg_eq(args[2], lens[2], "appendfsync")) value = aof_sync_policy_name();
        if (!value) {
            reply_array_header(client, 0);
            return;
        }
        reply_array_header(client, 2);
        reply_bulk(client, args[2], lens[2]);
        reply_bulk(client, value, strlen(value));
        return;
    }
    if (arg_eq(args[0], lens[0], "ECHO") && count == 2) {
        reply_bulk(client, args[1], lens[1]);
        return;
    }
    if (arg_eq(args[0], lens[0], "SHUTDOWN") && (count == 1 || count == 2)) {
        reply_simple(client, "OK");
        client->close_after_flush = 1;
        g_stop = 1;
        return;
    }
    if (arg_eq(args[0], lens[0], "SET") && count >= 3) {
        int only_if_missing = 0;
        int only_if_present = 0;
        int has_expiry = 0;
        int64_t expiry = 0;
        for (size_t i = 3; i < count; i++) {
            if (arg_eq(args[i], lens[i], "NX")) only_if_missing = 1;
            else if (arg_eq(args[i], lens[i], "XX")) only_if_present = 1;
            else if (arg_eq(args[i], lens[i], "EX") || arg_eq(args[i], lens[i], "PX") ||
                     arg_eq(args[i], lens[i], "EXAT") || arg_eq(args[i], lens[i], "PXAT")) {
                char *opt = args[i];
                size_t opt_len = lens[i];
                if (i + 1 >= count) {
                    reply_error(client, "syntax error");
                    return;
                }
                i++;
                long long duration;
                if (!parse_integer(args[i], &duration)) {
                    reply_error(client, "invalid expire time");
                    return;
                }
                has_expiry = 1;
                if (arg_eq(opt, opt_len, "EX")) {
                    if (duration <= 0) {
                        reply_error(client, "invalid expire time");
                        return;
                    }
                    expiry = unix_ms() + duration * 1000;
                } else if (arg_eq(opt, opt_len, "PX")) {
                    if (duration <= 0) {
                        reply_error(client, "invalid expire time");
                        return;
                    }
                    expiry = unix_ms() + duration;
                } else if (arg_eq(opt, opt_len, "EXAT")) {
                    expiry = duration * 1000;
                } else {
                    expiry = duration;
                }
            } else {
                reply_error(client, "syntax error");
                return;
            }
        }
        if (only_if_missing && only_if_present) {
            reply_error(client, "syntax error");
            return;
        }
        Entry *existing = find_entry(args[1], lens[1]);
        if ((only_if_missing && existing) || (only_if_present && !existing)) {
            reply_null(client);
            return;
        }
        if (has_expiry && expiry <= unix_ms()) {
            if (existing) {
                bump_version(args[1], lens[1]);
                free_entry(existing);
                log_del(args[1], lens[1]);
            }
            reply_simple(client, "OK");
            return;
        }
        int created = existing == NULL;
        Entry *entry = get_entry(args[1], lens[1], 1, VALUE_STRING);
        if (!entry) {
            reply_error(client, "maximum key limit reached");
            return;
        }
        if (entry->type != VALUE_STRING) {
            reply_error(client, "wrong type");
            return;
        }
        char *value = malloc(lens[2] + 1);
        if (!value) {
            if (created) free_entry(entry);
            reply_error(client, "out of memory");
            return;
        }
        if (lens[2] > 0) memcpy(value, args[2], lens[2]);
        value[lens[2]] = '\0';
        free(entry->string);
        entry->string = value;
        entry->string_len = lens[2];
        entry->expires_at_ms = has_expiry ? expiry : 0;
        bump_version(args[1], lens[1]);
        log_set(args[1], lens[1], args[2], lens[2], has_expiry, expiry);
        g_repl_offset += (long long)count;
        reply_simple(client, "OK");
        return;
    }
    if (arg_eq(args[0], lens[0], "GET") && count == 2) {
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry) reply_null(client);
        else if (entry->type != VALUE_STRING) reply_error(client, "wrong type");
        else reply_bulk(client, entry->string ? entry->string : "", entry->string_len);
        return;
    }
    if (arg_eq(args[0], lens[0], "MGET") && count >= 2) {
        reply_array_header(client, (long long)count - 1);
        for (size_t i = 1; i < count; i++) {
            Entry *entry = find_entry(args[i], lens[i]);
            if (!entry || entry->type != VALUE_STRING) reply_null(client);
            else reply_bulk(client, entry->string ? entry->string : "", entry->string_len);
        }
        return;
    }
    if (arg_eq(args[0], lens[0], "MSET") && count >= 3 && (count % 2) == 1) {
        for (size_t i = 1; i < count; i += 2) {
            Entry *entry = find_entry(args[i], lens[i]);
            if (entry && entry->type != VALUE_STRING) {
                reply_error(client, "wrong type");
                return;
            }
        }
        for (size_t i = 1; i < count; i += 2) {
            Entry *existing = find_entry(args[i], lens[i]);
            int created = existing == NULL;
            Entry *entry = get_entry(args[i], lens[i], 1, VALUE_STRING);
            if (!entry || entry->type != VALUE_STRING) {
                reply_error(client, "maximum key limit reached");
                return;
            }
            char *value = malloc(lens[i + 1] + 1);
            if (!value) {
                if (created) free_entry(entry);
                reply_error(client, "out of memory");
                return;
            }
            if (lens[i + 1] > 0) memcpy(value, args[i + 1], lens[i + 1]);
            value[lens[i + 1]] = '\0';
            free(entry->string);
            entry->string = value;
            entry->string_len = lens[i + 1];
            entry->expires_at_ms = 0;
            bump_version(args[i], lens[i]);
            log_set(args[i], lens[i], args[i + 1], lens[i + 1], 0, 0);
        }
        reply_simple(client, "OK");
        return;
    }
    if (arg_eq(args[0], lens[0], "DEL") && count >= 2) {
        long removed = 0;
        for (size_t i = 1; i < count; i++) {
            Entry *entry = find_entry(args[i], lens[i]);
            if (!entry) continue;
            bump_version(args[i], lens[i]);
            free_entry(entry);
            removed++;
        }
        aof_log(args, lens, count);
        reply_integer(client, removed);
        return;
    }
    if (arg_eq(args[0], lens[0], "EXISTS") && count >= 2) {
        long present = 0;
        for (size_t i = 1; i < count; i++) if (find_entry(args[i], lens[i])) present++;
        reply_integer(client, present);
        return;
    }
    if (arg_eq(args[0], lens[0], "TYPE") && count == 2) {
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry) reply_simple(client, "none");
        else if (entry->type == VALUE_LIST) reply_simple(client, "list");
        else if (entry->type == VALUE_STREAM) reply_simple(client, "stream");
        else reply_simple(client, "string");
        return;
    }
    if (arg_eq(args[0], lens[0], "XADD") && count >= 5 && (count % 2) == 1) {
        int created = find_entry(args[1], lens[1]) == NULL;
        Entry *entry = get_entry(args[1], lens[1], 1, VALUE_STREAM);
        if (!entry || entry->type != VALUE_STREAM) {
            reply_error(client, "wrong type");
            return;
        }
        long long milliseconds = 0;
        long long sequence = 0;
        int generated = lens[2] == 1 && args[2][0] == '*';
        if (generated) {
            milliseconds = unix_ms();
            if (entry->record_count > 0 && milliseconds <= entry->last_stream_ms) {
                milliseconds = entry->last_stream_ms;
                sequence = entry->last_stream_sequence + 1;
            } else {
                sequence = 0;
            }
        } else if (!parse_stream_id(args[2], &milliseconds, &sequence)) {
            drop_empty_entry(entry, created);
            reply_error(client, "invalid stream ID");
            return;
        } else if (entry->record_count > 0 &&
                   (milliseconds < entry->last_stream_ms ||
                    (milliseconds == entry->last_stream_ms && sequence <= entry->last_stream_sequence))) {
            reply_error(client, "ID specified in XADD is equal or smaller than the target stream top item");
            return;
        }
        if (milliseconds == 0 && sequence == 0) {
            drop_empty_entry(entry, created);
            reply_error(client, "The ID specified in XADD must be greater than 0-0");
            return;
        }
        if (entry->record_count == entry->record_capacity) {
            size_t capacity = entry->record_capacity ? entry->record_capacity * 2 : 8;
            StreamRecord *records = realloc(entry->records, capacity * sizeof(*records));
            if (!records) {
                drop_empty_entry(entry, created);
                reply_error(client, "out of memory");
                return;
            }
            entry->records = records;
            entry->record_capacity = capacity;
        }
        StreamRecord *record = &entry->records[entry->record_count];
        memset(record, 0, sizeof(*record));
        record->milliseconds = milliseconds;
        record->sequence = sequence;
        record->field_count = count - 3;
        record->fields = calloc(record->field_count, sizeof(*record->fields));
        if (!record->fields) {
            drop_empty_entry(entry, created);
            reply_error(client, "out of memory");
            return;
        }
        for (size_t i = 0; i < record->field_count; i++) {
            size_t n = lens[i + 3];
            record->fields[i].data = malloc(n + 1);
            if (!record->fields[i].data) {
                for (size_t j = 0; j < i; j++) free(record->fields[j].data);
                free(record->fields);
                record->fields = NULL;
                drop_empty_entry(entry, created);
                reply_error(client, "out of memory");
                return;
            }
            if (n > 0) memcpy(record->fields[i].data, args[i + 3], n);
            record->fields[i].data[n] = '\0';
            record->fields[i].length = n;
        }
        entry->record_count++;
        entry->last_stream_ms = milliseconds;
        entry->last_stream_sequence = sequence;
        bump_version(args[1], lens[1]);
        char idbuf[64];
        int idn = snprintf(idbuf, sizeof(idbuf), "%lld-%lld", milliseconds, sequence);
        if (idn > 0) {
            char **logged = malloc(count * sizeof(*logged));
            size_t *logged_lens = malloc(count * sizeof(*logged_lens));
            if (logged && logged_lens) {
                logged[0] = args[0];
                logged_lens[0] = lens[0];
                logged[1] = args[1];
                logged_lens[1] = lens[1];
                logged[2] = idbuf;
                logged_lens[2] = (size_t)idn;
                for (size_t i = 3; i < count; i++) {
                    logged[i] = args[i];
                    logged_lens[i] = lens[i];
                }
                aof_log(logged, logged_lens, count);
            }
            free(logged);
            free(logged_lens);
        }
        reply_stream_id(client, milliseconds, sequence);
        return;
    }
    if (arg_eq(args[0], lens[0], "XRANGE") && count == 4) {
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry || entry->type != VALUE_STREAM) {
            reply_array_header(client, 0);
            return;
        }
        long long start_ms = 0, start_seq = 0, end_ms = LLONG_MAX, end_seq = LLONG_MAX;
        if (!(lens[2] == 1 && args[2][0] == '-') && !parse_stream_id(args[2], &start_ms, &start_seq)) {
            reply_error(client, "invalid stream ID");
            return;
        }
        if (!(lens[3] == 1 && args[3][0] == '+') && !parse_stream_id(args[3], &end_ms, &end_seq)) {
            reply_error(client, "invalid stream ID");
            return;
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
        reply_array_header(client, (long long)matches);
        for (size_t i = 0; i < entry->record_count; i++) {
            StreamRecord *record = &entry->records[i];
            int after_start = record->milliseconds > start_ms ||
                (record->milliseconds == start_ms && record->sequence >= start_seq);
            int before_end = record->milliseconds < end_ms ||
                (record->milliseconds == end_ms && record->sequence <= end_seq);
            if (!after_start || !before_end) continue;
            reply_array_header(client, 2);
            reply_stream_id(client, record->milliseconds, record->sequence);
            reply_array_header(client, (long long)record->field_count);
            for (size_t j = 0; j < record->field_count; j++) {
                reply_bulk(client, record->fields[j].data, record->fields[j].length);
            }
        }
        return;
    }
    if (arg_eq(args[0], lens[0], "XREAD") && count == 4 && arg_eq(args[1], lens[1], "STREAMS")) {
        Entry *entry = find_entry(args[2], lens[2]);
        long long after_ms, after_seq;
        if (!parse_stream_id(args[3], &after_ms, &after_seq)) {
            reply_error(client, "invalid stream ID");
            return;
        }
        if (!entry || entry->type != VALUE_STREAM) {
            reply_null_array(client);
            return;
        }
        size_t matches = 0;
        for (size_t i = 0; i < entry->record_count; i++) {
            StreamRecord *record = &entry->records[i];
            if (record->milliseconds > after_ms ||
                (record->milliseconds == after_ms && record->sequence > after_seq)) matches++;
        }
        if (matches == 0) {
            reply_null_array(client);
            return;
        }
        reply_array_header(client, 1);
        reply_array_header(client, 2);
        reply_bulk(client, args[2], lens[2]);
        reply_array_header(client, (long long)matches);
        for (size_t i = 0; i < entry->record_count; i++) {
            StreamRecord *record = &entry->records[i];
            if (record->milliseconds < after_ms ||
                (record->milliseconds == after_ms && record->sequence <= after_seq)) continue;
            reply_array_header(client, 2);
            reply_stream_id(client, record->milliseconds, record->sequence);
            reply_array_header(client, (long long)record->field_count);
            for (size_t j = 0; j < record->field_count; j++) {
                reply_bulk(client, record->fields[j].data, record->fields[j].length);
            }
        }
        return;
    }
    if ((arg_eq(args[0], lens[0], "INCR") || arg_eq(args[0], lens[0], "DECR")) && count == 2) {
        Entry *existing = find_entry(args[1], lens[1]);
        int created = existing == NULL;
        Entry *entry = get_entry(args[1], lens[1], 1, VALUE_STRING);
        if (!entry || entry->type != VALUE_STRING) {
            reply_error(client, "value is not an integer");
            return;
        }
        long long value;
        if (!parse_integer(entry->string ? entry->string : "0", &value)) {
            reply_error(client, "value is not an integer");
            return;
        }
        long long delta = arg_eq(args[0], lens[0], "INCR") ? 1 : -1;
        if (add_overflow(value, delta)) {
            if (created && entry->string == NULL) free_entry(entry);
            reply_error(client, "increment or decrement would overflow");
            return;
        }
        value += delta;
        char number[64];
        snprintf(number, sizeof(number), "%lld", value);
        size_t number_len = strlen(number);
        char *stored = malloc(number_len + 1);
        if (!stored) {
            reply_error(client, "out of memory");
            return;
        }
        memcpy(stored, number, number_len + 1);
        free(entry->string);
        entry->string = stored;
        entry->string_len = number_len;
        bump_version(args[1], lens[1]);
        aof_log(args, lens, count);
        reply_integer(client, value);
        return;
    }
    if ((arg_eq(args[0], lens[0], "INCRBY") || arg_eq(args[0], lens[0], "DECRBY")) && count == 3) {
        long long delta;
        if (!parse_integer(args[2], &delta)) {
            reply_error(client, "value is not an integer");
            return;
        }
        if (arg_eq(args[0], lens[0], "DECRBY")) {
            if (delta == LLONG_MIN) {
                reply_error(client, "increment or decrement would overflow");
                return;
            }
            delta = -delta;
        }
        Entry *existing = find_entry(args[1], lens[1]);
        int created = existing == NULL;
        Entry *entry = get_entry(args[1], lens[1], 1, VALUE_STRING);
        if (!entry || entry->type != VALUE_STRING) {
            reply_error(client, "value is not an integer");
            return;
        }
        long long value;
        if (!parse_integer(entry->string ? entry->string : "0", &value)) {
            reply_error(client, "value is not an integer");
            return;
        }
        if (add_overflow(value, delta)) {
            if (created && entry->string == NULL) free_entry(entry);
            reply_error(client, "increment or decrement would overflow");
            return;
        }
        value += delta;
        char number[64];
        snprintf(number, sizeof(number), "%lld", value);
        size_t number_len = strlen(number);
        char *stored = malloc(number_len + 1);
        if (!stored) {
            reply_error(client, "out of memory");
            return;
        }
        memcpy(stored, number, number_len + 1);
        free(entry->string);
        entry->string = stored;
        entry->string_len = number_len;
        bump_version(args[1], lens[1]);
        aof_log(args, lens, count);
        reply_integer(client, value);
        return;
    }
    if (arg_eq(args[0], lens[0], "APPEND") && count == 3) {
        Entry *entry = get_entry(args[1], lens[1], 1, VALUE_STRING);
        if (!entry || entry->type != VALUE_STRING) {
            reply_error(client, "wrong type");
            return;
        }
        size_t old_length = entry->string_len;
        size_t added = lens[2];
        char *value = realloc(entry->string, old_length + added + 1);
        if (!value) {
            reply_error(client, "out of memory");
            return;
        }
        if (added > 0) memcpy(value + old_length, args[2], added);
        value[old_length + added] = '\0';
        entry->string = value;
        entry->string_len = old_length + added;
        bump_version(args[1], lens[1]);
        aof_log(args, lens, count);
        reply_integer(client, (long long)entry->string_len);
        return;
    }
    if (arg_eq(args[0], lens[0], "STRLEN") && count == 2) {
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry) reply_integer(client, 0);
        else if (entry->type != VALUE_STRING) reply_error(client, "wrong type");
        else reply_integer(client, (long long)entry->string_len);
        return;
    }
    if (arg_eq(args[0], lens[0], "PEXPIREAT") && count == 3) {
        long long when;
        if (!parse_integer(args[2], &when)) {
            reply_error(client, "invalid expire time");
            return;
        }
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry) {
            reply_integer(client, 0);
            return;
        }
        if (when <= unix_ms()) {
            bump_version(args[1], lens[1]);
            free_entry(entry);
        } else {
            entry->expires_at_ms = when;
            bump_version(args[1], lens[1]);
        }
        log_expire_at(args[1], lens[1], when);
        reply_integer(client, 1);
        return;
    }
    if ((arg_eq(args[0], lens[0], "EXPIRE") || arg_eq(args[0], lens[0], "PEXPIRE")) && count == 3) {
        long long duration;
        if (!parse_integer(args[2], &duration)) {
            reply_error(client, "invalid expire time");
            return;
        }
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry) {
            reply_integer(client, 0);
            return;
        }
        int64_t when = unix_ms() + duration * (arg_eq(args[0], lens[0], "EXPIRE") ? 1000 : 1);
        if (duration <= 0 || when <= unix_ms()) {
            bump_version(args[1], lens[1]);
            free_entry(entry);
            log_del(args[1], lens[1]);
        } else {
            entry->expires_at_ms = when;
            bump_version(args[1], lens[1]);
            log_expire_at(args[1], lens[1], when);
        }
        reply_integer(client, 1);
        return;
    }
    if ((arg_eq(args[0], lens[0], "TTL") || arg_eq(args[0], lens[0], "PTTL")) && count == 2) {
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry) reply_integer(client, -2);
        else if (entry->expires_at_ms == 0) reply_integer(client, -1);
        else {
            int64_t remaining = entry->expires_at_ms - unix_ms();
            if (remaining <= 0) {
                bump_version(args[1], lens[1]);
                free_entry(entry);
                reply_integer(client, -2);
            } else if (arg_eq(args[0], lens[0], "TTL")) {
                reply_integer(client, (remaining + 999) / 1000);
            } else {
                reply_integer(client, remaining);
            }
        }
        return;
    }
    if (arg_eq(args[0], lens[0], "PERSIST") && count == 2) {
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry || entry->expires_at_ms == 0) reply_integer(client, 0);
        else {
            entry->expires_at_ms = 0;
            bump_version(args[1], lens[1]);
            aof_log(args, lens, count);
            reply_integer(client, 1);
        }
        return;
    }
    if ((arg_eq(args[0], lens[0], "LPUSH") || arg_eq(args[0], lens[0], "RPUSH")) && count >= 3) {
        int left = arg_eq(args[0], lens[0], "LPUSH");
        Entry *entry = get_entry(args[1], lens[1], 1, VALUE_LIST);
        if (!entry || entry->type != VALUE_LIST) {
            reply_error(client, "wrong type");
            return;
        }
        size_t added = count - 2;
        char **copies = calloc(added, sizeof(*copies));
        size_t *copy_lens = calloc(added, sizeof(*copy_lens));
        if (!copies || !copy_lens) {
            free(copies);
            free(copy_lens);
            reply_error(client, "out of memory");
            return;
        }
        for (size_t i = 0; i < added; i++) {
            size_t source = left ? count - 1 - i : i + 2;
            copies[i] = malloc(lens[source] + 1);
            if (!copies[i]) {
                for (size_t j = 0; j < i; j++) free(copies[j]);
                free(copies);
                free(copy_lens);
                reply_error(client, "out of memory");
                return;
            }
            if (lens[source] > 0) memcpy(copies[i], args[source], lens[source]);
            copies[i][lens[source]] = '\0';
            copy_lens[i] = lens[source];
        }
        Item *items = realloc(entry->items, (entry->count + added) * sizeof(*items));
        if (!items) {
            for (size_t i = 0; i < added; i++) free(copies[i]);
            free(copies);
            free(copy_lens);
            reply_error(client, "out of memory");
            return;
        }
        entry->items = items;
        if (left) memmove(entry->items + added, entry->items, entry->count * sizeof(*entry->items));
        for (size_t i = 0; i < added; i++) {
            size_t index = left ? i : entry->count + i;
            entry->items[index].data = copies[i];
            entry->items[index].length = copy_lens[i];
        }
        entry->count += added;
        free(copies);
        free(copy_lens);
        bump_version(args[1], lens[1]);
        aof_log(args, lens, count);
        reply_integer(client, (long long)entry->count);
        return;
    }
    if (arg_eq(args[0], lens[0], "LLEN") && count == 2) {
        Entry *entry = find_entry(args[1], lens[1]);
        reply_integer(client, entry && entry->type == VALUE_LIST ? (long long)entry->count : 0);
        return;
    }
    if (arg_eq(args[0], lens[0], "LRANGE") && count == 4) {
        Entry *entry = find_entry(args[1], lens[1]);
        long start = strtol(args[2], NULL, 10);
        long stop = strtol(args[3], NULL, 10);
        if (!entry || entry->type != VALUE_LIST || entry->count == 0) {
            reply_array_header(client, 0);
            return;
        }
        if (start < 0) start = (long)entry->count + start;
        if (stop < 0) stop = (long)entry->count + stop;
        if (start < 0) start = 0;
        if (stop >= (long)entry->count) stop = (long)entry->count - 1;
        if (start > stop || start >= (long)entry->count) {
            reply_array_header(client, 0);
            return;
        }
        reply_array_header(client, stop - start + 1);
        for (long i = start; i <= stop; i++) reply_bulk(client, entry->items[i].data, entry->items[i].length);
        return;
    }
    if ((arg_eq(args[0], lens[0], "LPOP") || arg_eq(args[0], lens[0], "RPOP")) && count >= 2 && count <= 3) {
        Entry *entry = find_entry(args[1], lens[1]);
        if (!entry || entry->type != VALUE_LIST || entry->count == 0) {
            reply_null(client);
            return;
        }
        long amount = count == 3 ? strtol(args[2], NULL, 10) : 1;
        if (amount < 1) {
            reply_array_header(client, 0);
            return;
        }
        if (amount > (long)entry->count) amount = (long)entry->count;
        int right = arg_eq(args[0], lens[0], "RPOP");
        if (count == 2) {
            size_t index = right ? entry->count - 1 : 0;
            reply_bulk(client, entry->items[index].data, entry->items[index].length);
        } else {
            reply_array_header(client, amount);
            for (long i = 0; i < amount; i++) {
                size_t index = right ? entry->count - 1 - (size_t)i : (size_t)i;
                reply_bulk(client, entry->items[index].data, entry->items[index].length);
            }
        }
        if (right) {
            for (long i = 0; i < amount; i++) free(entry->items[entry->count - 1 - (size_t)i].data);
        } else {
            for (long i = 0; i < amount; i++) free(entry->items[i].data);
            memmove(entry->items, entry->items + amount, (entry->count - (size_t)amount) * sizeof(*entry->items));
        }
        entry->count -= (size_t)amount;
        bump_version(args[1], lens[1]);
        aof_log(args, lens, count);
        if (entry->count == 0) free_entry(entry);
        return;
    }
    if (arg_eq(args[0], lens[0], "FLUSHALL") && count == 1) {
        store_flush();
        aof_log(args, lens, count);
        reply_simple(client, "OK");
        return;
    }
    if (arg_eq(args[0], lens[0], "QUIT") && count == 1) {
        reply_simple(client, "OK");
        client->close_after_flush = 1;
        return;
    }
    reply_error(client, "unknown command");
}
