#include "client.h"
#include "commands.h"

#include <limits.h>

static int find_crlf(const char *buf, size_t len, size_t offset, size_t *line_end, size_t *next) {
    size_t limit = offset + 65536;
    if (limit > len) limit = len;
    for (size_t i = offset; i + 1 < limit; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n') {
            *line_end = i;
            *next = i + 2;
            return 1;
        }
    }
    if (len - offset > 65536) return -1;
    return 0;
}

static int parse_i64(const char *text, size_t len, long long *out) {
    if (len == 0 || len > 18) return 0;
    size_t i = 0;
    int neg = 0;
    if (text[0] == '-') {
        neg = 1;
        i++;
        if (i == len) return 0;
    }
    long long value = 0;
    for (; i < len; i++) {
        if (text[i] < '0' || text[i] > '9') return 0;
        int digit = text[i] - '0';
        if (value > (LLONG_MAX - digit) / 10) return 0;
        value = value * 10 + digit;
    }
    *out = neg ? -value : value;
    return 1;
}

int resp_parse(const char *buf, size_t len, int allocate, char ***args_out, size_t **lens_out,
               size_t *count_out, size_t *consumed) {
    if (consumed) *consumed = 0;
    if (len == 0) return RESP_NEED;
    if (buf[0] != '*') return RESP_ERR;

    size_t line_end = 0;
    size_t next = 0;
    int marker = find_crlf(buf, len, 0, &line_end, &next);
    if (marker == 0) return RESP_NEED;
    if (marker < 0 || line_end < 1) return RESP_ERR;

    long long count = 0;
    if (!parse_i64(buf + 1, line_end - 1, &count) || count < 1 || count > MAX_ARGS) return RESP_ERR;

    size_t cursor = next;
    size_t data_off[MAX_ARGS];
    size_t data_len[MAX_ARGS];
    for (long long i = 0; i < count; i++) {
        marker = find_crlf(buf, len, cursor, &line_end, &next);
        if (marker == 0) return RESP_NEED;
        if (marker < 0 || line_end < cursor + 1 || buf[cursor] != '$') return RESP_ERR;
        long long bulk = 0;
        if (!parse_i64(buf + cursor + 1, line_end - (cursor + 1), &bulk) || bulk < 0 || bulk > MAX_VALUE) {
            return RESP_ERR;
        }
        if (next > len) return RESP_NEED;
        size_t avail = len - next;
        if (avail < (size_t)bulk + 2) return RESP_NEED;
        if (buf[next + (size_t)bulk] != '\r' || buf[next + (size_t)bulk + 1] != '\n') return RESP_ERR;
        data_off[i] = next;
        data_len[i] = (size_t)bulk;
        cursor = next + (size_t)bulk + 2;
    }

    if (consumed) *consumed = cursor;
    if (count_out) *count_out = (size_t)count;
    if (!allocate) return RESP_OK;

    char **args = calloc((size_t)count, sizeof(*args));
    size_t *lens = calloc((size_t)count, sizeof(*lens));
    if (!args || !lens) {
        free(args);
        free(lens);
        return RESP_OOM;
    }
    for (long long i = 0; i < count; i++) {
        args[i] = malloc(data_len[i] + 1);
        if (!args[i]) {
            for (long long j = 0; j < i; j++) free(args[j]);
            free(args);
            free(lens);
            return RESP_OOM;
        }
        if (data_len[i] > 0) memcpy(args[i], buf + data_off[i], data_len[i]);
        args[i][data_len[i]] = '\0';
        lens[i] = data_len[i];
    }
    *args_out = args;
    *lens_out = lens;
    return RESP_OK;
}

void free_command(char **args, size_t *lens, size_t count) {
    if (args) {
        for (size_t i = 0; i < count; i++) free(args[i]);
        free(args);
    }
    free(lens);
}

int client_append(Client *client, const char *data, size_t len) {
    if (!client || client->replay || len == 0) return 0;
    if (client->out_overflow) return -1;
    if (len > CLIENT_OUT_MAX || client->out_len > CLIENT_OUT_MAX - len) {
        client->out_overflow = 1;
        return -1;
    }
    if (client->out_len + len > client->out_cap) {
        size_t cap = client->out_cap ? client->out_cap : 1024;
        while (cap < client->out_len + len) {
            if (cap > CLIENT_OUT_MAX / 2) {
                cap = CLIENT_OUT_MAX;
                break;
            }
            cap *= 2;
        }
        if (cap < client->out_len + len) {
            client->out_overflow = 1;
            return -1;
        }
        char *grown = realloc(client->outbuf, cap);
        if (!grown) {
            client->out_overflow = 1;
            return -1;
        }
        client->outbuf = grown;
        client->out_cap = cap;
    }
    memcpy(client->outbuf + client->out_len, data, len);
    client->out_len += len;
    return 0;
}

void reply_simple(Client *client, const char *value) {
    client_append(client, "+", 1);
    client_append(client, value, strlen(value));
    client_append(client, "\r\n", 2);
}

void reply_error(Client *client, const char *value) {
    client_append(client, "-ERR ", 5);
    client_append(client, value, strlen(value));
    client_append(client, "\r\n", 2);
}

void reply_integer(Client *client, long long value) {
    char buf[64];
    int n = snprintf(buf, sizeof(buf), ":%lld\r\n", value);
    if (n > 0 && (size_t)n < sizeof(buf)) client_append(client, buf, (size_t)n);
}

void reply_bulk(Client *client, const char *value, size_t length) {
    char header[64];
    int n = snprintf(header, sizeof(header), "$%zu\r\n", length);
    if (n < 0 || (size_t)n >= sizeof(header)) return;
    client_append(client, header, (size_t)n);
    if (length > 0) client_append(client, value, length);
    client_append(client, "\r\n", 2);
}

void reply_null(Client *client) {
    client_append(client, "$-1\r\n", 5);
}

void reply_null_array(Client *client) {
    client_append(client, "*-1\r\n", 5);
}

void reply_array_header(Client *client, long long count) {
    char buf[64];
    int n = snprintf(buf, sizeof(buf), "*%lld\r\n", count);
    if (n > 0 && (size_t)n < sizeof(buf)) client_append(client, buf, (size_t)n);
}

static int inbuf_add(Client *client, const char *data, size_t len) {
    if (len > CLIENT_IN_MAX || client->in_len > CLIENT_IN_MAX - len) return -1;
    if (client->in_len + len > client->in_cap) {
        size_t cap = client->in_cap ? client->in_cap : 4096;
        while (cap < client->in_len + len) {
            if (cap > CLIENT_IN_MAX / 2) {
                cap = CLIENT_IN_MAX;
                break;
            }
            cap *= 2;
        }
        if (cap < client->in_len + len) return -1;
        char *grown = realloc(client->inbuf, cap);
        if (!grown) return -1;
        client->inbuf = grown;
        client->in_cap = cap;
    }
    memcpy(client->inbuf + client->in_len, data, len);
    client->in_len += len;
    return 0;
}

int client_read_socket(Client *client) {
    char tmp[16384];
    for (;;) {
        int n = socket_recv(client->fd, tmp, sizeof(tmp));
        if (n > 0) {
            if (inbuf_add(client, tmp, (size_t)n) != 0) {
                reply_error(client, "protocol error: request too large");
                client->close_after_flush = 1;
                return -1;
            }
            continue;
        }
        if (n == 0) {
            client->peer_closed = 1;
            return 0;
        }
        int err = socket_last_error();
        if (socket_is_intr(err)) continue;
        if (socket_would_block(err)) return 0;
        client->peer_closed = 1;
        return -1;
    }
}

static void compact_output(Client *client) {
    if (client->out_pos == 0) return;
    if (client->out_pos == client->out_len) {
        client->out_pos = 0;
        client->out_len = 0;
        return;
    }
    if (client->out_pos >= 65536) {
        memmove(client->outbuf, client->outbuf + client->out_pos, client->out_len - client->out_pos);
        client->out_len -= client->out_pos;
        client->out_pos = 0;
    }
}

int client_flush_socket(Client *client) {
    while (client->out_pos < client->out_len) {
        int n = socket_send(client->fd, client->outbuf + client->out_pos, client->out_len - client->out_pos);
        if (n > 0) {
            client->out_pos += (size_t)n;
            continue;
        }
        int err = socket_last_error();
        if (n < 0 && socket_is_intr(err)) continue;
        compact_output(client);
        if (n < 0 && socket_would_block(err)) return 0;
        client->peer_closed = 1;
        return -1;
    }
    compact_output(client);
    return 1;
}

int client_consume(Client *client, int budget) {
    client->paused = 0;
    int processed = 0;
    size_t base = 0;
    while (processed < budget && !client->close_after_flush && !client->out_overflow) {
        char **args = NULL;
        size_t *lens = NULL;
        size_t count = 0;
        size_t consumed = 0;
        int status = resp_parse(client->inbuf + base, client->in_len - base, 1, &args, &lens, &count, &consumed);
        if (status == RESP_NEED) break;
        if (status == RESP_OOM) {
            reply_error(client, "out of memory");
            client->close_after_flush = 1;
            break;
        }
        if (status != RESP_OK) {
            reply_error(client, "protocol error");
            client->close_after_flush = 1;
            break;
        }
        base += consumed;
        command(client, args, lens, count);
        free_command(args, lens, count);
        processed++;
    }
    if (base > 0) {
        memmove(client->inbuf, client->inbuf + base, client->in_len - base);
        client->in_len -= base;
    }
    if (!client->close_after_flush && !client->out_overflow && client->in_len > 0) {
        size_t consumed = 0;
        int status = resp_parse(client->inbuf, client->in_len, 0, NULL, NULL, NULL, &consumed);
        if (status == RESP_OK || status == RESP_ERR || status == RESP_OOM) client->paused = 1;
    }
    return processed;
}

void client_free_buffers(Client *client) {
    free(client->inbuf);
    free(client->outbuf);
    client->inbuf = NULL;
    client->outbuf = NULL;
    client->in_len = client->in_cap = 0;
    client->out_len = client->out_cap = client->out_pos = 0;
}
