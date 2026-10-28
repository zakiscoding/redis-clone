#ifndef CLIENT_H
#define CLIENT_H

#include "platform.h"

#define MAX_ARGS 64
#define MAX_VALUE (1024 * 1024)
#define CLIENT_IN_MAX (8 * 1024 * 1024)
#define CLIENT_OUT_MAX (4 * 1024 * 1024)
#define COMMANDS_PER_TURN 128

enum { RESP_NEED = 0, RESP_OK = 1, RESP_ERR = -1, RESP_OOM = -2 };

typedef struct {
    char **args;
    size_t *lens;
    size_t count;
} QueuedCommand;

typedef struct {
    char *key;
    size_t key_len;
    unsigned long long version;
} WatchedKey;

typedef struct Client {
    socket_t fd;
    int active;
    int in_multi;
    int executing;
    int close_after_flush;
    int peer_closed;
    int replay;
    int out_overflow;
    int paused;
    int in_epoll;
    char *inbuf;
    size_t in_len;
    size_t in_cap;
    char *outbuf;
    size_t out_len;
    size_t out_cap;
    size_t out_pos;
    QueuedCommand *queue;
    size_t queue_count;
    size_t queue_capacity;
    WatchedKey *watched;
    size_t watched_count;
    size_t watched_capacity;
    uint32_t epoll_interest;
} Client;

int resp_parse(const char *buf, size_t len, int allocate, char ***args, size_t **lens,
               size_t *count, size_t *consumed);
void free_command(char **args, size_t *lens, size_t count);

int client_append(Client *client, const char *data, size_t len);
void reply_simple(Client *client, const char *value);
void reply_error(Client *client, const char *value);
void reply_integer(Client *client, long long value);
void reply_bulk(Client *client, const char *value, size_t length);
void reply_null(Client *client);
void reply_null_array(Client *client);
void reply_array_header(Client *client, long long count);

int client_read_socket(Client *client);
int client_flush_socket(Client *client);
int client_consume(Client *client, int budget);
void client_free_buffers(Client *client);

static inline int client_pending_output(const Client *client) {
    return client->out_len > client->out_pos;
}

#endif
