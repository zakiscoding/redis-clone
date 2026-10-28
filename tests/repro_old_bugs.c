/* Reproductions of bugs in the original blocking server.
   overflow: reply length was snprintf's untruncated return value.
   leak: a failed read dropped argument buffers because count stayed 0.
   stall-server: select plus a blocking read lets one partial command freeze the process.
*/
#include "../src/platform.h"

#include <limits.h>

static void buggy_reply_overread(const char *value) {
    char response[256];
    int length = snprintf(response, sizeof(response), "+%s\r\n", value);
    volatile unsigned char acc = 0;
    for (int i = 0; i < length; i++) acc = (unsigned char)(acc + (unsigned char)response[i]);
    if (acc == 1) printf("unreachable %u\n", acc);
}

static int old_read_fail(char **args, size_t *count) {
    args[0] = malloc(4);
    args[1] = malloc(3);
    if (!args[0] || !args[1]) return 0;
    memcpy(args[0], "PING", 4);
    memcpy(args[1], "foo", 3);
    (void)count;
    return 0;
}

static int read_line_blocking(socket_t fd, char *line, size_t capacity) {
    size_t length = 0;
    while (length + 1 < capacity) {
        char ch;
        int n = socket_recv(fd, &ch, 1);
        if (n <= 0) return 0;
        if (ch == '\n') {
            line[length] = '\0';
            return 1;
        }
        if (ch != '\r') line[length++] = ch;
    }
    return 0;
}

static int handle_blocking(socket_t fd) {
    char line[128];
    if (!read_line_blocking(fd, line, sizeof(line))) return 0;
    if (strcmp(line, "PING") == 0) {
        send(fd, "+PONG\r\n", 7, 0);
        return 1;
    }
    if (line[0] != '*') return 0;
    int count = atoi(line + 1);
    if (count < 1 || count > 8) return 0;
    printf("waiting for the rest of the command\n");
    fflush(stdout);
    for (int i = 0; i < count; i++) {
        if (!read_line_blocking(fd, line, sizeof(line)) || line[0] != '$') return 0;
        int bulk = atoi(line + 1);
        if (bulk < 0 || bulk > 1024) return 0;
        char *buf = malloc((size_t)bulk + 1);
        if (!buf) return 0;
        size_t got = 0;
        while (got < (size_t)bulk) {
            int n = socket_recv(fd, buf + got, (size_t)bulk - got);
            if (n <= 0) {
                free(buf);
                return 0;
            }
            got += (size_t)n;
        }
        buf[bulk] = '\0';
        char crlf[2];
        if (socket_recv(fd, crlf, 2) != 2) {
            free(buf);
            return 0;
        }
        if (i == 0 && count == 1 && strcmp(buf, "PING") == 0) send(fd, "+PONG\r\n", 7, 0);
        free(buf);
    }
    return 1;
}

static int stall_server(int port) {
    if (platform_init() != 0) return 1;
    socket_t server = socket(AF_INET, SOCK_STREAM, 0);
    if (server == SOCK_INVALID) return 1;
    int reuse = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, (const char *)&reuse, sizeof(reuse));
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(server, (struct sockaddr *)&address, sizeof(address)) < 0 || listen(server, 16) < 0) return 1;
    socket_t clients[32];
    int count = 0;
    for (int i = 0; i < 32; i++) clients[i] = SOCK_INVALID;
    setbuf(stdout, NULL);
    printf("stall server listening on port %d\n", port);
    for (;;) {
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(server, &readable);
#ifndef _WIN32
        int maxfd = (int)server;
#endif
        for (int i = 0; i < count; i++) {
            FD_SET(clients[i], &readable);
#ifndef _WIN32
            if ((int)clients[i] > maxfd) maxfd = (int)clients[i];
#endif
        }
#ifdef _WIN32
        if (select(0, &readable, NULL, NULL, NULL) < 0) continue;
#else
        if (select(maxfd + 1, &readable, NULL, NULL, NULL) < 0) continue;
#endif
        if (FD_ISSET(server, &readable) && count < 32) {
            socket_t fd = accept(server, NULL, NULL);
            if (fd != SOCK_INVALID) clients[count++] = fd;
        }
        for (int i = 0; i < count; i++) {
            if (clients[i] == SOCK_INVALID || !FD_ISSET(clients[i], &readable)) continue;
            if (!handle_blocking(clients[i])) {
                socket_close(clients[i]);
                clients[i] = clients[count - 1];
                clients[count - 1] = SOCK_INVALID;
                count--;
                i--;
            }
        }
    }
}

int main(int argc, char **argv) {
    const char *mode = argc > 1 ? argv[1] : "overflow";
    if (strcmp(mode, "leak") == 0) {
        char *args[4] = {0};
        size_t count = 0;
        old_read_fail(args, &count);
        for (size_t i = 0; i < count; i++) free(args[i]);
        printf("dropped allocations because count stayed %zu\n", count);
        return 0;
    }
    if (strcmp(mode, "stall-server") == 0) {
        int port = argc > 2 ? atoi(argv[2]) : 6399;
        return stall_server(port);
    }
    char value[400];
    memset(value, 'A', sizeof(value) - 1);
    value[sizeof(value) - 1] = '\0';
    buggy_reply_overread(value);
    printf("overflow read completed\n");
    return 0;
}
