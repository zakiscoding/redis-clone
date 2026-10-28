#include "loop.h"

#include "client.h"
#include "commands.h"
#include "server.h"

#ifdef __linux__
#include <sys/epoll.h>
#endif

#ifdef _WIN32
#define MAX_CLIENTS 1024
#else
#define MAX_CLIENTS 4096
#endif

static Client clients[MAX_CLIENTS];
#ifdef __linux__
static int epfd = -1;
#endif

#ifdef __linux__
static void epoll_update(Client *client);
#endif

static void settle(Client *client);

static void client_close(Client *client) {
    if (!client || !client->active) return;
    client->active = 0;
#ifdef __linux__
    if (client->in_epoll && epfd >= 0) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, (int)client->fd, NULL);
        client->in_epoll = 0;
    }
#endif
    client_free_transaction_state(client);
    client_free_buffers(client);
    socket_close(client->fd);
    memset(client, 0, sizeof(*client));
    client->fd = SOCK_INVALID;
}

static void settle(Client *client) {
    if (!client->active) return;
    if (client->out_overflow) {
        client_close(client);
        return;
    }
    if (client_pending_output(client)) {
        int rc = client_flush_socket(client);
        if (!client->active) return;
        if (rc < 0) {
            client_close(client);
            return;
        }
    }
    if ((client->close_after_flush || client->peer_closed) && !client_pending_output(client)) {
        client_close(client);
        return;
    }
#ifdef __linux__
    if (client->active && g_io_mode == IO_EPOLL) epoll_update(client);
#endif
}

static void service_input(Client *client) {
    if (!client->active) return;
    client_read_socket(client);
    if (!client->active) return;
    client_consume(client, COMMANDS_PER_TURN);
    if (client->active && client->peer_closed) client->close_after_flush = 1;
    settle(client);
}

static int pump_paused(void) {
    int still = 0;
    for (size_t i = 0; i < MAX_CLIENTS; i++) {
        Client *client = &clients[i];
        if (!client->active || !client->paused) continue;
        client_consume(client, COMMANDS_PER_TURN);
        if (client->active && client->peer_closed) client->close_after_flush = 1;
        settle(client);
        if (client->active && client->paused) still = 1;
    }
    return still;
}

static void try_flush_all(void) {
    for (size_t i = 0; i < MAX_CLIENTS; i++) {
        Client *client = &clients[i];
        if (!client->active) continue;
        if (client_pending_output(client) || client->close_after_flush || client->out_overflow) settle(client);
    }
}

static int any_pending_output(void) {
    for (size_t i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && client_pending_output(&clients[i])) return 1;
    }
    return 0;
}

static void drain_writes(void) {
    for (int i = 0; i < 100 && any_pending_output(); i++) {
        try_flush_all();
        if (!any_pending_output()) break;
        sleep_ms(1);
    }
}

static void close_all_clients(void) {
    for (size_t i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active) client_close(&clients[i]);
    }
}

static void reject_fd(socket_t fd, const char *why) {
    const char *msg = "-ERR max number of clients reached\r\n";
    send(fd, msg, (int)strlen(msg), 0);
    socket_close(fd);
    fprintf(stderr, "%s\n", why);
}

static Client *take_slot(socket_t fd) {
    for (size_t i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active) continue;
        memset(&clients[i], 0, sizeof(clients[i]));
        clients[i].fd = fd;
        clients[i].active = 1;
        return &clients[i];
    }
    return NULL;
}

#ifdef __linux__
static void epoll_add(Client *client) {
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.ptr = client;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, (int)client->fd, &event) < 0) {
        perror("epoll_ctl add");
        client_close(client);
        return;
    }
    client->in_epoll = 1;
    client->epoll_interest = event.events;
}

static void epoll_update(Client *client) {
    if (!client->active || !client->in_epoll) return;
    uint32_t want = EPOLLIN | EPOLLRDHUP;
    if (client_pending_output(client)) want |= EPOLLOUT;
    if (want == client->epoll_interest) return;
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = want;
    event.data.ptr = client;
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, (int)client->fd, &event) < 0) {
        perror("epoll_ctl mod");
        return;
    }
    client->epoll_interest = want;
}
#endif

static void accept_all(socket_t listener) {
    for (;;) {
        socket_t fd = accept(listener, NULL, NULL);
        if (fd == SOCK_INVALID) {
            int err = socket_last_error();
            if (socket_would_block(err) || socket_is_intr(err)) return;
            socket_perror("accept");
            return;
        }
#ifndef _WIN32
        if (g_io_mode == IO_SELECT && fd >= FD_SETSIZE) {
            reject_fd(fd, "rejecting connection: fd does not fit in select fd_set");
            continue;
        }
#endif
        if (socket_set_nonblocking(fd) != 0) {
            socket_close(fd);
            continue;
        }
        socket_set_nodelay(fd);
        Client *client = take_slot(fd);
        if (!client) {
            reject_fd(fd, "rejecting connection: client table is full");
            continue;
        }
#ifdef __linux__
        if (g_io_mode == IO_EPOLL) epoll_add(client);
#endif
    }
}

socket_t server_listen(int port) {
    socket_t server = socket(AF_INET, SOCK_STREAM, 0);
    if (server == SOCK_INVALID) {
        socket_perror("socket");
        return SOCK_INVALID;
    }
    int reuse = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, (const char *)&reuse, sizeof(reuse));
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(server, (struct sockaddr *)&address, sizeof(address)) < 0) {
        socket_perror("bind");
        socket_close(server);
        return SOCK_INVALID;
    }
    if (listen(server, 512) < 0) {
        socket_perror("listen");
        socket_close(server);
        return SOCK_INVALID;
    }
    if (socket_set_nonblocking(server) != 0) {
        socket_perror("nonblocking");
        socket_close(server);
        return SOCK_INVALID;
    }
    return server;
}

static void run_select(socket_t listener) {
    while (!g_stop) {
        int paused = pump_paused();
        fd_set readable;
        fd_set writable;
        FD_ZERO(&readable);
        FD_ZERO(&writable);
        FD_SET(listener, &readable);
#ifndef _WIN32
        int maxfd = (int)listener;
#endif
        for (size_t i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) continue;
#ifndef _WIN32
            if (clients[i].fd >= FD_SETSIZE) {
                fprintf(stderr, "closing fd %d above FD_SETSIZE\n", (int)clients[i].fd);
                client_close(&clients[i]);
                continue;
            }
            if ((int)clients[i].fd > maxfd) maxfd = (int)clients[i].fd;
#endif
            FD_SET(clients[i].fd, &readable);
            if (client_pending_output(&clients[i])) FD_SET(clients[i].fd, &writable);
        }
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = paused ? 0 : 200000;
#ifdef _WIN32
        int ready = select(0, &readable, &writable, NULL, &timeout);
#else
        int ready = select(maxfd + 1, &readable, &writable, NULL, &timeout);
#endif
        if (ready < 0) {
            if (socket_is_intr(socket_last_error())) continue;
            socket_perror("select");
            break;
        }
        if (FD_ISSET(listener, &readable)) accept_all(listener);
        for (size_t i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) continue;
            if (FD_ISSET(clients[i].fd, &readable)) service_input(&clients[i]);
        }
        try_flush_all();
    }
    drain_writes();
    close_all_clients();
}

#ifdef __linux__
static void run_epoll(socket_t listener) {
    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        perror("epoll_create1");
        return;
    }
    struct epoll_event listen_event;
    memset(&listen_event, 0, sizeof(listen_event));
    listen_event.events = EPOLLIN;
    listen_event.data.ptr = NULL;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, (int)listener, &listen_event) < 0) {
        perror("epoll_ctl listen");
        close(epfd);
        epfd = -1;
        return;
    }
    struct epoll_event events[256];
    while (!g_stop) {
        int paused = pump_paused();
        int n = epoll_wait(epfd, events, 256, paused ? 0 : 200);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            uint32_t flags = events[i].events;
            if (events[i].data.ptr == NULL) {
                if (flags & EPOLLIN) accept_all(listener);
                continue;
            }
            Client *client = events[i].data.ptr;
            if (!client->active) continue;
            if (flags & EPOLLIN) service_input(client);
            if (client->active && (flags & EPOLLOUT)) {
                int rc = client_flush_socket(client);
                if (rc < 0 || client->out_overflow ||
                    ((client->close_after_flush || client->peer_closed) && !client_pending_output(client))) {
                    client_close(client);
                }
            }
            if (client->active && (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))) {
                client->peer_closed = 1;
                client->close_after_flush = 1;
                if (!client_pending_output(client)) client_close(client);
            }
            if (client->active) epoll_update(client);
        }
        try_flush_all();
    }
    drain_writes();
    close_all_clients();
    close(epfd);
    epfd = -1;
}
#endif

void run_loop(socket_t listener) {
#ifdef __linux__
    if (g_io_mode == IO_EPOLL) {
        run_epoll(listener);
        return;
    }
#endif
    run_select(listener);
}
