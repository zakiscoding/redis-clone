#ifndef PLATFORM_H
#define PLATFORM_H

#ifndef _WIN32
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE 1
#endif
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#endif

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#ifndef FD_SETSIZE
#define FD_SETSIZE 2048
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <io.h>
typedef SOCKET socket_t;
#define SOCK_INVALID INVALID_SOCKET
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
typedef int socket_t;
#define SOCK_INVALID (-1)
#endif

#ifndef O_BINARY
#define O_BINARY 0
#endif
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

static inline int platform_init(void) {
#ifdef _WIN32
    WSADATA data;
    return WSAStartup(MAKEWORD(2, 2), &data) == 0 ? 0 : -1;
#else
    signal(SIGPIPE, SIG_IGN);
    return 0;
#endif
}

static inline void platform_cleanup(void) {
#ifdef _WIN32
    WSACleanup();
#endif
}

static inline void socket_close(socket_t fd) {
    if (fd == SOCK_INVALID) return;
#ifdef _WIN32
    closesocket(fd);
#else
    close(fd);
#endif
}

static inline int socket_last_error(void) {
#ifdef _WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

static inline int socket_would_block(int err) {
#ifdef _WIN32
    return err == WSAEWOULDBLOCK;
#else
    return err == EAGAIN || err == EWOULDBLOCK;
#endif
}

static inline int socket_is_intr(int err) {
#ifdef _WIN32
    return err == WSAEINTR;
#else
    return err == EINTR;
#endif
}

static inline void socket_perror(const char *what) {
#ifdef _WIN32
    fprintf(stderr, "%s: winsock error %d\n", what, WSAGetLastError());
#else
    perror(what);
#endif
}

static inline int socket_set_nonblocking(socket_t fd) {
#ifdef _WIN32
    u_long mode = 1;
    return ioctlsocket(fd, FIONBIO, &mode) == 0 ? 0 : -1;
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#endif
}

static inline int socket_set_nodelay(socket_t fd) {
    int yes = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char *)&yes, sizeof(yes));
}

static inline int socket_send(socket_t fd, const char *buf, size_t len) {
    int chunk = len > 1048576u ? 1048576 : (int)len;
    return (int)send(fd, buf, chunk, 0);
}

static inline int socket_recv(socket_t fd, char *buf, size_t len) {
    int chunk = len > 1048576u ? 1048576 : (int)len;
    return (int)recv(fd, buf, chunk, 0);
}

static inline int64_t unix_ms(void) {
    struct timespec ts;
    if (timespec_get(&ts, TIME_UTC) != TIME_UTC) return 0;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static inline uint64_t mono_ns(void) {
#ifdef _WIN32
    static LARGE_INTEGER freq = {0};
    LARGE_INTEGER now;
    if (freq.QuadPart == 0) QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&now);
    return (uint64_t)now.QuadPart * 1000000000ULL / (uint64_t)freq.QuadPart;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

static inline void sleep_ms(int ms) {
    if (ms < 0) ms = 0;
#ifdef _WIN32
    Sleep((DWORD)ms);
#else
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
#endif
}

/* fdatasync(2) on Linux. Windows has no fdatasync; FlushFileBuffers is the
   same policy hook (flush file data to stable storage). Other POSIX uses fsync. */
static inline int durable_datasync(int fd) {
#ifdef __linux__
    return fdatasync(fd);
#elif defined(_WIN32)
    HANDLE handle = (HANDLE)_get_osfhandle(fd);
    if (handle == INVALID_HANDLE_VALUE) return -1;
    return FlushFileBuffers(handle) ? 0 : -1;
#else
    return fsync(fd);
#endif
}

#endif
