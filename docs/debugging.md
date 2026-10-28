# Debugging

The original server called `select()`, then a blocking `recv`/`send` for whichever socket was readable. Three failures from that design are reproduced in `tests/repro_old_bugs.c`. The current server was then run under AddressSanitizer.

This machine does not have GDB. The investigation used LLDB 23.1.1: a breakpoint, `frame variable`, and `thread backtrace all`. That is the same workflow GDB uses (`break`, `print`, `bt`).

## A partial command froze every client

`repro-old-bugs stall-server PORT` is the old loop. It accepts a connection, reads the `*N` line, and then blocks in `recv` until the rest of the array arrives.

LLDB was attached after the process printed `waiting for the rest of the command`, with a client that had sent only `*1\r\n` and then stopped:

```text
ws2_32.dll`recv
socket_recv(fd=232, len=1) at platform.h:133
read_line_blocking(..., line="*1") at repro_old_bugs.c:32
handle_blocking at repro_old_bugs.c:56
stall_server at repro_old_bugs.c:123
main at repro_old_bugs.c:146
```

`select()` had already reported the socket readable. `handle_blocking` then called `recv` for the next byte and stayed there. The loop cannot accept another connection or read another socket until that `recv` returns. A second client's `PING` waits until the first client finishes or disconnects. `tests/test_server.py` checks that the old server times out the second `PING`, and that the current server answers it within half a second.

The current sockets are non-blocking. Each client has its own input buffer. `resp_parse` returns `RESP_NEED` until a full command is buffered, and the loop goes back to `select()` or `epoll_wait()`. A client is limited to 128 commands per turn, so a large pipeline yields. The input buffer is capped at 8MB and the output buffer at 4MB; a slow consumer that fills the output buffer is disconnected. A malformed request closes that client. A disconnect mid-command frees the buffer and drops the socket.

## Reply formatting read past a stack buffer

The default mode of `repro-old-bugs` formats 399 `A`s as a simple string. `+%s\r\n` is 402 bytes. The destination is `char response[256]`. `snprintf` returns 402, the length it would have written, and the following loop reads `response[i]` through that length.

A breakpoint on `tests/repro_old_bugs.c:14` in an `-O0` binary showed:

- `response` is `char[256]`
- `length` is `402`

AddressSanitizer (`clang -fsanitize=address`, process exit code 3) reported a stack-buffer-overflow: a 1-byte read in `buggy_reply_overread` at that same line. The 256-byte `response` occupied frame offsets `[32, 288)`, and the first out-of-range access was the byte at offset 288, immediately after the array. The loop continues for another 145 bytes (`402 - 256`).

Replies in the current server copy the bytes that were actually produced. A truncated `snprintf` return value is never used as a read length.

## A failed parse leaked the argument buffers

`repro-old-bugs leak` mallocs two argument strings and returns failure without publishing how many it allocated. The caller frees `count` pointers, and `count` is still 0, so both blocks are dropped. The program prints `dropped allocations because count stayed 0`.

This LLVM-MinGW AddressSanitizer build has no LeakSanitizer. Starting it with leak detection enabled prints `detect_leaks is not supported on this platform` and never reaches the server. The leak is visible from the count staying 0. The server test suite was run with `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1` and passed, which checks for the overflows and use-after-free that this toolchain can report.

`resp_parse` walks the whole command and returns `RESP_NEED` or `RESP_ERR` before any `malloc`. It allocates argument buffers only after the command is complete. If a later allocation fails, it frees what it already allocated and returns `RESP_OOM`.

## Reproduce

```powershell
cmake -S . -B build-asan -G Ninja -DENABLE_ASAN=ON
cmake --build build-asan
$env:ASAN_OPTIONS = "detect_leaks=0:halt_on_error=1"
.\build-asan\repro-old-bugs.exe
.\build-asan\repro-old-bugs.exe leak
```

Overflow under LLDB:

```text
lldb -- .\build-asan\repro-old-bugs.exe
breakpoint set --file repro_old_bugs.c --line 14
run
frame variable
bt
```

Stall under LLDB: start `repro-old-bugs stall-server 6397`, send `*1\r\n` and leave the socket open, then `process interrupt` and `thread backtrace all`. The current server's equivalent check is `python tests\test_server.py`.
