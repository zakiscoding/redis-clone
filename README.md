# Redis Clone

An in-memory Redis-compatible server written in C while working through the
[CodeCrafters Build Your Own Redis](https://codecrafters.io/challenges/redis)
challenge, then extended with non-blocking networking, an append-only log, and
a Linux `epoll` loop.

Clients speak the Redis Serialization Protocol (RESP) over TCP. Command
handling is shared by both event loops. Append-only logging is off unless
`--appendonly yes` is passed, so a default CodeCrafters run still starts empty.

Write-ups:

- [Debugging](docs/debugging.md) — a blocking `recv` that froze every client, a stack over-read, and a parse leak, investigated with LLDB and AddressSanitizer
- [Persistence](docs/persistence.md) — append-only recovery, and `fdatasync` on every write versus about once a second
- [Benchmarks](docs/benchmarks.md) — throughput, p50/p99, RSS, and CPU on this machine

## What the measurements show

Release build, `select()`, Intel Core i7-14700F, Windows. `SET` of 64-byte values. `epoll` is implemented for Linux and was not run on this host. Full tables are in [docs/benchmarks.md](docs/benchmarks.md).

The in-memory server is one thread and it saturates. Pipeline-1 throughput levels off near **44,000 ops/s** by 50 clients and stays there at 100. Pipeline 16 tops out near **74,000 ops/s**. p50 latency grows with the number of clients waiting on that thread: **27 µs**, **226 µs**, **1.12 ms**, and **2.26 ms** at 1, 10, 50, and 100 clients. CPU stays at 96–100%. RSS grows from about 16 MB to about 48 MB.

Syncing the append-only log on every write holds the rate near **4,000 ops/s**, about 6× slower at one client and about 9–11× slower once commands are pipelined. CPU falls to roughly 30–40% because the thread is waiting inside the flush (`fdatasync` on Linux, `FlushFileBuffers` on Windows). Syncing about once a second matches the no-sync rate, around 24,000–45,000 ops/s. A power loss can then drop about a second of acknowledged writes. Killing the process does not show that gap: `write()` has already copied the bytes into the kernel page cache.

The original blocking server had three failures, reproduced in `tests/repro_old_bugs.c` and fixed in the current loop:

- A client that sent only `*1\r\n` blocked in `recv`, so `select()` never returned and every other client stalled. LLDB stopped in `recv` ← `read_line_blocking` ← `handle_blocking`.
- Formatting a 402-byte reply used `snprintf`'s untruncated return value as a read length into a 256-byte stack buffer. AddressSanitizer reported a stack-buffer-overflow on the first byte past the array, and LLDB showed `length == 402`.
- A failed parse leaked its argument buffers because the allocation count stayed 0. The parser now allocates only after the command is complete.

GDB is not installed on the development machine. The sessions used LLDB 23.1.1 with the same breakpoint, frame-variable, and backtrace steps. Windows AddressSanitizer in this toolchain has no leak detector, so the suite runs with `detect_leaks=0`.

## What we learned

### Redis protocol and server design

- Redis clients communicate over a TCP connection.
- RESP arrays encode commands such as `*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n`.
- Replies use simple strings, bulk strings, integers, arrays, and null values.
- A server must continue reading commands from the same client connection.
- `select()` lets one process monitor multiple client sockets. On Linux,
  `epoll` reports the sockets that are ready instead of scanning the whole set.
- Sockets are non-blocking. A partial read stays in that client's buffer, and
  the loop goes back to waiting. One slow client cannot stall the others.
- `SO_REUSEADDR` makes local development and repeated test runs reliable.
- Commands should be case-insensitive, while values remain unchanged.

### Data structures and behavior

- Strings support `SET`, `GET`, `MGET`, `MSET`, `APPEND`, counters, and expiry.
- Lists support pushes, pops, ranges, negative indexes, and length queries.
- Streams use ordered `milliseconds-sequence` IDs and field/value pairs.
- Transactions queue commands per client and run them together with `MULTI` and
  `EXEC`.
- `WATCH` uses key versions to abort a transaction when another client
  modifies a watched key.
- Expiration is checked lazily when a key is accessed. Deadlines are Unix
  milliseconds, so an expiry written to the append-only log still means the
  same instant after a restart.

### C and memory management

- Heap-allocated command arguments must be released after every command.
- The RESP parser allocates those arguments only after the command is complete,
  so a partial read or a disconnect does not leak them.
- Stored values own their memory and are freed when keys are deleted or expire.
- `realloc()` is useful for growing lists, streams, transaction queues, and
  per-client buffers.
- Error responses are preferable to silently accepting malformed commands.

### Durability and the event loop

- An append-only log records mutations as RESP and replays them at startup.
- `fdatasync` (and `FlushFileBuffers` on Windows) pushes those bytes to stable
  storage. Syncing every write keeps acknowledged commands across a power loss
  and cuts throughput several times over. Syncing about once a second keeps
  the in-memory rate and can lose about a second of writes if power fails.
- Killing the process does not show that difference: `write()` has already
  copied the bytes into the kernel page cache.

### Build tools and platforms

- **CMake** generates the native build files from `CMakeLists.txt`.
- **Ninja** provides a fast build backend for CMake.
- **LLVM/Clang** compiles the C source.
- **LLVM-MinGW** supplies a Windows-compatible toolchain and resource compiler.
- `epoll` and `fdatasync` are Linux calls. The Windows build uses `select()`
  and `FlushFileBuffers`, and `--io epoll` exits with an error on other systems.
- The CodeCrafters CLI performs the authoritative Linux build and stage tests.

## Features implemented

- TCP server on port `6379` (override with `--port`)
- Non-blocking reads and writes, pipelining, and per-client buffers
- Disconnects, malformed RESP, and slow consumers are isolated to that client
- `select()`, and `epoll` on Linux (`--io select|epoll`)
- Append-only log with `--appendonly`, `--appendfsync always|everysec|no`,
  `--appendfilename`, and `--dir`
- `PING` and `ECHO`
- `SET`, `GET`, `MGET`, `MSET`, including `NX`, `XX`, `EX`, `PX`, `EXAT`, `PXAT`
- `DEL`, `EXISTS`, `TYPE`, `FLUSHALL`
- `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`
- `INCR`, `DECR`, `INCRBY`, `DECRBY`
- `APPEND` and `STRLEN`
- `LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `RPOP`
- `XADD`, `XRANGE`, `XREAD`
- `MULTI`, `EXEC`, and `DISCARD`
- `WATCH` and `UNWATCH`
- `INFO`, `ROLE`, `REPLCONF`, `WAIT`, `CONFIG GET`, `QUIT`, `SHUTDOWN`

## Building locally

### Linux or WSL

```bash
sudo apt update
sudo apt install -y build-essential cmake ninja-build
cmake -S . -B build -G Ninja
cmake --build build
./build/redis --port 6380
```

Release build, used by the benchmark script:

```bash
cmake -S . -B build-rel -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-rel
```

AddressSanitizer:

```bash
cmake -S . -B build-asan -G Ninja -DENABLE_ASAN=ON
cmake --build build-asan
```

On Linux, `ASAN_OPTIONS` can include leak detection. This project's Windows
toolchain cannot, so the test run uses `detect_leaks=0`.

### Windows toolchain

The Windows tools used during development were CMake, LLVM/Clang, LLVM-MinGW,
and Ninja. Configure and build the same way:

```powershell
cmake -S . -B build -G Ninja
cmake --build build
.\build\redis.exe --port 6380
```

## Tests and benchmarks

Protocol, restart, and durability checks:

```powershell
python tests\test_server.py
```

Throughput, latency, RSS, and CPU. The script writes `build/bench-results.txt`.
On Linux it also runs the scaling table with `--io epoll`.

```powershell
python tests\run_benchmarks.py
```

`REDIS_BIN` and `REDIS_BENCH` override the binaries. The script prefers
`build-rel`.

## Testing with CodeCrafters

Install the official CLI:

```powershell
irm https://codecrafters.io/install.ps1 | iex
```

Submit the current project:

```powershell
codecrafters submit
```

The CLI commits and pushes the current changes, then runs the challenge tests
on CodeCrafters' Linux environment. `your_program.sh` and `.codecrafters/`
still build with CMake and exec `build/redis`. Unknown `--` options are ignored
so extra challenge flags do not fail startup.

## Repository layout

```text
src/main.c             Startup and command-line flags
src/loop.c             select() and epoll loops
src/client.c           Buffers and the RESP parser
src/commands.c         Command dispatch
src/store.c            Strings, lists, streams, expiry
src/aof.c              Append-only log and recovery
src/bench.c            Throughput and latency client
tests/test_server.py   Protocol and recovery tests
tests/repro_old_bugs.c Original-bug reproductions
docs/                  Debugging, persistence, and benchmark notes
CMakeLists.txt         CMake build configuration
your_program.sh        Local build-and-run helper
codecrafters.yml       CodeCrafters buildpack configuration
```
