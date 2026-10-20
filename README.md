# Redis Clone

An in-memory Redis-compatible server written in C while working through the
[CodeCrafters Build Your Own Redis](https://codecrafters.io/challenges/redis)
challenge.

The implementation lives in [`src/main.c`](./src/main.c) and uses TCP sockets
and the Redis Serialization Protocol (RESP).

## What we learned

### Redis protocol and server design

- Redis clients communicate over a TCP connection.
- RESP arrays encode commands such as `*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n`.
- Replies use simple strings, bulk strings, integers, arrays, and null values.
- A server must continue reading commands from the same client connection.
- `select()` lets one process monitor multiple client sockets.
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
- Expiration is checked lazily when a key is accessed.

### C and memory management

- Heap-allocated command arguments must be released after every command.
- Stored values own their memory and are freed when keys are deleted or expire.
- `realloc()` is useful for growing lists, streams, and transaction queues.
- Recursive command execution needs a forward declaration in C.
- Error responses are preferable to silently accepting malformed commands.

### Build tools and platforms

- **CMake** generates the native build files from `CMakeLists.txt`.
- **Ninja** provides a fast build backend for CMake.
- **LLVM/Clang** compiles the C source.
- **LLVM-MinGW** supplies a Windows-compatible toolchain and resource compiler.
- The Redis starter uses POSIX networking headers such as `sys/socket.h` and
  `arpa/inet.h`, so Linux or WSL is the most compatible build environment.
- The CodeCrafters CLI performs the authoritative Linux build and stage tests.

## Features implemented

- TCP server on port `6379`
- Multiple concurrent clients
- RESP command parsing
- `PING` and `ECHO`
- `SET`, `GET`, `MGET`, `MSET`
- `DEL`, `EXISTS`, `TYPE`, `FLUSHALL`
- `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
- `INCR`, `DECR`, `INCRBY`, `DECRBY`
- `APPEND` and `STRLEN`
- `LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `RPOP`
- `XADD`, `XRANGE`, `XREAD`
- `MULTI`, `EXEC`, and `DISCARD`
- `WATCH` and `UNWATCH`
- Basic `INFO`, `ROLE`, `REPLCONF`, `WAIT`, and `CONFIG GET` responses
- Configurable port with `--port <number>`

## Building locally

### Linux or WSL

Install the compiler and build tools:

```bash
sudo apt update
sudo apt install -y build-essential cmake ninja-build
```

Configure and build:

```bash
cmake -S . -B build -G Ninja
cmake --build build
```

Run the server:

```bash
./build/redis
```

Run it on another port:

```bash
./build/redis --port 6380
```

### Windows toolchain

The Windows tools used during development were:

- CMake
- LLVM/Clang
- LLVM-MinGW
- Ninja

The project can configure with these tools, but the source targets POSIX socket
headers. For reliable local compilation, use WSL or another Linux environment.

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
on CodeCrafters' Linux environment. The latest successful submission from this
project was made under the `zakiscoding` Git identity.

## Repository layout

```text
src/main.c             Redis server implementation
CMakeLists.txt         CMake build configuration
your_program.sh        Local build-and-run helper
codecrafters.yml       CodeCrafters buildpack configuration
.codecrafters/         Remote compile and run scripts
```
