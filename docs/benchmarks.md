# Benchmarks

Release (`-O2`) binaries from `build-rel`, measured on Windows with an Intel Core i7-14700F. The workload is `redis-bench`: `SET`, 64-byte values, keys rotating through 256 names. Each client thread sends 8 warmup commands before the timed section. RSS is the server's working set. CPU is server user+kernel time divided by wall time (`GetProcessTimes`). A run shorter than the Windows timer quantum reports 0% CPU, so these runs last about 0.4–2 seconds.

When the pipeline is greater than 1, each latency sample is the batch round trip divided by the batch size. The p50 and p99 figures below are from pipeline 1. Pipeline 16 is quoted for throughput.

`epoll` is implemented and was not measured here. This host is Windows, and `--io epoll` exits with `epoll is only available on Linux`. Every number below is the `select()` loop.

## Client count, in memory

`--appendonly no --io select`.

| Clients | Pipeline | Ops/s | p50 | p99 | RSS | CPU |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 34,102 | 27 µs | 53 µs | 16.2 MB | 96% |
| 10 | 1 | 42,942 | 226 µs | 344 µs | 28.3 MB | 97% |
| 50 | 1 | 43,892 | 1.12 ms | 1.38 ms | 33.6 MB | 100% |
| 100 | 1 | 43,642 | 2.26 ms | 3.12 ms | 34.1 MB | 100% |
| 1 | 16 | 67,677 |  |  | 28.2 MB | 95% |
| 10 | 16 | 73,815 |  |  | 47.4 MB | 98% |
| 50 | 16 | 74,210 |  |  | 47.8 MB | 100% |
| 100 | 16 | 74,009 |  |  | 48.5 MB | 100% |

One thread serves every client. Throughput levels off near 44,000 ops/s with pipeline 1 and near 74,000 ops/s with pipeline 16 once there are 10–50 clients. Going from 50 to 100 clients adds latency and does not add throughput. p50 grows with the client count (27 µs, 226 µs, 1.12 ms, 2.26 ms) because each request waits behind the others on that thread. CPU stays at 96–100%, so the limit is the thread, not idle sockets. RSS grows with the connection count: about 16 MB at one client and about 48 MB at 100 pipelined clients. The key set is 256 short strings, so most of that growth is per-client buffers.

## Durability

`--appendonly yes --io select`. The Windows sync is `FlushFileBuffers`, the stand-in for `fdatasync` described in [persistence.md](persistence.md).

| Policy | Clients | Pipeline | Ops/s | p50 | p99 | CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| no | 1 | 1 | 23,949 | 35 µs | 95 µs | 90% |
| everysec | 1 | 1 | 26,002 | 35 µs | 82 µs | 98% |
| always | 1 | 1 | 4,278 | 214 µs | 382 µs | 35% |
| no | 8 | 1 | 30,870 | 250 µs | 422 µs | 99% |
| everysec | 8 | 1 | 30,603 | 250 µs | 468 µs | 99% |
| always | 8 | 1 | 4,225 | 1.73 ms | 3.49 ms | 37% |
| no | 1 | 16 | 41,540 |  |  | 97% |
| everysec | 1 | 16 | 40,312 |  |  | 91% |
| always | 1 | 16 | 4,657 |  |  | 31% |
| no | 8 | 16 | 43,479 |  |  | 95% |
| everysec | 8 | 16 | 45,418 |  |  | 99% |
| always | 8 | 16 | 4,028 |  |  | 39% |

`always` is about 6× slower than `no` at one client with pipeline 1 (23,949 / 4,278) and about 9–11× slower once commands are pipelined. Adding clients does not raise it: 4,278, 4,225, and 4,028 ops/s. CPU falls to 31–39% because the thread is blocked in the flush. `everysec` matches `no` within run-to-run noise. The flush is about once a second, so it rarely lands inside a one-second p99 sample. The price of `everysec` is up to about a second of acknowledged writes on power loss, plus a stall while that flush holds the AOF lock.

Logging itself costs throughput even with no sync. In-memory pipeline-1 throughput is about 34,000–44,000 ops/s; the same runs with `--appendfsync no` are about 24,000–31,000, because every mutation still goes through `write()`.

## epoll

`src/loop.c` has a second loop, compiled only on Linux. It is level-triggered: `EPOLLIN|EPOLLRDHUP` always, and `EPOLLOUT` only while the client still has unsent bytes. After a turn that generated output, including a client that was paused mid-pipeline, `settle()` updates that interest set so the bytes are written when the socket can accept them.

`select()` copies and scans the descriptor set on every turn, and this build rejects a connection whose fd does not fit in `fd_set` (`FD_SETSIZE` is 2048 on Windows). The epoll loop reports the descriptors that are ready and allows 4096 clients. At the rates above the thread is already busy executing commands, so the two loops can be close at 100 clients. The gap is worth measuring when the client count is high and much of the work is polling. `tests/run_benchmarks.py` probes `--io epoll` and, when the server accepts it, repeats the scaling table.

```bash
cmake -S . -B build-rel -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-rel
python tests/run_benchmarks.py
```

The raw rows from the Windows run are in `build/bench-results.txt` after that script.
