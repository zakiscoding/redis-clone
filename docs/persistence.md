# Persistence

The append-only file is off unless the process is started with `--appendonly yes`, so a default CodeCrafters run still begins with an empty database. With it on, every successful mutation is appended as a RESP command and replayed on the next start.

```text
redis --appendonly yes --appendfsync always|everysec|no --dir PATH --appendfilename NAME
```

`everysec` is the default when the log is enabled.

## What gets logged

The log stores the command that actually changed the database:

- `SET` with a relative expiry is rewritten as `SET key value PXAT <unix-ms>`. A timestamp in the file means the same instant after restart. Expiry is stored as wall-clock Unix milliseconds from `timespec_get(TIME_UTC)`. A monotonic clock would not survive a restart.
- `EXPIRE` and `PEXPIRE` are rewritten as `PEXPIREAT`. A deadline that is already in the past is logged as `DEL`.
- `XADD` with `*` is logged with the id the server assigned.
- `INCR`, list commands, `DEL`, `FLUSHALL`, and `PERSIST` are logged as they ran.
- `MULTI`, `EXEC`, and `WATCH` are not logged. Commands inside a transaction are logged when `EXEC` runs them.

Replay uses a stack `Client` with `replay` set, so generated replies are discarded, and `loading` set, so replay does not append the same commands again.

## Recovery

A crash can leave a command cut off at the end of the file. If the trailing bytes are an incomplete RESP command, startup prints a warning, truncates that tail, and keeps every complete command before it.

A protocol error in the middle of the file is different: startup refuses to serve and leaves the file as it is. Truncating there would drop or mis-parse commands that follow the error.

`tests/test_server.py` restarts a server and checks that `SET`, `INCR`, `RPUSH`, a resolved stream id, and `PEXPIRE` come back, that a torn tail is removed, and that a file of garbage (`!!!!`) exits non-zero and is kept.

## always, everysec, and no

On Linux the sync call is `fdatasync(2)`. It writes the file's data to stable storage, including the metadata required to read that data back (the size after an append). It does not flush unrelated inode fields the way `fsync` does. This Windows build has no `fdatasync`; the same function calls `FlushFileBuffers`. Other POSIX platforms use `fsync`.

The three policies differ in when that call runs, not in what is appended:

| Policy | When the data is durable | What a power loss can drop |
| --- | --- | --- |
| `always` | Before the client receives the reply. The logging thread calls the sync under the AOF lock. | Nothing that was acknowledged. |
| `everysec` | A background thread sleeps in 100ms steps and, about once a second, syncs if new bytes were appended. The sync holds the same lock, so writers pause for that one flush. | About one second of acknowledged writes. |
| `no` | `write()` only, until shutdown, which still syncs. | Every write since the previous sync. |

`write()` copies bytes into the kernel page cache. Killing the process leaves that cache intact, so a crash-and-restart test recovers the key for every policy. Only losing power (or an OS crash) distinguishes them. `INFO` is the check that works on a running machine: `aof_sync_count` increases once per logged command for `always`, and about once per second for `everysec`. The test suite requires `always` to add at least one sync per `SET`, and `everysec` to add a sync after a two-second wait.

Measured `SET` rates are in [benchmarks.md](benchmarks.md). On this machine `always` stays near 4,000 ops/s and uses about 30–40% CPU, because the thread spends the rest of its time inside the flush. `no` and `everysec` stay in the 24,000–45,000 ops/s range and sit near 100% CPU. Pipelining does not raise the `always` rate: each logged write waits for the disk. `everysec` matches `no` in a one-second run because the extra flush is rare; its cost is the loss window and an occasional stall while the lock is held inside the sync.
