"""Measure throughput, latency, RSS, and CPU for select() and appendfsync policies."""

import os
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def binary(stem):
    override = os.environ.get("REDIS_BIN" if stem == "redis" else "REDIS_BENCH")
    if override:
        return override
    for folder in ("build-rel", "build"):
        for name in (stem + ".exe", stem):
            path = ROOT / folder / name
            if path.exists():
                return str(path)
    raise SystemExit(f"missing {stem}; build the Release configuration first")


REDIS = binary("redis")
BENCH = binary("redis-bench")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, extra):
        self.port = free_port()
        self.lines = []
        self.ready = threading.Event()
        self.proc = subprocess.Popen(
            [REDIS, "--port", str(self.port), *extra],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._read, daemon=True).start()
        deadline = time.monotonic() + 5
        while not self.ready.is_set():
            if self.proc.poll() is not None or time.monotonic() >= deadline:
                self.stop()
                raise RuntimeError(f"server failed to start: {self.lines}")
            self.ready.wait(0.05)

    def _read(self):
        for line in self.proc.stdout:
            self.lines.append(line.rstrip())
            if "listening on port" in line:
                self.ready.set()

    def stop(self):
        if self.proc.poll() is None:
            self.proc.kill()
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass


def run_bench(server, clients, requests, pipeline):
    command = [
        BENCH,
        "--port", str(server.port),
        "--clients", str(clients),
        "--requests", str(requests),
        "--pipeline", str(pipeline),
        "--datasize", "64",
        "--mode", "set",
        "--server-pid", str(server.proc.pid),
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    line = ""
    for candidate in completed.stdout.splitlines():
        if candidate.startswith("result "):
            line = candidate
    if not line:
        raise RuntimeError(completed.stdout + completed.stderr)
    fields = {}
    for token in line.split()[1:]:
        key, value = token.split("=", 1)
        fields[key] = value
    return fields


def main():
    rows = []
    # Totals are large enough that a run lasts about a second, so process CPU
    # time is above the Windows timer quantum.
    scaling = [
        (1, 1, 15000), (1, 16, 40000),
        (10, 1, 40000), (10, 16, 80000),
        (50, 1, 50000), (50, 16, 80000),
        (100, 1, 50000), (100, 16, 80000),
    ]
    epoll_ok = False
    try:
        probe = Server(["--io", "epoll", "--appendonly", "no"])
        probe.stop()
        epoll_ok = True
    except RuntimeError:
        print("epoll: unavailable on this host", flush=True)
    io_modes = ["select"] + (["epoll"] if epoll_ok else [])
    for io_mode in io_modes:
        print(f"client scaling (appendonly no, {io_mode})", flush=True)
        for clients, pipeline, requests in scaling:
            server = Server(["--appendonly", "no", "--io", io_mode])
            try:
                fields = run_bench(server, clients, requests, pipeline)
            finally:
                server.stop()
            fields.update(case=io_mode, clients=str(clients), pipeline=str(pipeline), fsync="off")
            rows.append(fields)
            print(
                f"  clients={clients} pipeline={pipeline} ops={fields['ops_per_s']} "
                f"p50={fields['p50_us']}us p99={fields['p99_us']}us rss={fields['rss_kb']} cpu={fields['cpu_pct']}",
                flush=True,
            )

    print("durability (select)", flush=True)
    for policy in ("no", "everysec", "always"):
        for clients in (1, 8):
            for pipeline in (1, 16):
                requests = 8000 if policy == "always" else (20000 if clients == 1 else 40000)
                with tempfile.TemporaryDirectory() as directory:
                    server = Server([
                        "--appendonly", "yes",
                        "--appendfsync", policy,
                        "--dir", directory,
                        "--appendfilename", "bench.aof",
                        "--io", "select",
                    ])
                    try:
                        fields = run_bench(server, clients, requests, pipeline)
                    finally:
                        server.stop()
                fields.update(case="aof", clients=str(clients), pipeline=str(pipeline), fsync=policy)
                rows.append(fields)
                print(
                    f"  fsync={policy} clients={clients} pipeline={pipeline} ops={fields['ops_per_s']} "
                    f"p50={fields['p50_us']}us p99={fields['p99_us']}us rss={fields['rss_kb']} cpu={fields['cpu_pct']}",
                    flush=True,
                )

    out = ROOT / "build" / "bench-results.txt"
    with out.open("w", encoding="utf-8") as handle:
        for fields in rows:
            handle.write(
                " ".join(f"{key}={fields[key]}" for key in
                         ("case", "fsync", "clients", "pipeline", "requests", "elapsed_s",
                          "ops_per_s", "p50_us", "p99_us", "rss_kb", "cpu_pct"))
                + "\n"
            )
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
