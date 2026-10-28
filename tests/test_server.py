"""Behavioral tests for buffering, malformed input, and append-only recovery."""

import os
import socket
import subprocess
import tempfile
import threading
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RedisError(Exception):
    pass


def find_binary(env_name, stem):
    override = os.environ.get(env_name)
    if override:
        return override
    names = [stem + ".exe", stem]
    for folder in ("build", "build-asan", "build-rel"):
        for name in names:
            path = ROOT / folder / name
            if path.exists():
                return str(path)
    raise SystemExit(f"could not find {stem}; build it or set {env_name}")


BIN = find_binary("REDIS_BIN", "redis")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def encode(*args):
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode()
        parts.append(f"${len(arg)}\r\n".encode() + arg + b"\r\n")
    return b"".join(parts)


class Conn:
    def __init__(self, port, timeout=5):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def send_cmd(self, *args):
        self.sock.sendall(encode(*args))

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise EOFError("connection closed")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, size):
        while len(self.buf) < size:
            self._fill()
        data, self.buf = self.buf[:size], self.buf[size:]
        return data

    def read(self):
        line = self._line()
        if not line:
            raise RuntimeError("empty reply")
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest
        if kind == b"-":
            raise RedisError(rest.decode("utf-8", "replace"))
        if kind == b":":
            return int(rest)
        if kind == b"$":
            size = int(rest)
            if size < 0:
                return None
            data = self._exact(size)
            if self._exact(2) != b"\r\n":
                raise RuntimeError("bulk terminator")
            return data
        if kind == b"*":
            size = int(rest)
            if size < 0:
                return None
            return [self.read() for _ in range(size)]
        raise RuntimeError(f"bad reply {line!r}")


class Server:
    def __init__(self, extra):
        self.port = free_port()
        self.logs = []
        self.ready = threading.Event()
        self.proc = subprocess.Popen(
            [BIN, "--port", str(self.port), *extra],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def wait_until_listening(self):
        if not self.ready.wait(5):
            self.kill()
            raise RuntimeError(f"server did not start: {self.logs}")
        return self

    def _read(self):
        try:
            for line in self.proc.stdout:
                self.logs.append(line.rstrip())
                if "listening on port" in line:
                    self.ready.set()
        except Exception:
            self.ready.set()

    def stop(self):
        if self.proc.poll() is not None:
            return
        try:
            conn = Conn(self.port, timeout=2)
            conn.send_cmd("SHUTDOWN")
            try:
                conn.read()
            except (RedisError, EOFError, OSError):
                pass
            conn.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.kill()

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass


def test_ping_set_get_and_binary():
    server = Server([]).wait_until_listening()
    try:
        conn = Conn(server.port)
        conn.send_cmd("PING")
        assert conn.read() == b"PONG"
        conn.send_cmd("ECHO", "xyz")
        assert conn.read() == b"xyz"
        payload = b"a\r\nb\x00c"
        conn.send_cmd("SET", "bin", payload)
        assert conn.read() == b"OK"
        conn.send_cmd("GET", "bin")
        assert conn.read() == payload
        conn.send_cmd("SET", "empty", b"")
        assert conn.read() == b"OK"
        conn.send_cmd("GET", "empty")
        assert conn.read() == b""
        conn.close()
    finally:
        server.stop()


def test_partial_bytes_and_other_client_stays_responsive():
    server = Server([]).wait_until_listening()
    try:
        slow = Conn(server.port)
        other = Conn(server.port)
        payload = encode("PING")
        for index in range(len(payload)):
            slow.sock.sendall(payload[index : index + 1])
            if index == 2:
                other.send_cmd("PING")
                other.sock.settimeout(0.5)
                assert other.read() == b"PONG"
            time.sleep(0.01)
        slow.sock.settimeout(2)
        assert slow.read() == b"PONG"
        slow.close()
        other.close()
    finally:
        server.stop()


def test_incomplete_command_does_not_block_other_clients():
    server = Server([]).wait_until_listening()
    try:
        held = socket.create_connection(("127.0.0.1", server.port), timeout=2)
        held.sendall(b"*1\r\n")
        time.sleep(0.05)
        other = Conn(server.port)
        other.sock.settimeout(0.5)
        other.send_cmd("PING")
        assert other.read() == b"PONG"
        held.sendall(b"$4\r\nPING\r\n")
        held.settimeout(2)
        assert held.recv(16).startswith(b"+PONG")
        held.close()
        other.close()
    finally:
        server.stop()


def test_disconnect_and_malformed_requests():
    server = Server([]).wait_until_listening()
    try:
        broken = socket.create_connection(("127.0.0.1", server.port), timeout=2)
        broken.sendall(b"*1\r\n$4\r\nPI")
        broken.close()
        bad = Conn(server.port)
        bad.sock.sendall(b"PING\r\n")
        try:
            bad.read()
            raise AssertionError("expected a protocol error")
        except RedisError as exc:
            assert "protocol" in str(exc)
        bad.close()
        piped = Conn(server.port)
        piped.sock.sendall(encode("PING") + b"!!!!")
        assert piped.read() == b"PONG"
        try:
            piped.read()
            raise AssertionError("expected a protocol error after PING")
        except (RedisError, EOFError):
            pass
        piped.close()
        again = Conn(server.port)
        again.send_cmd("PING")
        assert again.read() == b"PONG"
        again.sock.sendall(encode("PING") * 200)
        for _ in range(200):
            assert again.read() == b"PONG"
        again.close()
    finally:
        server.stop()


def test_slow_client_does_not_block_and_partial_write_completes():
    server = Server([]).wait_until_listening()
    try:
        conn = Conn(server.port)
        blob = b"z" * 100000
        conn.send_cmd("SET", "big", blob)
        assert conn.read() == b"OK"
        conn.close()
        slow = socket.create_connection(("127.0.0.1", server.port), timeout=2)
        slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        slow.sendall(encode("GET", "big") * 20)
        fast = Conn(server.port)
        fast.sock.settimeout(1)
        fast.send_cmd("PING")
        assert fast.read() == b"PONG"
        fast.close()
        reader = socket.create_connection(("127.0.0.1", server.port), timeout=2)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        reader.sendall(encode("GET", "big"))
        time.sleep(0.05)
        checker = Conn(server.port)
        checker.sock.settimeout(1)
        checker.send_cmd("ECHO", "still-here")
        assert checker.read() == b"still-here"
        reader.settimeout(2)
        data = b""
        while len(data) < len(blob):
            chunk = reader.recv(1024)
            if not chunk:
                break
            data += chunk
        assert blob in data
        slow.close()
        reader.close()
        checker.close()
    finally:
        server.stop()


def test_transactions_lists_streams_and_counters():
    server = Server([]).wait_until_listening()
    try:
        conn = Conn(server.port)
        conn.send_cmd("MULTI")
        assert conn.read() == b"OK"
        conn.send_cmd("SET", "t", "1")
        assert conn.read() == b"QUEUED"
        conn.send_cmd("INCR", "t")
        assert conn.read() == b"QUEUED"
        conn.send_cmd("EXEC")
        assert conn.read() == [b"OK", 2]
        other = Conn(server.port)
        conn.send_cmd("WATCH", "t")
        assert conn.read() == b"OK"
        other.send_cmd("SET", "t", "9")
        assert other.read() == b"OK"
        conn.send_cmd("MULTI")
        assert conn.read() == b"OK"
        conn.send_cmd("SET", "t", "nope")
        assert conn.read() == b"QUEUED"
        conn.send_cmd("EXEC")
        assert conn.read() is None
        conn.send_cmd("RPUSH", "letters", "a", "b", "c")
        assert conn.read() == 3
        conn.send_cmd("LRANGE", "letters", "0", "-1")
        assert conn.read() == [b"a", b"b", b"c"]
        conn.send_cmd("XADD", "events", "*", "field", "value")
        entry_id = conn.read()
        assert b"-" in entry_id
        conn.send_cmd("XRANGE", "events", "-", "+")
        ranged = conn.read()
        assert ranged[0][0] == entry_id
        assert ranged[0][1] == [b"field", b"value"]
        conn.send_cmd("INCR", "n")
        assert conn.read() == 1
        conn.send_cmd("INCR", "n")
        assert conn.read() == 2
        conn.close()
        other.close()
    finally:
        server.stop()


def test_aof_restart_torn_tail_and_garbage():
    with tempfile.TemporaryDirectory() as directory:
        args = ["--dir", directory, "--appendonly", "yes", "--appendfilename", "appendonly.aof",
                "--appendfsync", "always"]
        server = Server(args).wait_until_listening()
        try:
            conn = Conn(server.port)
            conn.send_cmd("SET", "foo", "bar")
            assert conn.read() == b"OK"
            conn.send_cmd("INCR", "n")
            assert conn.read() == 1
            conn.send_cmd("INCR", "n")
            assert conn.read() == 2
            conn.send_cmd("RPUSH", "letters", "a", "b")
            assert conn.read() == 2
            conn.send_cmd("XADD", "events", "*", "field", "value")
            entry_id = conn.read()
            conn.send_cmd("SET", "temp", "x")
            assert conn.read() == b"OK"
            conn.send_cmd("PEXPIRE", "temp", "60000")
            assert conn.read() == 1
            conn.send_cmd("MULTI")
            conn.read()
            conn.send_cmd("SET", "tx", "done")
            conn.read()
            conn.send_cmd("EXEC")
            assert conn.read() == [b"OK"]
            conn.close()
        finally:
            server.stop()

        server = Server(args).wait_until_listening()
        try:
            conn = Conn(server.port)
            conn.send_cmd("GET", "foo")
            assert conn.read() == b"bar"
            conn.send_cmd("GET", "n")
            assert conn.read() == b"2"
            conn.send_cmd("LRANGE", "letters", "0", "-1")
            assert conn.read() == [b"a", b"b"]
            conn.send_cmd("XRANGE", "events", "-", "+")
            assert conn.read()[0][0] == entry_id
            conn.send_cmd("PTTL", "temp")
            ttl = conn.read()
            assert 1000 <= ttl <= 60000
            conn.send_cmd("GET", "tx")
            assert conn.read() == b"done"
            conn.close()
        finally:
            server.stop()

        path = Path(directory) / "appendonly.aof"
        original = path.read_bytes()
        path.write_bytes(original + b"*3\r\n$3\r\nSET\r\n$4\r\nTORN")
        server = Server(args).wait_until_listening()
        try:
            assert any("truncated" in line for line in server.logs)
            conn = Conn(server.port)
            conn.send_cmd("GET", "foo")
            assert conn.read() == b"bar"
            conn.close()
            assert b"TORN" not in path.read_bytes()
        finally:
            server.stop()

        path.write_bytes(path.read_bytes() + b"!!!!")
        failed = Server(args)
        deadline = time.time() + 5
        while failed.proc.poll() is None and time.time() < deadline:
            time.sleep(0.05)
        assert failed.proc.poll() not in (None, 0)
        assert b"!!!!" in path.read_bytes()
        assert b"foo" in path.read_bytes()
        failed.kill()


def test_sync_policy_counters_and_crash_restart():
    with tempfile.TemporaryDirectory() as directory:
        always_args = ["--dir", directory, "--appendonly", "yes", "--appendfilename", "always.aof",
                       "--appendfsync", "always"]
        server = Server(always_args).wait_until_listening()
        try:
            conn = Conn(server.port)
            conn.send_cmd("INFO", "persistence")
            before = info_value(conn.read(), "aof_sync_count")
            for value in range(5):
                conn.send_cmd("SET", "k", str(value))
                assert conn.read() == b"OK"
            conn.send_cmd("INFO", "persistence")
            after = info_value(conn.read(), "aof_sync_count")
            assert int(after) >= int(before) + 5
            conn.send_cmd("CONFIG", "GET", "appendfsync")
            assert conn.read() == [b"appendfsync", b"always"]
            conn.close()
        finally:
            server.stop()

    with tempfile.TemporaryDirectory() as directory:
        every_args = ["--dir", directory, "--appendonly", "yes", "--appendfilename", "every.aof",
                      "--appendfsync", "everysec"]
        server = Server(every_args).wait_until_listening()
        try:
            conn = Conn(server.port)
            conn.send_cmd("INFO", "persistence")
            before = int(info_value(conn.read(), "aof_sync_count"))
            for value in range(5):
                conn.send_cmd("SET", "k", str(value))
                assert conn.read() == b"OK"
            conn.send_cmd("INFO", "persistence")
            quick = int(info_value(conn.read(), "aof_sync_count"))
            assert quick - before <= 1
            time.sleep(2.0)
            conn.send_cmd("INFO", "persistence")
            later = int(info_value(conn.read(), "aof_sync_count"))
            assert later >= before + 1
            conn.send_cmd("SET", "crash", "survived")
            assert conn.read() == b"OK"
            conn.close()
            server.kill()
        except Exception:
            server.kill()
            raise
        server = Server(every_args).wait_until_listening()
        try:
            conn = Conn(server.port)
            conn.send_cmd("GET", "crash")
            assert conn.read() == b"survived"
            conn.send_cmd("CONFIG", "GET", "dir")
            assert conn.read()[1].decode() == directory
            conn.close()
        finally:
            server.stop()


def test_many_clients():
    server = Server([]).wait_until_listening()
    try:
        clients = [Conn(server.port) for _ in range(20)]
        for conn in clients:
            conn.send_cmd("PING")
        for conn in clients:
            assert conn.read() == b"PONG"
            conn.close()
    finally:
        server.stop()


def test_old_server_partial_read_stalls():
    try:
        repro = find_binary("REPRO_BIN", "repro-old-bugs")
    except SystemExit:
        return
    port = free_port()
    proc = subprocess.Popen(
        [repro, "stall-server", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    try:
        line = proc.stdout.readline()
        assert "listening" in line
        held = socket.create_connection(("127.0.0.1", port), timeout=2)
        held.sendall(b"*1\r\n")
        time.sleep(0.1)
        other = socket.create_connection(("127.0.0.1", port), timeout=2)
        other.settimeout(0.4)
        other.sendall(b"PING\r\n")
        timed_out = False
        try:
            data = other.recv(32)
            timed_out = data == b""
        except socket.timeout:
            timed_out = True
        assert timed_out, "old blocking server should not answer the second client"
        held.close()
        other.close()
    finally:
        proc.kill()
        proc.wait(timeout=3)


def info_value(text, key):
    if isinstance(text, bytes):
        text = text.decode()
    for line in text.splitlines():
        if line.startswith(key + ":"):
            return line.split(":", 1)[1]
    raise AssertionError(f"{key} missing from {text!r}")


def main():
    tests = [
        test_ping_set_get_and_binary,
        test_partial_bytes_and_other_client_stays_responsive,
        test_incomplete_command_does_not_block_other_clients,
        test_disconnect_and_malformed_requests,
        test_slow_client_does_not_block_and_partial_write_completes,
        test_transactions_lists_streams_and_counters,
        test_aof_restart_torn_tail_and_garbage,
        test_sync_policy_counters_and_crash_restart,
        test_many_clients,
        test_old_server_partial_read_stalls,
    ]
    failed = 0
    for test in tests:
        try:
            test()
            print(f"ok {test.__name__}")
        except Exception as exc:
            failed += 1
            traceback.print_exc()
            print(f"FAIL {test.__name__}: {exc}")
    if failed:
        print(f"{failed} failed")
        return 1
    print(f"{len(tests)} passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
