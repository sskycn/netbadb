#!/usr/bin/env python3
"""macOS OS-resource probe; build netbadb-server's go_sdk_fixture first.

Core lifecycle tests use deterministic ownership/channel assertions. This optional
probe additionally checks real descriptors and threads after 1,000 TCP failures.
"""

from __future__ import annotations

import json
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "target/debug/examples/go_sdk_fixture"


def snapshot(pid: int) -> tuple[int, int]:
    descriptors = subprocess.check_output(
        ["/usr/sbin/lsof", "-a", "-p", str(pid), "-Fn"], text=True, timeout=10
    ).splitlines()
    fd_count = sum(line.startswith("f") and line[1:].isdigit() for line in descriptors)
    threads = subprocess.check_output(
        ["/bin/ps", "-M", "-p", str(pid)], text=True, timeout=10
    ).splitlines()
    return fd_count, len(threads) - 1


def main() -> None:
    if sys.platform != "darwin":
        raise RuntimeError("this optional OS probe uses macOS lsof and ps -M")
    if not FIXTURE.is_file():
        raise RuntimeError("run cargo build -p netbadb-server --example go_sdk_fixture first")
    with tempfile.TemporaryFile(mode="w+") as errors:
        process = subprocess.Popen(
            [str(FIXTURE), "plaintext"], cwd=ROOT, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=errors, text=True,
        )
        try:
            assert process.stdout is not None and process.stdin is not None
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=10):
                    raise TimeoutError("fixture did not publish its ready address")
            ready = json.loads(process.stdout.readline())
            host, port = ready["address"].rsplit(":", 1)
            before = snapshot(process.pid)
            print(f"ready: fds={before[0]} threads={before[1]}", flush=True)
            for completed in range(1, 1_001):
                with socket.create_connection((host, int(port)), timeout=3) as stream:
                    # A complete Native header with bad magic: a connection-fatal
                    # read after worker-session admission, not a SQL mutation.
                    stream.sendall(b"BAD!" + bytes(20))
                    try:
                        if stream.recv(1) != b"":
                            raise AssertionError("malformed frame did not close the connection")
                    except ConnectionResetError:
                        pass
                if completed in (100, 1_000):
                    # EOF precedes listener reaping. Observe actual quiescence;
                    # no sleep duration is treated as proof of cleanup.
                    deadline = time.monotonic() + 5
                    current = snapshot(process.pid)
                    while current != before and time.monotonic() < deadline:
                        current = snapshot(process.pid)
                    print(f"after {completed}: fds={current[0]} threads={current[1]}", flush=True)
                    if current != before:
                        raise AssertionError(f"resources did not return to baseline: {before} -> {current}")
            process.stdin.close()
            status = process.wait(timeout=10)
            if status != 0:
                raise AssertionError(f"fixture shutdown exited {status}")
            print("shutdown: exit 0", flush=True)
        except BaseException:
            errors.seek(0)
            sys.stderr.write(errors.read())
            raise
        finally:
            if process.poll() is None:
                if process.stdin is not None and not process.stdin.closed:
                    process.stdin.close()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=10)


if __name__ == "__main__":
    main()
