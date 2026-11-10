#!/usr/bin/env python3
"""
End-to-end verification on one machine (4 processes on 127.0.0.1).
Maps checks to Assignment2.md minimum requirements.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / ".smoke_logs"

sys.path.insert(0, str(ROOT))
from node import count_primes, send_and_receive  # noqa: E402

PORTS = [5001, 5002, 5003, 5004]


def peer_args(port: int) -> str:
    others = [f"127.0.0.1:{p}" for p in PORTS if p != port]
    return ",".join(others)


class Check:
    def __init__(self) -> None:
        self.passed: list[str] = []
        self.failed: list[str] = []

    def ok(self, name: str, detail: str = "") -> None:
        self.passed.append(name if not detail else f"{name}: {detail}")

    def fail(self, name: str, detail: str) -> None:
        self.failed.append(f"{name}: {detail}")

    def report(self) -> int:
        print("\n=== Assignment verification ===\n")
        for p in self.passed:
            print(f"  PASS  {p}")
        for f in self.failed:
            print(f"  FAIL  {f}")
        print(f"\n{len(self.passed)} passed, {len(self.failed)} failed\n")
        return 1 if self.failed else 0


def launch(node_id: str, port: int) -> subprocess.Popen:
    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"{node_id}.log"
    log_f = log_path.open("w", encoding="utf-8")
    cmd = [
        sys.executable,
        str(ROOT / "node.py"),
        "--id",
        node_id,
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--advertise-host",
        "127.0.0.1",
        "--peers",
        peer_args(port),
        "--master",
        "127.0.0.1:5001",
    ]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        text=True,
    )
    proc._log_f = log_f  # type: ignore[attr-defined]
    proc.log_path = log_path  # type: ignore[attr-defined]
    return proc


def cmd(proc: subprocess.Popen, line: str) -> None:
    assert proc.stdin
    proc.stdin.write(line + "\n")
    proc.stdin.flush()


def read_log(node_id: str) -> str:
    return (LOG_DIR / f"{node_id}.log").read_text(encoding="utf-8", errors="replace")


def log_from(node_id: str, offset: int) -> str:
    return read_log(node_id)[offset:]


def wait_log(node_id: str, needle: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if needle in read_log(node_id):
            return True
        time.sleep(0.2)
    return False


def wait_log_from(node_id: str, offset: int, needle: str, timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if needle in log_from(node_id, offset):
            return True
        time.sleep(0.25)
    return False


def stop_all(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.stdin:
            try:
                cmd(p, "quit")
            except OSError:
                pass
    time.sleep(0.5)
    for p in procs:
        p.kill()
        if getattr(p, "_log_f", None):
            p._log_f.close()  # type: ignore[attr-defined]


def extract_compute_block(log: str) -> tuple[int | None, int | None, list[int]]:
    """Return (total_primes, workers_used, per_worker counts)."""
    m = re.search(r"primes in \[\d+,\d+\] = (\d+) in [\d.]+s \((\d+) workers\)", log)
    total = int(m.group(1)) if m else None
    workers = int(m.group(2)) if m else None
    chunks = [int(x) for x in re.findall(r"count=(\d+)", log)]
    return total, workers, chunks


def main() -> int:
    os.chdir(ROOT)
    if LOG_DIR.exists():
        for f in LOG_DIR.glob("*.log"):
            f.unlink()

    chk = Check()
    procs = [launch(f"p{i}", 5000 + i) for i in range(1, 5)]
    time.sleep(2.5)

    # --- 4 peer nodes ---
    cmd(procs[3], "peers")
    time.sleep(0.5)
    p4 = read_log("p4")
    peer_lines = re.findall(r"^\s+p\d @", p4, re.M)
    if len(peer_lines) >= 3:
        chk.ok("At least 4 peer nodes in mesh", f"p4 sees {len(peer_lines)} remote peers")
    else:
        chk.fail("At least 4 peer nodes", f"p4 log only has {len(peer_lines)} peer lines")

    # Direct ping peer-to-peer (no master)
    for i, port in enumerate(PORTS[1:], start=2):
        r = send_and_receive("127.0.0.1", port, {"type": "ping"})
        if r and r.get("type") == "pong" and r.get("id") == f"p{i}":
            chk.ok(f"Direct TCP to p{i}", "ping/pong without master")
        else:
            chk.fail(f"Direct TCP to p{i}", repr(r))

    # P2P resource lookup from p4 and p2 (different requesters)
    cmd(procs[3], "get sensor-1")
    time.sleep(2)
    if wait_log("p4", "(owner p1)", timeout=10):
        chk.ok("P2P get resource", "p4 fetched sensor-1 from p1")
    else:
        chk.fail("P2P get resource", read_log("p4")[-500:])

    cmd(procs[1], "get sensor-4")
    time.sleep(2)
    if "(owner p4)" in read_log("p2"):
        chk.ok("P2P get across peers", "p2 fetched sensor-4 from p4")
    else:
        chk.fail("P2P get across peers", read_log("p2")[-500:])

    # Cluster compute — full cluster
    start, end = 1, 50000
    expected = count_primes(start, end)
    cmd(procs[3], f"compute {start} {end}")
    wait_log("p4", "[cluster] job", timeout=60)
    time.sleep(0.5)
    p4_after = read_log("p4")
    total, wcount, chunks = extract_compute_block(p4_after)
    if total == expected:
        chk.ok("Cluster result correct", f"primes [{start},{end}] = {expected}")
    else:
        chk.fail("Cluster result correct", f"got {total}, expected {expected}")
    if wcount == 3:
        chk.ok("Workers used", "3 parallel workers")
    else:
        chk.fail("Workers used", f"expected 3, got {wcount}")
    if chunks and sum(chunks) == total:
        chk.ok("Parallel chunks sum", f"chunks={chunks}")
    else:
        chk.fail("Parallel chunks sum", f"chunks={chunks} total={total}")

    # Peer request processed by cluster (JSON RPC straight to master, no repl)
    exp_small = count_primes(1, 10000)
    resp = send_and_receive(
        "127.0.0.1",
        5001,
        {"type": "compute_job", "job_id": "verify-peer", "start": 1, "end": 10000},
        timeout=120.0,
    )
    if resp and resp.get("total_primes") == exp_small:
        chk.ok("Peer triggers cluster", f"RPC job primes={exp_small}")
    else:
        chk.fail("Peer triggers cluster", repr(resp))

    # Remove node p3
    procs[2].kill()
    procs[2].wait(timeout=5)
    deadline = time.time() + 20
    while time.time() < deadline:
        if "removed p3" in read_log("p4"):
            break
        time.sleep(0.5)

    off = len(read_log("p4"))
    cmd(procs[3], "peers")
    time.sleep(0.5)
    tail_peers = log_from("p4", off)
    if "p3 @" not in tail_peers:
        chk.ok("Node removal (membership)", "p3 absent from peer list")
    else:
        chk.fail("Node removal (membership)", tail_peers[-300:])

    off = len(read_log("p4"))
    cmd(procs[3], "get sensor-3")
    time.sleep(12)
    tail_get = log_from("p4", off).lower()
    if "not found" in tail_get:
        chk.ok("Node removal (resource)", "sensor-3 unavailable after p3 stopped")
    else:
        chk.fail("Node removal (resource)", tail_get[-300:])

    off = len(read_log("p4"))
    cmd(procs[3], f"compute {start} {end}")
    wait_log_from("p4", off, "(2 workers)", timeout=90)
    time.sleep(0.3)
    _, wcount2, _ = extract_compute_block(log_from("p4", off))
    if wcount2 == 2:
        chk.ok("Node removal (cluster)", "only 2 workers after p3 down")
    else:
        chk.fail("Node removal (cluster)", f"expected 2 workers, got {wcount2}")

    # Rejoin p3
    procs[2] = launch("p3", 5003)
    time.sleep(2.5)
    off = len(read_log("p4"))
    cmd(procs[3], "get sensor-3")
    time.sleep(4)
    tail_rejoin = log_from("p4", off)
    if "(owner p3)" in tail_rejoin:
        chk.ok("Node added (rejoin)", "sensor-3 available again")
    else:
        chk.fail("Node added (rejoin)", tail_rejoin[-400:])

    off = len(read_log("p4"))
    cmd(procs[3], f"compute {start} {end}")
    wait_log_from("p4", off, "(3 workers)", timeout=90)
    time.sleep(0.3)
    _, wcount3, _ = extract_compute_block(log_from("p4", off))
    if wcount3 == 3:
        chk.ok("Recovery after rejoin", "back to 3 workers")
    else:
        chk.fail("Recovery after rejoin", f"got {wcount3} workers")

    stop_all(procs)
    return chk.report()


if __name__ == "__main__":
    sys.exit(main())
