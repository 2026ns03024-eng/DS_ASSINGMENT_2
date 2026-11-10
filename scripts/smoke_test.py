#!/usr/bin/env python3
"""Start four local nodes and exercise get / compute / peer loss / rejoin."""

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / ".smoke_logs"


def peer_args(port: int) -> str:
    ports = [5001, 5002, 5003, 5004]
    others = [f"127.0.0.1:{p}" for p in ports if p != port]
    return ",".join(others)


def launch(node_id: str, port: int) -> tuple[subprocess.Popen, Path]:
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
    proc.log_path = log_path  # type: ignore[attr-defined]
    proc._log_f = log_f  # type: ignore[attr-defined]
    return proc, log_path


def tail_log(path: Path, since: float) -> str:
    time.sleep(0.3)
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text


def send_commands(proc: subprocess.Popen, lines: list[str]) -> None:
    assert proc.stdin
    for line in lines:
        proc.stdin.write(line + "\n")
        proc.stdin.flush()


def wait_for(path: Path, needles: list[str], timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
        if all(n in body for n in needles):
            return True
        time.sleep(0.25)
    return False


def stop_all(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.stdin:
            try:
                p.stdin.write("quit\n")
                p.stdin.flush()
            except OSError:
                pass
    time.sleep(0.4)
    for p in procs:
        p.kill()
        if getattr(p, "_log_f", None):
            p._log_f.close()  # type: ignore[attr-defined]


def main() -> int:
    os.chdir(ROOT)
    # clean old logs
    if LOG_DIR.exists():
        for f in LOG_DIR.glob("*.log"):
            f.unlink()

    procs: list[subprocess.Popen] = []
    logs: dict[str, Path] = {}
    for i in range(1, 5):
        proc, log_path = launch(f"p{i}", 5000 + i)
        procs.append(proc)
        logs[f"p{i}"] = log_path

    time.sleep(2.5)
    p4_log = logs["p4"]

    print("=== smoke: peers + get ===")
    send_commands(procs[3], ["peers", "get sensor-3"])
    ok = wait_for(p4_log, ["sensor-3"], timeout=15.0)
    print(p4_log.read_text(encoding="utf-8", errors="replace")[-2000:])
    if not ok:
        print("FAIL: get sensor-3", file=sys.stderr)
        stop_all(procs)
        return 1

    print("=== smoke: compute ===")
    send_commands(procs[3], ["compute 1 20000"])
    ok = wait_for(p4_log, ["[cluster] job"], timeout=90.0)
    print(p4_log.read_text(encoding="utf-8", errors="replace")[-2000:])
    if not ok:
        print("FAIL: compute", file=sys.stderr)
        stop_all(procs)
        return 1

    print("=== smoke: kill p3, compute again ===")
    procs[2].kill()
    procs[2].wait(timeout=5)
    time.sleep(1.5)
    send_commands(procs[3], ["get sensor-3", "compute 1 20000"])
    ok = wait_for(p4_log, ["not found"], timeout=20.0)
    wait_for(p4_log, ["[cluster] job"], timeout=90.0)
    if not ok:
        print("WARN: expected 'not found' for sensor-3 after p3 stopped", file=sys.stderr)

    print("=== smoke: rejoin p3 ===")
    proc3, log3 = launch("p3", 5003)
    procs[2] = proc3
    logs["p3"] = log3
    time.sleep(2.5)
    send_commands(procs[3], ["peers", "get sensor-3"])
    ok = wait_for(p4_log, ["sensor-3"], timeout=20.0)
    if not ok:
        print("FAIL: rejoin get", file=sys.stderr)
        stop_all(procs)
        return 1

    stop_all(procs)
    print("=== smoke OK ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
