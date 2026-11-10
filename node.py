#!/usr/bin/env python3
"""Distributed peer node with optional cluster master/worker compute."""

import argparse
import errno
import json
import socket
import sys
import threading
import time
import uuid
from typing import Any, Dict, Optional, Set, Tuple

PeerAddr = Tuple[str, int]  # (host, port)

DEFAULT_RESOURCES = {
    "p1": {"sensor-1": "temp=22.5C humidity=45%"},
    "p2": {"sensor-2": "temp=19.1C humidity=52%"},
    "p3": {"sensor-3": "temp=24.0C humidity=38%"},
    "p4": {"sensor-4": "temp=21.3C humidity=41%"},
}

HEARTBEAT_INTERVAL = 8
PEER_TIMEOUT = 25
CONNECT_TIMEOUT = 3
READ_BUF = 65536


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2
    return True


def count_primes(start: int, end: int) -> int:
    # end is inclusive
    if start < 2:
        start = 2
    count = 0
    for n in range(start, end + 1):
        if is_prime(n):
            count += 1
    return count


def parse_host_port(s: str) -> PeerAddr:
    if s.count(":") != 1:
        raise ValueError(f"expected host:port, got {s!r}")
    host, port_s = s.split(":", 1)
    return host.strip(), int(port_s.strip())


def addr_key(addr: PeerAddr) -> str:
    return f"{addr[0]}:{addr[1]}"


def send_message(host: str, port: int, msg: dict) -> bool:
    data = json.dumps(msg) + "\n"
    try:
        with socket.create_connection((host, port), timeout=CONNECT_TIMEOUT) as sock:
            sock.sendall(data.encode())
        return True
    except OSError as exc:
        print(f"[net] send to {host}:{port} failed: {exc}", flush=True)
        return False


def ping_peer(host: str, port: int) -> str:
    """Return 'ok', 'refused', or 'error' for liveness checks."""
    data = json.dumps({"type": "ping"}) + "\n"
    try:
        with socket.create_connection((host, port), timeout=CONNECT_TIMEOUT) as sock:
            sock.sendall(data.encode())
            sock.settimeout(5.0)
            buf = b""
            while b"\n" not in buf:
                chunk = sock.recv(READ_BUF)
                if not chunk:
                    return "error"
                buf += chunk
            line = buf.split(b"\n", 1)[0]
            if line:
                resp = json.loads(line.decode())
                if resp.get("type") == "pong":
                    return "ok"
    except ConnectionRefusedError:
        return "refused"
    except OSError as exc:
        if exc.errno in (
            errno.EHOSTUNREACH,
            errno.ENETUNREACH,
            errno.EHOSTDOWN,
            errno.ETIMEDOUT,
            errno.ENETDOWN,
        ):
            return "transient"
        return "error"
    except json.JSONDecodeError:
        return "error"
    return "error"


def send_and_receive(host: str, port: int, msg: dict, timeout: float = 120.0) -> Optional[dict]:
    data = json.dumps(msg) + "\n"
    try:
        with socket.create_connection((host, port), timeout=CONNECT_TIMEOUT) as sock:
            sock.sendall(data.encode())
            sock.settimeout(timeout)
            buf = b""
            while b"\n" not in buf:
                chunk = sock.recv(READ_BUF)
                if not chunk:
                    break
                buf += chunk
            line = buf.split(b"\n", 1)[0]
            if line:
                return json.loads(line.decode())
    except OSError as exc:
        print(f"[net] rpc to {host}:{port} failed: {exc}", flush=True)
    except json.JSONDecodeError as exc:
        print(f"[net] bad json from {host}:{port}: {exc}", flush=True)
    return None


class Node:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        bootstrap_peers: list[PeerAddr],
        master_addr: PeerAddr,
        bind_host: Optional[str] = None,
        verbose: bool = True,
    ):
        self.verbose = verbose
        self.bind_host = bind_host if bind_host is not None else host
        self.bootstrap_peers = bootstrap_peers
        self.node_id = node_id
        self.host = host
        self.port = port
        self.addr: PeerAddr = (host, port)
        self.master_addr = master_addr
        self.is_master = addr_key(self.addr) == addr_key(master_addr)

        self.resources = dict(DEFAULT_RESOURCES.get(node_id, {}))
        self.peers: Dict[str, dict] = {}  # peer_id -> {id, host, port, last_seen}
        self.lock = threading.Lock()
        self.seen_lookups: Set[str] = set()
        self.seen_joins: Set[str] = set()
        self.running = True
        self.pending_lookups: Dict[str, threading.Event] = {}
        self.lookup_results: Dict[str, dict] = {}

        self._server_sock: Optional[socket.socket] = None
        self._threads: list[threading.Thread] = []

    def vlog(self, tag: str, message: str) -> None:
        if self.verbose:
            print(f"[{self.node_id}][{tag}] {message}", flush=True)

    def peer_list(self) -> list[dict]:
        with self.lock:
            return [dict(p) for p in self.peers.values()]

    def add_peer(self, peer_id: str, host: str, port: int) -> bool:
        if peer_id == self.node_id:
            return False
        key = addr_key((host, port))
        with self.lock:
            if peer_id in self.peers:
                self.peers[peer_id]["last_seen"] = time.time()
                return False
            self.peers[peer_id] = {
                "id": peer_id,
                "host": host,
                "port": port,
                "last_seen": time.time(),
                "fail_count": 0,
            }
        self.vlog("peer", f"learned peer {peer_id} at {host}:{port}")
        return True

    def remove_peer(self, peer_id: str) -> None:
        with self.lock:
            if peer_id in self.peers:
                del self.peers[peer_id]
                self.vlog("peer", f"removed {peer_id}")

    def all_peer_addrs_except(self, skip_id: Optional[str] = None) -> list[PeerAddr]:
        out = []
        with self.lock:
            for p in self.peers.values():
                if skip_id and p["id"] == skip_id:
                    continue
                out.append((p["host"], p["port"]))
        return out

    def broadcast(self, msg: dict, skip_id: Optional[str] = None) -> None:
        for host, port in self.all_peer_addrs_except(skip_id):
            send_message(host, port, msg)

    def start(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.bind_host, self.port))
        self._server_sock.listen(32)
        print(
            f"[init] {self.node_id} listen {self.bind_host}:{self.port} "
            f"advertise {self.host}:{self.port} "
            f"master={addr_key(self.master_addr)} is_master={self.is_master}",
            flush=True,
        )
        print(f"[init] local resources: {list(self.resources.keys())}", flush=True)

        accept_t = threading.Thread(target=self._accept_loop, daemon=True)
        heartbeat_t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        accept_t.start()
        heartbeat_t.start()
        self._threads.extend([accept_t, heartbeat_t])

        time.sleep(0.3)
        self._bootstrap_join()

    def stop(self) -> None:
        self.running = False
        leave_msg = {"type": "leave", "id": self.node_id}
        self.broadcast(leave_msg)
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass

    def _bootstrap_join(self) -> None:
        join_msg = {
            "type": "join",
            "id": self.node_id,
            "host": self.host,
            "port": self.port,
        }
        for host, port in self.bootstrap_peers:
            if addr_key((host, port)) == addr_key(self.addr):
                continue
            self.vlog("peer", f"send join -> {host}:{port}")
            send_message(host, port, join_msg)

    def _accept_loop(self) -> None:
        assert self._server_sock
        while self.running:
            try:
                self._server_sock.settimeout(1.0)
                conn, _ = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_connection, args=(conn,), daemon=True).start()

    def _read_lines(self, conn: socket.socket) -> list[dict]:
        buf = b""
        messages = []
        while True:
            try:
                chunk = conn.recv(READ_BUF)
            except OSError:
                break
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    messages.append(json.loads(line.decode()))
                except json.JSONDecodeError:
                    pass
        return messages

    def _handle_connection(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(120.0)
            buf = b""
            while True:
                if b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                else:
                    chunk = conn.recv(READ_BUF)
                    if not chunk:
                        break
                    buf += chunk
                    continue
                if not line.strip():
                    continue
                msg = json.loads(line.decode())
                reply = self._dispatch(msg, conn)
                if reply is not None:
                    conn.sendall((json.dumps(reply) + "\n").encode())
                    if msg.get("type") in ("compute_job", "worker_task"):
                        conn.close()
                        return
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[net] connection error: {exc}", flush=True)
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _dispatch(self, msg: dict, conn: socket.socket) -> Optional[dict]:
        mtype = msg.get("type")

        if mtype == "join":
            pid = msg["id"]
            host = msg["host"]
            port = int(msg["port"])
            join_key = f"{pid}:{host}:{port}"
            self.vlog("peer", f"recv join from {pid} at {host}:{port}")
            if join_key not in self.seen_joins:
                self.seen_joins.add(join_key)
                if self.add_peer(pid, host, port):
                    targets = self.all_peer_addrs_except(pid)
                    self.vlog("peer", f"gossip join {pid} -> {len(targets)} peer(s)")
                    forward = dict(msg)
                    self.broadcast(forward, skip_id=pid)
            else:
                self.add_peer(pid, host, port)
            return None

        if mtype == "leave":
            self.vlog("peer", f"recv leave from {msg['id']}")
            self.remove_peer(msg["id"])
            return None

        if mtype == "ping":
            return {"type": "pong", "id": self.node_id}

        if mtype == "lookup":
            req_id = msg["req_id"]
            key = msg["key"]
            from_id = msg.get("from_id", "?")
            with self.lock:
                if req_id in self.seen_lookups:
                    self.vlog("lookup", f"dup req={req_id} key={key} (drop)")
                    return None
                self.seen_lookups.add(req_id)
            self.vlog("lookup", f"recv flood req={req_id} key={key} from={from_id}")
            if key in self.resources:
                reply_to = msg["reply_to"]
                self.vlog(
                    "lookup",
                    f"HIT key={key} -> reply to {reply_to} (owner {self.node_id})",
                )
                hit = {
                    "type": "lookup_hit",
                    "req_id": req_id,
                    "key": key,
                    "value": self.resources[key],
                    "owner_id": self.node_id,
                }
                reply_host, reply_port = parse_host_port(msg["reply_to"])
                send_message(reply_host, reply_port, hit)
                return None
            targets = self.all_peer_addrs_except(msg.get("from_id"))
            self.vlog("lookup", f"forward req={req_id} key={key} -> {len(targets)} peer(s)")
            fwd = dict(msg)
            self.broadcast(fwd, skip_id=msg.get("from_id"))
            return None

        if mtype == "lookup_hit":
            req_id = msg["req_id"]
            owner = msg.get("owner_id", "?")
            key = msg.get("key", "?")
            self.vlog("lookup", f"recv answer req={req_id} key={key} owner={owner}")
            with self.lock:
                self.lookup_results[req_id] = msg
                ev = self.pending_lookups.get(req_id)
                if ev:
                    ev.set()
            return None

        if mtype == "compute_job":
            if not self.is_master:
                return {"type": "error", "message": "not master"}
            start = int(msg["start"])
            end = int(msg["end"])
            job_id = msg.get("job_id", str(uuid.uuid4())[:8])
            who = msg.get("from_id", "?")
            self.vlog(
                "cluster",
                f"master recv job={job_id} range=[{start},{end}] from={who}",
            )
            result = self._run_cluster_job(job_id, start, end)
            self.vlog(
                "cluster",
                f"master done job={job_id} total={result.get('total_primes')} "
                f"workers={result.get('workers_used')}",
            )
            return result

        if mtype == "worker_task":
            start = int(msg["start"])
            end = int(msg["end"])
            job_id = msg.get("job_id", "?")
            chunk = msg.get("chunk_id", "?")
            self.vlog(
                "cluster",
                f"worker task job={job_id} chunk={chunk} range=[{start},{end}]",
            )
            t0 = time.time()
            cnt = count_primes(start, end)
            elapsed = time.time() - t0
            self.vlog(
                "cluster",
                f"worker done job={job_id} chunk={chunk} count={cnt} ({elapsed:.3f}s)",
            )
            return {
                "type": "worker_result",
                "job_id": msg.get("job_id"),
                "chunk_id": msg.get("chunk_id"),
                "worker_id": self.node_id,
                "start": start,
                "end": end,
                "count": cnt,
                "elapsed_sec": round(elapsed, 3),
            }

        return None

    def _run_cluster_job(self, job_id: str, start: int, end: int) -> dict:
        workers = self._live_workers()
        if not workers:
            return {
                "type": "compute_result",
                "job_id": job_id,
                "error": "no workers available",
                "total_primes": 0,
            }

        n = len(workers)
        span = end - start + 1
        chunk_size = max(1, (span + n - 1) // n)
        tasks = []
        cur = start
        for i in range(n):
            chunk_end = min(end, cur + chunk_size - 1)
            if cur > end:
                break
            tasks.append((i, workers[i], cur, chunk_end))
            cur = chunk_end + 1

        t0 = time.time()
        details: list[dict] = []
        threads = []
        results_lock = threading.Lock()
        result_holder: list[int] = []

        def run_one(chunk_id: int, worker: dict, cstart: int, cend: int) -> None:
            task = {
                "type": "worker_task",
                "job_id": job_id,
                "chunk_id": chunk_id,
                "start": cstart,
                "end": cend,
            }
            resp = send_and_receive(worker["host"], worker["port"], task, timeout=600.0)
            with results_lock:
                if resp and resp.get("type") == "worker_result":
                    details.append(resp)
                    result_holder.append(resp["count"])
                else:
                    details.append(
                        {
                            "worker_id": worker.get("id"),
                            "error": "no response",
                            "start": cstart,
                            "end": cend,
                        }
                    )

        for chunk_id, worker, cstart, cend in tasks:
            t = threading.Thread(
                target=run_one,
                args=(chunk_id, worker, cstart, cend),
                daemon=True,
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        total = sum(result_holder)
        elapsed = time.time() - t0
        out = {
            "type": "compute_result",
            "job_id": job_id,
            "range": [start, end],
            "workers_used": len([d for d in details if "count" in d]),
            "total_primes": total,
            "elapsed_sec": round(elapsed, 3),
            "per_worker": details,
        }
        return out

    def _live_workers(self) -> list[dict]:
        master_key = addr_key(self.master_addr)
        workers = []
        with self.lock:
            for p in self.peers.values():
                if addr_key((p["host"], p["port"])) == master_key:
                    continue
                workers.append(dict(p))
        alive = []
        for w in workers:
            resp = send_and_receive(w["host"], w["port"], {"type": "ping"}, timeout=5.0)
            if resp and resp.get("type") == "pong":
                alive.append(w)
        return alive

    def _heartbeat_loop(self) -> None:
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            now = time.time()
            to_remove = []
            with self.lock:
                peer_ids = list(self.peers.keys())
            for pid in peer_ids:
                with self.lock:
                    p = self.peers.get(pid)
                if not p:
                    continue
                status = ping_peer(p["host"], p["port"])
                if status == "ok":
                    with self.lock:
                        if pid in self.peers:
                            self.peers[pid]["last_seen"] = now
                            self.peers[pid]["fail_count"] = 0
                elif status == "refused":
                    self.vlog("peer", f"heartbeat {pid} connection refused (removing)")
                    to_remove.append(pid)
                elif status == "transient":
                    self.vlog(
                        "peer",
                        f"heartbeat {pid} unreachable (No route / timeout — keeping peer)",
                    )
                else:
                    with self.lock:
                        if pid in self.peers:
                            self.peers[pid]["fail_count"] = self.peers[pid].get("fail_count", 0) + 1
                            if self.peers[pid]["fail_count"] >= 5:
                                self.vlog("peer", f"heartbeat {pid} failed 5x (removing)")
                                to_remove.append(pid)
            for pid in to_remove:
                self.remove_peer(pid)
            with self.lock:
                empty = len(self.peers) == 0
            if empty and self.bootstrap_peers:
                self.vlog("peer", "no peers known — re-sending join to bootstrap list")
                self._bootstrap_join()

    def lookup_resource(self, key: str, timeout: float = 10.0) -> Optional[dict]:
        if key in self.resources:
            return {
                "key": key,
                "value": self.resources[key],
                "owner_id": self.node_id,
                "local": True,
            }
        req_id = str(uuid.uuid4())[:12]
        ev = threading.Event()
        with self.lock:
            self.pending_lookups[req_id] = ev
        msg = {
            "type": "lookup",
            "req_id": req_id,
            "key": key,
            "from_id": self.node_id,
            "reply_to": addr_key(self.addr),
        }
        targets = self.all_peer_addrs_except(None)
        self.vlog(
            "lookup",
            f"start flood req={req_id} key={key} -> {len(targets)} peer(s)",
        )
        self.broadcast(msg)
        if ev.wait(timeout):
            with self.lock:
                return self.lookup_results.get(req_id)
        print(f"[lookup] timeout for {key}", flush=True)
        return None

    def submit_compute(self, start: int, end: int) -> None:
        host, port = self.master_addr
        job_id = str(uuid.uuid4())[:8]
        msg = {
            "type": "compute_job",
            "job_id": job_id,
            "start": start,
            "end": end,
            "from_id": self.node_id,
        }
        print(f"[cluster] submitting job {job_id} to master {host}:{port} ...", flush=True)
        resp = send_and_receive(host, port, msg, timeout=600.0)
        if not resp:
            print("[cluster] no response from master", flush=True)
            return
        if resp.get("error"):
            print(f"[cluster] error: {resp['error']}", flush=True)
            return
        print(
            f"[cluster] job {resp.get('job_id')} primes in [{start},{end}] "
            f"= {resp.get('total_primes')} in {resp.get('elapsed_sec')}s "
            f"({resp.get('workers_used')} workers)",
            flush=True,
        )
        for row in resp.get("per_worker", []):
            if "count" in row:
                print(
                    f"         worker {row['worker_id']} [{row['start']}-{row['end']}] "
                    f"count={row['count']} ({row.get('elapsed_sec')}s)",
                    flush=True,
                )
            else:
                print(f"         worker {row.get('worker_id')} FAILED {row}", flush=True)

    def repl(self) -> None:
        print(
            "Commands: peers | get <key> | compute <start> <end> | quit",
            flush=True,
        )
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].lower()
            if cmd == "quit":
                self.stop()
                break
            if cmd == "peers":
                peers = self.peer_list()
                if not peers:
                    print("[peer] no known peers", flush=True)
                for p in peers:
                    print(f"  {p['id']} @ {p['host']}:{p['port']}", flush=True)
                continue
            if cmd == "get" and len(parts) >= 2:
                key = parts[1]
                hit = self.lookup_resource(key)
                if hit:
                    owner = hit.get("owner_id", "?")
                    print(f"[lookup] {key} = {hit.get('value')} (owner {owner})", flush=True)
                else:
                    print(f"[lookup] {key} not found", flush=True)
                continue
            if cmd == "compute" and len(parts) >= 3:
                try:
                    a, b = int(parts[1]), int(parts[2])
                except ValueError:
                    print("usage: compute <start> <end>", flush=True)
                    continue
                if a > b:
                    a, b = b, a
                self.submit_compute(a, b)
                continue
            print("unknown command", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="P2P resource node with cluster compute")
    parser.add_argument("--id", required=True, help="node id, e.g. p1")
    parser.add_argument("--host", default="0.0.0.0", help="bind address")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--advertise-host",
        help="address other nodes use to reach this host (required if --host is 0.0.0.0)",
    )
    parser.add_argument(
        "--peers",
        default="",
        help="comma-separated host:port bootstrap peers",
    )
    parser.add_argument("--master", required=True, help="cluster master host:port")
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="hide peer/lookup/cluster activity logs (errors still print)",
    )
    args = parser.parse_args()

    bootstrap_peers: list[PeerAddr] = []
    if args.peers.strip():
        for item in args.peers.split(","):
            item = item.strip()
            if item:
                bootstrap_peers.append(parse_host_port(item))

    adv = args.advertise_host
    if not adv:
        if args.host in ("0.0.0.0", ""):
            print("error: use --advertise-host when binding to 0.0.0.0", file=sys.stderr)
            sys.exit(1)
        adv = args.host

    master_addr = parse_host_port(args.master)
    bind_host = args.host if args.host else "0.0.0.0"

    node = Node(
        args.id,
        adv,
        args.port,
        bootstrap_peers,
        master_addr,
        bind_host=bind_host,
        verbose=not args.quiet,
    )
    node.start()
    try:
        node.repl()
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
