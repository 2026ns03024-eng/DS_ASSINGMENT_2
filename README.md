# Distributed Resource Sharing and Processing (Lab Assignment II)

Python 3 only — no extra packages. Each machine runs `node.py` once. Every node is a **peer** (direct TCP, JSON messages). One node is also the **cluster master**; the others are workers for parallel prime counting.

## Files

| File | Purpose |
|------|---------|
| `node.py` | Peer + cluster node |
| `Assignment2.md` | Assignment brief |

## Default resources (per node id)

| Node id | Resource key | Example value |
|---------|--------------|---------------|
| p1 | `sensor-1` | temp/humidity reading |
| p2 | `sensor-2` | … |
| p3 | `sensor-3` | … |
| p4 | `sensor-4` | … |

## Commands (interactive, on any node)

- `peers` — list known peers
- `get <key>` — P2P lookup (e.g. `get sensor-3`)
- `compute <start> <end>` — send range to cluster master (e.g. `compute 1 200000`)
- `quit` — broadcast leave and exit

While a node is running, its **terminal shows activity** from every other node that talks to it (join gossip, lookup flood / forward / hit, cluster jobs). Each line is tagged with the node id, e.g. `[p2][lookup] recv flood ...`. Heartbeats are silent. Use `--quiet` to turn activity logs off.

## Run on four VMs (same LAN)

Replace `192.168.x.x` with your VM IPs. **VM1** is master. Start VM1 first, then the others.

**VM1 (p1, master):**

```bash
python3 node.py --id p1 --host 0.0.0.0 --port 5000 --advertise-host 192.168.1.11 \
  --peers 192.168.1.12:5000,192.168.1.13:5000,192.168.1.14:5000 \
  --master 192.168.1.11:5000
```

**VM2 (p2):**

```bash
python3 node.py --id p2 --host 0.0.0.0 --port 5000 --advertise-host 192.168.1.12 \
  --peers 192.168.1.11:5000,192.168.1.13:5000,192.168.1.14:5000 \
  --master 192.168.1.11:5000
```

**VM3 (p3):**

```bash
python3 node.py --id p3 --host 0.0.0.0 --port 5000 --advertise-host 192.168.1.13 \
  --peers 192.168.1.11:5000,192.168.1.12:5000,192.168.1.14:5000 \
  --master 192.168.1.11:5000
```

**VM4 (p4):**

```bash
python3 node.py --id p4 --host 0.0.0.0 --port 5000 --advertise-host 192.168.1.14 \
  --peers 192.168.1.11:5000,192.168.1.12:5000,192.168.1.13:5000 \
  --master 192.168.1.11:5000
```

### Firewall

Allow TCP on the node port between VMs, for example on Ubuntu:

```bash
sudo ufw allow 5000/tcp
```

Check connectivity: `ping` between VMs; after starting a node, `nc -zv <peer-ip> 5000`.

### Common VM mistakes

1. **Same `--id` on every machine** — each VM needs a **different** id (`p1`, `p2`, `p3`, `p4`). If all are `p2`, every node owns only `sensor-2` and the demo is wrong.
2. **`--master` must match the master VM’s `advertise-host:port`** — only that node should print `is_master=True`.
3. **Start the master VM first**, then the others (so join messages have someone listening).

### Troubleshooting `[Errno 113] No route to host`

The OS cannot send packets to that IP (routing/firewall/lab network), not a bug in `node.py`.

On **VM A**, test **VM B** (replace with real peer IPs):

```bash
ping -c 3 10.3.21.110
nc -zv 10.3.23.248 5000   # after master is running
```

If `ping` fails, fix lab networking first (wrong IP, VMs on isolated networks, or ask Prayogshala support). If `ping` works but `nc` fails, check the node is running and firewall.

### Example: three lab IPs (add a 4th VM for the assignment minimum)

Master **10.3.23.248** = `p1`. Peers list must include **every other** node’s `IP:5000`.

**10.3.23.248 — p1 (master), start first:**

```bash
python3 node.py --id p1 --host 0.0.0.0 --port 5000 --advertise-host 10.3.23.248 \
  --peers 10.3.21.110:5000,10.3.23.15:5000,10.3.23.XXX:5000 \
  --master 10.3.23.248:5000
```

**10.3.21.110 — p2:**

```bash
python3 node.py --id p2 --host 0.0.0.0 --port 5000 --advertise-host 10.3.21.110 \
  --peers 10.3.23.248:5000,10.3.23.15:5000,10.3.23.XXX:5000 \
  --master 10.3.23.248:5000
```

**10.3.23.15 — p3:**

```bash
python3 node.py --id p3 --host 0.0.0.0 --port 5000 --advertise-host 10.3.23.15 \
  --peers 10.3.23.248:5000,10.3.21.110:5000,10.3.23.XXX:5000 \
  --master 10.3.23.248:5000
```

Replace `10.3.23.XXX` with your 4th VM’s IP. Each node should show `local resources: ['sensor-N']` matching its id.

### Git on each VM

```bash
git clone <your-remote-url>
cd DS-Ass2
# run one of the commands above
```

## Suggested demo (for report screenshots)

On **p4**, after all four nodes show peers:

1. `peers` — four nodes visible  
2. `get sensor-1` — P2P fetch owned by p1  
3. `compute 1 200000` — parallel prime count; note total time and per-worker lines  
4. Stop **p3** (`Ctrl+C` or kill process). On p4: `get sensor-3` (should fail) and `compute 1 200000` again (fewer workers, longer time, same total).  
5. Restart p3; `peers` and another `compute` to show recovery.

## Local smoke test (one laptop)

Use different ports and `127.0.0.1` as advertise host.

**Full checklist (run before the VM demo):**

```bash
python3 scripts/verify_assignment.py
```

Quick smoke: `python3 scripts/smoke_test.py` or `scripts/local_smoke.sh`.

## Architecture (short)

```text
Peers:  lookup/get_resource  --->  direct messages between all peers (flooded with req id)
Cluster: compute  --->  any peer  --->  master  --->  worker_task  --->  workers in parallel
```

## P2P vs cluster (for write-up)

| Aspect | Peer-to-peer | Cluster |
|--------|--------------|---------|
| Communication | Many-to-many; no central peer coordinator for `get` | Workers talk only to master for jobs |
| Resource sharing | Each peer stores its own resources | CPU shared via task chunks |
| Scalability | More peers → more data, more lookup traffic | More workers → faster `compute` |
| Processing | Lookup works if owner is up; flood tolerates partial membership | Job completes with remaining workers if one dies |

## Message types (JSON, one object per line)

`join`, `leave`, `ping`/`pong`, `lookup`, `lookup_hit`, `compute_job`, `compute_result`, `worker_task`, `worker_result`
