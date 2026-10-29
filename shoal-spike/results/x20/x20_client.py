"""X20's client: X16's foreground and a tablet-era full scan, against a ScyllaDB of the lab's

    x20_client.py load --hosts 172.16.2.10,172.16.2.4,172.16.2.5
    x20_client.py run --hosts ... --out /out/run.json

`load` makes the keyspace, tablets on and three copies, and fills two tables: `cell`, the foreground's,
and `big`, the one scanned, a kibibyte row a partition each, the shapes of X16's Cell and Big. `run`
drives the cell as X16's bench drives it, four closed-loop workers each one operation at a time,
half reads of a preloaded key and half inserts of a new one, and a third of the way through scans
`big` whole the way ScyllaDB's 2025 advice says to: its tablets read from `system.tablets`, one at a
time a request, each routed to a replica holding it, two in flight a replica shard, `BYPASS CACHE`.
It writes every window's latency and the scan's own figures as one json document.

The post's own scanner, `fee-mendes/tablet-fullscans`, simulates the scheduling with a random tablet
map and a sleep a query, so this is a scanner of the same shape against a real cluster.
"""

import argparse
import bisect
import json
import random
import struct
import sys
import threading
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

try:
    # the driver's murmur3, the partitioner's hash, to find a key inside each tablet
    from cassandra.murmur3 import murmur3
except ImportError:  # pragma: no cover - every build of the driver carries one
    from cassandra.metadata import murmur3  # type: ignore

KEYSPACE = "x20"


def body(seed, key, width):
    """A row's bytes: hex digits of a SplitMix64 stream from the seed and the key, as x16-data makes them"""
    state = (seed ^ ((key * 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF)) & 0xFFFFFFFFFFFFFFFF
    out = []
    length = 0
    while length < width:
        state = (state + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
        mixed = state
        mixed = ((mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
        mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
        mixed ^= mixed >> 31
        piece = f"{mixed:016x}"
        out.append(piece)
        length += len(piece)
    return "".join(out)[:width]


def connect(hosts, profiles=None):
    """A cluster of the hosts with token and shard aware routing, quorum writes and reads at one"""
    default = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        consistency_level=ConsistencyLevel.ONE,
        request_timeout=60,
    )
    every = {EXEC_PROFILE_DEFAULT: default}
    every.update(profiles or {})
    cluster = Cluster(contact_points=hosts, execution_profiles=every, connect_timeout=30)
    return cluster, cluster.connect()


def cmd_load(args):
    """Make the keyspace and its tables and fill them"""
    cluster, session = connect(args.hosts.split(","))
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': true}"
    )
    for table in ("cell", "big"):
        session.execute(f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{table} (id bigint PRIMARY KEY, body text)")
    for table, count, seed in (("cell", args.cell, args.seed), ("big", args.big, args.seed ^ 0xB16)):
        insert = session.prepare(f"INSERT INTO {KEYSPACE}.{table} (id, body) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.QUORUM
        started = time.time()
        done = 0
        # in chunks, so a failure says where it was and memory stays flat
        for first in range(0, count, 100_000):
            keys = range(first, min(first + 100_000, count))
            results = execute_concurrent_with_args(
                session, insert, ((key, body(seed, key, args.width)) for key in keys), concurrency=256
            )
            failed = [result for ok, result in results if not ok]
            if failed:
                sys.exit(f"{table}: {len(failed)} inserts failed from {first}: {failed[0]}")
            done += len(keys)
        took = time.time() - started
        print(f"{table}: {done} rows in {took:.1f}s, {done / took:.0f} rows/s", flush=True)
    cluster.shutdown()


def percentiles(samples):
    """A window's latencies as their count, rate's inputs and percentiles, in milliseconds"""
    if not samples:
        return {"count": 0}
    ordered = sorted(samples)
    pick = lambda q: ordered[min(len(ordered) - 1, int(q * len(ordered)))] * 1000.0
    return {
        "count": len(ordered),
        "p50_ms": pick(0.50),
        "p99_ms": pick(0.99),
        "p999_ms": pick(0.999),
        "max_ms": ordered[-1] * 1000.0,
    }


def tablets_of(session, table):
    """The table's tablets in token order: each one's last token and its replicas as (host id, shard)"""
    table_id = session.execute(
        "SELECT id FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s", (KEYSPACE, table)
    ).one().id
    rows = session.execute("SELECT last_token, replicas FROM system.tablets WHERE table_id = %s", (table_id,))
    tablets = sorted((row.last_token, [(replica[0], replica[1]) for replica in row.replicas]) for row in rows)
    return table_id, tablets


def keys_in_tablets(tablets, count):
    """A key of the table inside each tablet, found by hashing keys as the partitioner does"""
    lasts = [last for last, _ in tablets]
    found = {}
    for key in range(count):
        token = murmur3(struct.pack(">q", key))
        at = bisect.bisect_left(lasts, token)
        if at < len(lasts) and at not in found:
            found[at] = key
            if len(found) == len(lasts):
                break
    return found


def prime_tablets(cluster, table, tablets):
    """Tell the driver every tablet's replicas, so a request routed by a key reaches its shard

    A driver learns a tablet only from a single-partition request's routing payload, and a range
    scan gets none (`docs/dev/tablets-routing-v2.md:108`), so a scanner tells it. Returns whether
    the driver took them; one that did not routes each request to its host alone.
    """
    try:
        from cassandra.tablets import Tablet

        first = -(2**63)
        for last, replicas in tablets:
            cluster.metadata._tablets.add_tablet(KEYSPACE, table, Tablet(first, last, replicas))
            first = last
        return True
    except Exception as error:  # noqa: BLE001 - any driver without the map routes by host
        print(f"tablets not primed, routing by host alone: {error!r}", flush=True)
        return False


def cmd_run(args):
    """The cell driven alone, beside a whole scan of big, and after it"""
    hosts = args.hosts.split(",")
    # an execution profile a host, so a tablet's request goes to a replica of it
    profiles = {
        f"host-{host}": ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy([host]),
            consistency_level=ConsistencyLevel.ONE,
            request_timeout=120,
        )
        for host in hosts
    }
    cluster, session = connect(hosts, profiles)
    # the scan's plan, made before anything is timed
    _, tablets = tablets_of(session, "big")
    keys = keys_in_tablets(tablets, args.big)
    primed = prime_tablets(cluster, "big", tablets)
    address = {host.host_id: host.address for host in cluster.metadata.all_hosts()}
    read = session.prepare(f"SELECT body FROM {KEYSPACE}.cell WHERE id = ?")
    insert = session.prepare(f"INSERT INTO {KEYSPACE}.cell (id, body) VALUES (?, ?)")
    insert.consistency_level = ConsistencyLevel.QUORUM
    scan = session.prepare(f"SELECT id, body FROM {KEYSPACE}.big WHERE token(id) > ? AND token(id) <= ? BYPASS CACHE")
    scan.fetch_size = args.fetch
    # every operation the foreground answered: when it began, its kind, how long it took
    records = []
    lock = threading.Lock()
    stop = threading.Event()
    next_key = [args.cell]
    t0 = time.perf_counter()

    def foreground(seed):
        mine = []
        rng = random.Random(seed)
        while not stop.is_set():
            started = time.perf_counter()
            if rng.random() < 0.5:
                session.execute(read, (rng.randrange(args.cell),))
                kind = "read"
            else:
                with lock:
                    key = next_key[0]
                    next_key[0] += 1
                session.execute(insert, (key, body(args.seed, key, args.width)))
                kind = "insert"
            mine.append((started - t0, kind, time.perf_counter() - started))
        with lock:
            records.extend(mine)

    workers = [threading.Thread(target=foreground, args=(seed,)) for seed in range(args.workers)]
    for worker in workers:
        worker.start()
    time.sleep(args.before)
    # the scan: each tablet given to one of its replicas in turn, two requests in flight a
    # replica shard, each a token range of one tablet read to its end
    queues = {}
    first = -(2**63)
    for index, (last, replicas) in enumerate(tablets):
        replica = replicas[index % len(replicas)]
        queues.setdefault((address[replica[0]], replica[1]), []).append((index, first, last))
        first = last
    scanned = {"rows": 0, "bytes": 0, "tablets": [], "errors": 0}
    scan_lock = threading.Lock()

    def scanner(host, work):
        while True:
            with scan_lock:
                if not work:
                    return
                index, low, high = work.pop()
            started = time.perf_counter()
            bound = scan.bind((low, high))
            if index in keys:
                bound.routing_key = struct.pack(">q", keys[index])
            rows = bytes_ = 0
            try:
                for row in session.execute(bound, execution_profile=f"host-{host}"):
                    rows += 1
                    bytes_ += len(row.body) + 8
            except Exception as error:  # noqa: BLE001 - counted, and the scan goes on
                with scan_lock:
                    scanned["errors"] += 1
                print(f"tablet {index} on {host}: {error!r}", flush=True)
            with scan_lock:
                scanned["rows"] += rows
                scanned["bytes"] += bytes_
                scanned["tablets"].append(time.perf_counter() - started)

    scan_started = time.perf_counter() - t0
    threads = []
    for (host, _shard), work in queues.items():
        for _ in range(args.inflight):
            thread = threading.Thread(target=scanner, args=(host, work))
            thread.start()
            threads.append(thread)
    for thread in threads:
        thread.join()
    scan_ended = time.perf_counter() - t0
    time.sleep(args.after)
    stop.set()
    for worker in workers:
        worker.join()
    end = time.perf_counter() - t0
    # the windows, each kind's latencies in them
    windows = {}
    for name, low, high in (("before", 0.0, scan_started), ("during", scan_started, scan_ended), ("after", scan_ended, end)):
        window = {"secs": high - low}
        for kind in ("read", "insert"):
            latencies = [took for at, k, took in records if k == kind and low <= at < high]
            window[kind] = percentiles(latencies)
            window[kind]["per_sec"] = len(latencies) / max(high - low, 1e-9)
        windows[name] = window
    secs = scan_ended - scan_started
    report = {
        "windows": windows,
        "scan": {
            "tablets": len(tablets),
            "replica_shards": len(queues),
            "inflight_a_shard": args.inflight,
            "shard_routed": primed,
            "rows": scanned["rows"],
            "bytes": scanned["bytes"],
            "errors": scanned["errors"],
            "secs": secs,
            "rows_per_sec": scanned["rows"] / max(secs, 1e-9),
            "mib_per_sec": scanned["bytes"] / max(secs, 1e-9) / (1 << 20),
            "tablet": percentiles(scanned["tablets"]),
        },
        "ratio": {
            kind: (windows["during"][kind].get("p99_ms") or 0) / (windows["before"][kind].get("p99_ms") or 1)
            for kind in ("read", "insert")
        },
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=1)
    print(json.dumps({"ratio": report["ratio"], "scan": {k: report["scan"][k] for k in ("rows", "secs", "mib_per_sec", "shard_routed", "errors")}}), flush=True)
    cluster.shutdown()


def main():
    """Load the tables, or run the cell beside a scan"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["load", "run"])
    parser.add_argument("--hosts", required=True)
    parser.add_argument("--cell", type=int, default=100_000)
    parser.add_argument("--big", type=int, default=3_000_000)
    parser.add_argument("--width", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=16)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--inflight", type=int, default=2)
    parser.add_argument("--fetch", type=int, default=1000)
    parser.add_argument("--before", type=float, default=30.0)
    parser.add_argument("--after", type=float, default=20.0)
    parser.add_argument("--out", default="/out/run.json")
    args = parser.parse_args()
    {"load": cmd_load, "run": cmd_run}[args.command](args)


if __name__ == "__main__":
    main()
