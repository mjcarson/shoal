"""X14's client, run inside `cephadm shell` with this directory mounted at /x14.

`rados put` is a write_full and cannot write at an offset, so the experiments that need a write of
a few KiB into one shard's unit, timed, use librados through this instead.

    python3 /x14/x14-rados.py fill <pool> <obj> <bytes> <seed>
    python3 /x14/x14-rados.py write <pool> <obj> <offset> <length> <seed>
    python3 /x14/x14-rados.py writes <pool> <obj> <count> <offset0> <stride> <length>
    python3 /x14/x14-rados.py get <pool> <obj>
"""
import hashlib
import random
import sys
import time

import rados


def connect():
    """Connect as the admin with a client timeout, so a blocked op ends and reports itself."""
    # an op held by a stopped OSD would otherwise wait forever (rados_osd_op_timeout is 0)
    cluster = rados.Rados(conffile="/etc/ceph/ceph.conf",
                          conf={"rados_osd_op_timeout": "120"})
    cluster.connect()
    return cluster


def seeded(length, seed):
    """Bytes made from a seed, so the same call makes the same object again."""
    return random.Random(seed).randbytes(length)


def main():
    """Run one subcommand and print one line a result."""
    cmd, pool = sys.argv[1], sys.argv[2]
    cluster = connect()
    ioctx = cluster.open_ioctx(pool)
    if cmd == "fill":
        # a whole object, written once, its digest kept to compare reads with
        obj, size, seed = sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
        data = seeded(size, seed)
        ioctx.write_full(obj, data)
        print(f"fill {pool}/{obj} {size} sha256 {hashlib.sha256(data).hexdigest()}")
    elif cmd == "write":
        # one write at an offset, timed from submission to its acknowledgement
        obj, off, length, seed = sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
        data = seeded(length, seed)
        start = time.monotonic()
        try:
            ioctx.write(obj, data, off)
            print(f"write {pool}/{obj} @{off}+{length} acked {1000 * (time.monotonic() - start):.1f} ms "
                  f"at {time.strftime('%H:%M:%S', time.gmtime())}")
        except rados.Error as err:
            print(f"write {pool}/{obj} @{off}+{length} failed after "
                  f"{1000 * (time.monotonic() - start):.1f} ms: {err}")
    elif cmd == "writes":
        # a run of writes one at a time, each into the same unit of successive stripes
        obj, count, off0, stride, length = (sys.argv[3], int(sys.argv[4]), int(sys.argv[5]),
                                            int(sys.argv[6]), int(sys.argv[7]))
        start = time.monotonic()
        for i in range(count):
            ioctx.write(obj, seeded(length, i), off0 + i * stride)
        elapsed = time.monotonic() - start
        print(f"writes {pool}/{obj} {count} x {length} from @{off0} stride {stride}: "
              f"{1000 * elapsed / count:.2f} ms each")
    elif cmd == "get":
        # the whole object, read as `rados get` would, and its digest
        obj = sys.argv[3]
        try:
            size, _ = ioctx.stat(obj)
            data = ioctx.read(obj, size, 0)
            print(f"get {pool}/{obj} {len(data)} sha256 {hashlib.sha256(data).hexdigest()}")
        except rados.Error as err:
            print(f"get {pool}/{obj} failed: {err}")
    ioctx.close()
    cluster.shutdown()


if __name__ == "__main__":
    main()
