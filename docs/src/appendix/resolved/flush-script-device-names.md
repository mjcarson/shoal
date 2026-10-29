# 320. The lab's flush counter read one NVMe by name, which a host with two renames at boot

`shoal-spike/results/stores-ab-flushes.py` reads the counters `m16-sample.sh` takes of a host's block
devices every second beside a run, and prints each arm's device writes, flushes and busy share. A
logical volume counts no flushes, so it reads the whole NVMe under a store's volume. It named that
device `nvme0n1` on every host.

A kernel names NVMe drives in the order it finds them at boot. Titan and hyperion have one, so their
`nvme0n1` is always the 970 EVO their stores sit on. Europa has two, the Optane 900P its stores sit
on and its system drive, and their names swapped when europa rebooted on 2026-10-25.

Filed and fixed in one change, found while reading europa's snapshot against its state after the
[store comparison](../../object-storage/store-comparison.md)'s second run.

## Symptom

Nothing published is wrong. Every flush figure the script gave is titan's and hyperion's: the
[group commit delay](../../object-storage/store-optimizations/group-commit-delay.md)'s tables say
"Europa's Optane was not sampled". Run on europa's samples before 2026-10-25, the script would have
printed its system drive's counters under europa's name, where the stores were on the Optane, with
nothing in the table to say which drive it was.

## Cause

```python
# the whole devices to read from each host's samples
DEVICES = ("nvme0n1",)
```

Europa's lsblk before and after its reboot, from the comparison's snapshots (`target/lab/stores/snap/`):

```text
before: nvme0n1 931.5G disk (/boot/efi, /boot, the root's volume group)    nvme1n1 260.8G /optane
after:  nvme1n1 931.5G disk (/boot/efi, /boot, the root's volume group)    nvme0n1 260.8G /optane
```

## Evidence

**Established by reading the script and the snapshots, and reproduced by hand** on europa
(2026-10-26): four seconds of `m16-sample.sh`, which names both `nvme0n1` and `nvme1n1`, and a capture
of one run whose window covers them. Before the fix the script printed one row, `europa nvme0n1`, and
nothing for `nvme1n1`; after it, a row for each.

## The fix

**Every whole NVMe in a host's samples is reported, each by its name**, never a partition or a
volume, which count no flushes. The script's header says that a host's names follow its boot, and
that which drive holds a store is `lsblk -no PKNAME` of the store's device on the host at the time.

## Alternatives rejected

- **Naming europa's drive by hand**, `nvme1n1`. Right until the next boot that swaps them again.
- **Choosing the busiest drive.** A store's drive is not always the busiest: the root's journal and
  logs write to the system drive beside every run.
- **Recording the mapping in every sample.** `m16-sample.sh` is the sampler of every lab since F88,
  and the mapping does not change during a run; one `lsblk` on the host at the time says it.

## Invariants to uphold

- **A device's figures are named by the device they were read from.** A reader matches the name to
  a store with lsblk, never by assuming a name.
- **Flushes are read on a whole NVMe**, since a volume and a partition report none.

## Still open

- **The figures of a host with two drives still need the reader to know which holds the store.**
  Nothing in the samples says so.

## Tests

None in the workspace: the script is a lab script, run by hand beside a lab's captures, and the
reproduction above was run by hand.

## Related

- [The group commit delay](../../object-storage/store-optimizations/group-commit-delay.md), the flush
  figures the script produced, titan's and hyperion's alone.
- [O124](../optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes), whose
  writes a put are counted the same way.
