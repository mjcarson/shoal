# 306. An upgrade restarted a lab run, and the run discarded its devices under the cluster it left

The follow-up to O131's diagnosis (`w3-ssd-ec21-o131e`, 2026-10-24) ran from `stores-lab.sh`'s copy
in a unit on europa, as every lab run of the store comparison does. At 06:23 UTC an unattended upgrade
of zlib1g made needrestart restart every service that had the old library mapped, and the lab's unit
was one. Its stop waited out its timeout. Then every process in it was killed, so no trap ran, and
the unit started again from its first line over the cluster its killed bench had left up. The second
run's `fresh` could not unmount the store volumes, and it discarded them anyway, under that cluster's
nodes.

Found the same day, filed as item 306 and fixed before the follow-up was taken again.

## Symptom

- **The run lost two of its four captures.**
  - Captures `a-1`, `e-2` and `e-3` had finished.
  - `a-4` had 11 of its 16 runs when its bench was killed at 06:33:03.
  - The second run's first capture was `a-1` again. Its bench refused the hosts ("something already
    listens on 12500" on every host). As `--overwrite` tells it to, it wrote an empty capture over
    the finished one.
- **The samples were lost.** The second run's samplers opened the same files over the samples of
  each connection that the first run had kept.
- **The bench's cluster ran on for twelve hours.** `shoal-storesssdec21-bench` stayed up on all
  three hosts, beside the tmdb nodes that the second run's restore started again.
  - Every governor stayed at `performance`. The second run's hold recorded `performance`, the
    governor it found, and its restore put that back.
  - The killed run's tail of kernel settings had already been put back. The BBR module that lab 3a's
    side c loaded stayed loaded, as its record said.
- **titan's and hyperion's store volumes were emptied under that cluster's nodes.**
  - The second run's `fresh` could not unmount `/srv/stores`, which was busy.
  - Its `wipefs` and its `mkfs.xfs` refused the busy device.
  - Its `blkdiscard -f` discarded all 100 GiB of each.
  - At 07:13 each XFS read its metadata back as zeros and shut down ("Metadata CRC error detected at
    xfs_rmapbt_read_verify ... Shutting down filesystem").
  - titan's node then exited at every start (`StorageDirectoryUnusable`), 8,225 times by 18:56.
- **europa's volume had two loops.** The second run's `devices` put a second loop over the image file
  that the first run's volume was on. Its `devices-down` then removed that file while both loops held
  it. LVM refused to remove a VG whose PV it saw twice.
- **The hosts' record said otherwise.** It records `lvremove ubuntu-vg/stores` on titan and hyperion
  at 06:33:13, and europa's volume removed, though all three were still there.

## Cause

There were three causes, and each was harmless on its own.

**A unit any service manager could restart.**
- After every upgrade, unattended-upgrades runs needrestart. It restarts every service with a process
  that has an upgraded library mapped. Here that library was libz, which ssh links: the samplers'
  connections to titan and hyperion, and every remote command the script runs.
- At 06:23 it restarted thirteen services, `wave3-lab-o131e.service` among them.
- A restart is a stop and then a start. The stop signalled only the unit's main process, the chain
  script (`KillMode=mixed`). That script's trap waits for its foreground child: the `ab`, and the
  bench under it.
- So nothing ended until the stop's 600 seconds ran out. Then every process of the unit got SIGKILL,
  on which no trap runs. The bench's teardown, the `ab`'s restore and the samplers' stop never ran.
- The start then ran the chain again. CLAUDE.md already said a leg ends by `stop`, never by
  `systemctl stop`, for the first half of this. Nothing kept anything else from stopping it.

**A device made fresh without knowing it was free.**
- `fresh` unmounted the device's directory with its errors discarded, and went on.
- `wipefs` and `mkfs.xfs` refuse a busy device. `blkdiscard -f`, told not to open the device
  exclusively, discarded it under its mounted filesystem.
- `fresh` then checked only that its directory was mounted, which the old filesystem still was.
- The bench's own check that nothing listens on its ports came later, at its scan, after the
  devices were made fresh.

**A capture taken again over the one there.** `ab` passes `--overwrite`, which a capture's retries
need. Nothing kept a run from writing a label that a run before it had finished.

Two smaller faults made the aftermath worse:
- The hold recorded each host's governor at its start, so a hold after a killed run recorded that
  run's `performance`.
- `devices` and `devices-down` recorded every change they asked for, whether it was made or refused.

## Evidence

**Established by the hosts' logs.** The device half was then reproduced on a scratch device.

- **The restart:**
  - europa's `unattended-upgrades-dpkg.log` names `systemctl restart fwupd.service polkit.service
    rsyslog.service ssh.service ... wave3-lab-o131e.service zfs-zed.service`, after zlib1g went from
    `1:1.3.dfsg+really1.3.1-1ubuntu3.1` to `3.2` at 06:23:02.
  - `unattended-upgrades.log` says "All upgrades installed" at 06:33:04, after the 600 seconds the
    stop waited.
  - The chain's log has the last capture's arms up to 06:33:01, then `chain: w3-ssd-ec21-o131e` again
    at 06:33:03.
- **The discard.**
  - titan's journal has `blkdiscard -f /dev/ubuntu-vg/stores` at 06:33:06, between the refused
    `wipefs` and `mkfs.xfs`.
  - Its kernel reports the shutdown at 07:12:56, and hyperion's at 07:13:27.
- **Nothing before it.** No host's upgrade log names any lab or store unit restarted but this one, as
  far back as each log goes (2026-10-01). No unattended upgrade ran on any host during the
  comparison's legs (2026-10-18 and 19).
- **The captures it could have reached.** Since then, two lab runs had an upgrade inside or within 30
  seconds of their measured window:
  - this run's lost last capture;
  - `w3-ssd-ec21-o131e-e-3`'s first run of 1 MiB puts at one in flight, whose window had titan's
    upgrade at 06:13:31, twelve seconds in.

  Item 305's stalled capture (2026-10-23, 21:13 to 21:23) is hours from any upgrade. Every host
  upgrades between 06:05 and 06:45.
- **Reproduced:**
  - **The setup.** `fresh`, taken from the script at a786f490, against a 512 MiB XFS on a loop over a
    file, mounted and held by a process whose directory is in it, as a node holds its storage
    directory. `shoal-spike/results/stores-lab-guards.sh` runs it against any copy of the script.
  - **The run's output:**

    ```
    wipefs: error: /dev/loop7: probing initialization failed: Device or resource busy
    mkfs.xfs: /dev/loop7 contains a mounted filesystem
    fresh returned 0
    data: 7f614da9329cd3aebf59b91aadc30bf0  - (was 5525ff2105de511fa085c5beb94b00f9  -)
    xfs_repair exit 1
    Phase 1 - find and verify superblock...
    couldn't verify primary superblock - not enough secondary superblocks with matching geometry !!!
    ```

    The 64 MiB file under the holder read back as zeros: the first sum is that of 64 MiB of zeros.
    The filesystem had no superblock left.
  - **The guards.** `stores-lab-guards.sh` against a786f490's script failed seven of its eight checks.
    The eighth, an idle device made fresh, passes on both scripts.

## The fix

All of it is in `stores-lab.sh`:

- **Its units refuse a manual stop and a restart.** `start`'s and `ab-start`'s units carry
  `RefuseManualStop=yes`, as every chain unit of wave three does.
  - needrestart's restart of such a unit is refused, and its other restarts go on.
  - `stop` signals the script and its bench as it always did, and `systemctl kill` still reaches a
    unit.
- **A hold keeps the unattended upgrades out.**
  - `apt-daily.timer` and `apt-daily-upgrade.timer` are stopped with the scrub and trim timers,
    after any upgrade already running is done.
  - The restore starts them again.
  - A store's nodes are services that needrestart would restart too.
- **Nothing is made fresh under a mount or a cluster.**
  - `fresh` refuses a device that is still mounted once its directory is unmounted, by its directory
    or by itself, before any wipe.
  - It discards without `-f`, so `blkdiscard`'s exclusive open refuses a device that anything
    holds.
  - A filesystem that is not made is fatal.
  - `prepare`, `ab` and a leg refuse to start while any host has a unit of a cluster the bench made
    (`shoal-*-bench*`), and name the command that takes it down.
- **A capture is never taken again over one that is there.** `ab` refuses a label that exists,
  unless `AB_OVERWRITE=1`.
- **The hosts' record says what was done.**
  - A hold keeps the governor that the first hold found, until a restore puts it back.
  - `devices` refuses a volume or a loop that is there already, so it never puts a second loop over
    one file.
  - `devices-down` records a refused removal as refused, and keeps a loop's file while its VG
    cannot be removed.

## Alternatives rejected

- **needrestart told to leave the lab's units alone** (`$nrconf{override_rc}` under
  `/etc/needrestart/conf.d/`). It changes a host's configuration for good, for one script's units,
  where a unit can say so itself. Nor would it keep an upgrade's restart off a store's nodes.
- **The upgrades turned off on the lab hosts.** They are the hosts' security updates. A hold keeps
  them off only while a leg runs, as it keeps the scrub and trim timers off.
- **A chain whose stop ends its bench**, its trap forwarding the signal as `stop` does. That would
  have put the hosts back, but the restart's start would still have run the chain again. A store's
  node restarted mid-capture loses that measurement either way.
- **`fresh` killing whatever holds a device before it wipes it** (`fuser -km`). A leftover cluster is
  the operator's to take down, with its loader's `destroy`, which removes its units too. A capture
  that killed the cluster's processes would leave its units restarting.

## Invariants to uphold

- **A lab unit ends through its script's `stop`, and refuses any other stop.** Every way a run ends
  must run its trap, or the hosts are left held and a cluster up.
- **A device is never wiped, discarded or formatted while it is mounted or held.** The check comes
  before the first destructive command, and a discard is never forced.
- **No run writes over a capture that is already there unless it was asked to**, so a run started
  twice loses no measurement.
- **The governor put back is the one the first hold found.** A record of it outlives a killed run, so
  it is removed only by the restore that uses it.

## Still open

- **The follow-up was lost and is taken again.** Its two finished captures of side e are kept beside
  the new run, `e-3`'s run under titan's upgrade set aside. Its overwritten first capture and the
  last capture's eleven runs are kept as they are and not used.
- **The other lab scripts** (`x7-lab.sh`, `x12-lab.sh`, `x15-lab.sh`) start units of their own without
  `RefuseManualStop`, and `x7-lab.sh` and `x15-lab.sh` discard with `blkdiscard`. None runs now. Each
  is to be changed before it next runs.
- **The comparison's page** says how its hosts were held without the upgrades' timers, which no upgrade
  needed. Its captures taken again are held with them.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `stores-lab-guards.sh` "fresh refuses a device mounted and held" | A busy mounted device is discarded under its filesystem, its data read back as zeros and its superblock gone, and `fresh` returns success |
| `stores-lab-guards.sh` "fresh makes an idle device a new XFS" | The guard refuses a device it should make fresh: no new filesystem, or not the store user's |
| `stores-lab-guards.sh` "a cluster the bench made, left running, is refused" | A unit of a bench cluster on a host does not stop a capture before its devices are made fresh |
| `stores-lab-guards.sh` "a second loop over a file is refused" | `devices` makes a second loop over an image a loop is over, and LVM sees its PV twice |
| `stores-lab-guards.sh` "a loop's VG that cannot be removed keeps its file" | `devices-down` removes the image under a VG still in use and records it removed |
| `stores-lab-guards.sh` "a before and after refuses a capture that is there" | `ab` writes over a capture of its tag that a run before it finished |
| `stores-lab-guards.sh` "a hold waits out an unattended upgrade already running" | A hold starts a leg while an upgrade runs, whose needrestart then restarts services under it |
| `stores-lab-guards.sh` "units refuse a manual stop and a restart" | A leg's or a before and after's unit can be restarted from under its run |

The script runs on europa with sudo against scratch devices, never beside a leg. Against the fixed
script it passed all nine checks, the before and after's with a clean worktree as its side
(`GUARDS_WT`).

## Related

- [Resolved #301](a-settle-waits-for-reclamation.md), the same script's settle.
- [Item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
  which this does not explain.
- [The store comparison](../../object-storage/store-comparison.md) and
  [its optimizations](../../object-storage/store-optimizations.md), whose runs this script makes.
- `shoal-spike/results/stores-host-changes.txt`, whose lines of 2026-10-24 record what was put back by
  hand.
