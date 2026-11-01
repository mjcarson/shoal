# Item 351's run

Every figure is a median over the seconds named: *early* is a leg's seconds 10 to 60 and *late* its seconds 120 to its end. A step is where a figure's five second mean went over twice its seconds 10 to 40 and stayed there 30 s.

## The cell's arms

| Arm | Seconds | Inserts a second, early | late | Insert p50 ms, early | late | The fall, s in | Inserts by then |
| --- | --- | --- | --- | --- | --- | --- | --- |
| a | 211 | 2471 | 1454 | 11.47 | 20.57 | 76 | 186812 |
| b | 211 | 2506 | 1454 | 11.61 | 20.88 | 81 | 201771 |

## Each host's disk and node, beside the arms

| Host | Disk | Arm | Figure | Early | Late | Stepped, s in |
| --- | --- | --- | --- | --- | --- | --- |
| europa | nvme0n1 | a | flushes | 0 | 0 | - |
| europa | nvme0n1 | a | ms_flush | - | - | none |
| europa | nvme0n1 | a | writes | 9203 | 5597 | - |
| europa | nvme0n1 | a | ms_write | 0.02 | 0.02 | - |
| europa | nvme0n1 | a | mb | 28.2 | 16.8 | - |
| europa | nvme0n1 | a | busy | 0.16 | 0.10 | - |
| europa | nvme0n1 | a | cpu_shards | 1.09 | 0.71 | - |
| europa | nvme0n1 | a | cpu_control | 0.01 | 0.01 | - |
| europa | nvme0n1 | b | flushes | 0 | 0 | - |
| europa | nvme0n1 | b | ms_flush | - | - | none |
| europa | nvme0n1 | b | writes | 9203 | 5592 | - |
| europa | nvme0n1 | b | ms_write | 0.02 | 0.02 | - |
| europa | nvme0n1 | b | mb | 28.2 | 16.0 | - |
| europa | nvme0n1 | b | busy | 0.16 | 0.10 | - |
| europa | nvme0n1 | b | cpu_shards | 1.10 | 0.71 | - |
| europa | nvme0n1 | b | cpu_control | 0.01 | 0.01 | - |
| hyperion | nvme0n1 | a | flushes | 583 | 324 | - |
| hyperion | nvme0n1 | a | ms_flush | 0.82 | 2.82 | 76 |
| hyperion | nvme0n1 | a | writes | 1695 | 946 | - |
| hyperion | nvme0n1 | a | ms_write | 0.81 | 1.91 | - |
| hyperion | nvme0n1 | a | mb | 7.6 | 4.3 | - |
| hyperion | nvme0n1 | a | busy | 0.42 | 0.39 | - |
| hyperion | nvme0n1 | a | cpu_shards | 2.62 | 1.53 | - |
| hyperion | nvme0n1 | a | cpu_control | 0.02 | 0.01 | - |
| hyperion | nvme0n1 | b | flushes | 585 | 323 | - |
| hyperion | nvme0n1 | b | ms_flush | 0.81 | 2.80 | 80 |
| hyperion | nvme0n1 | b | writes | 1707 | 939 | - |
| hyperion | nvme0n1 | b | ms_write | 0.80 | 1.92 | - |
| hyperion | nvme0n1 | b | mb | 7.5 | 4.2 | - |
| hyperion | nvme0n1 | b | busy | 0.52 | 0.44 | - |
| hyperion | nvme0n1 | b | cpu_shards | 2.64 | 1.55 | - |
| hyperion | nvme0n1 | b | cpu_control | 0.01 | 0.01 | - |
| titan | nvme0n1 | a | flushes | 593 | 324 | - |
| titan | nvme0n1 | a | ms_flush | 0.82 | 2.83 | 75 |
| titan | nvme0n1 | a | writes | 1726 | 946 | - |
| titan | nvme0n1 | a | ms_write | 0.80 | 1.93 | - |
| titan | nvme0n1 | a | mb | 7.8 | 4.3 | - |
| titan | nvme0n1 | a | busy | 0.43 | 0.40 | - |
| titan | nvme0n1 | a | cpu_shards | 2.55 | 1.52 | - |
| titan | nvme0n1 | a | cpu_control | 0.02 | 0.02 | - |
| titan | nvme0n1 | b | flushes | 582 | 321 | - |
| titan | nvme0n1 | b | ms_flush | 0.81 | 2.79 | 78 |
| titan | nvme0n1 | b | writes | 1709 | 941 | - |
| titan | nvme0n1 | b | ms_write | 0.81 | 1.93 | - |
| titan | nvme0n1 | b | mb | 7.4 | 4.1 | - |
| titan | nvme0n1 | b | busy | 0.51 | 0.45 | - |
| titan | nvme0n1 | b | cpu_shards | 2.64 | 1.57 | - |
| titan | nvme0n1 | b | cpu_control | 0.02 | 0.02 | - |

## Each member's own figures, beside the arms

| Member | Arm | Figure | Early | Late |
| --- | --- | --- | --- | --- |
| europa | a | wal_syncs_per_sec | 4338.80 | 2629.93 |
| europa | a | wal_sync_ms | 0.18 | 0.19 |
| europa | a | wal_appends_per_sync | 0.56 | 0.55 |
| europa | a | wal_segments | 374.00 | 401.00 |
| europa | a | compacting_segments | 0.00 | 0.00 |
| europa | a | apply_lag | 0.00 | 0.00 |
| europa | a | pending_bytes | 14148.00 | 16768.00 |
| europa | b | wal_syncs_per_sec | 4362.85 | 2621.88 |
| europa | b | wal_sync_ms | 0.18 | 0.19 |
| europa | b | wal_appends_per_sync | 0.57 | 0.55 |
| europa | b | wal_segments | 417.50 | 444.00 |
| europa | b | compacting_segments | 0.00 | 0.00 |
| europa | b | apply_lag | 0.00 | 0.00 |
| europa | b | pending_bytes | 14672.00 | 16768.00 |
| hyperion | a | wal_syncs_per_sec | 591.93 | 320.97 |
| hyperion | a | wal_sync_ms | 6.14 | 13.28 |
| hyperion | a | wal_appends_per_sync | 4.04 | 4.38 |
| hyperion | a | wal_segments | 372.00 | 398.00 |
| hyperion | a | compacting_segments | 0.00 | 0.00 |
| hyperion | a | apply_lag | 0.00 | 0.00 |
| hyperion | a | pending_bytes | 5240.00 | 6288.00 |
| hyperion | b | wal_syncs_per_sec | 579.94 | 321.97 |
| hyperion | b | wal_sync_ms | 6.33 | 13.25 |
| hyperion | b | wal_appends_per_sync | 4.16 | 4.35 |
| hyperion | b | wal_segments | 416.00 | 443.00 |
| hyperion | b | compacting_segments | 0.00 | 0.00 |
| hyperion | b | apply_lag | 0.00 | 0.00 |
| hyperion | b | pending_bytes | 5240.00 | 6288.00 |
| titan | a | wal_syncs_per_sec | 591.95 | 325.96 |
| titan | a | wal_sync_ms | 6.11 | 13.15 |
| titan | a | wal_appends_per_sync | 3.96 | 4.35 |
| titan | a | wal_segments | 372.00 | 398.00 |
| titan | a | compacting_segments | 0.00 | 0.00 |
| titan | a | apply_lag | 0.00 | 0.00 |
| titan | a | pending_bytes | 6288.00 | 6288.00 |
| titan | b | wal_syncs_per_sec | 591.93 | 321.97 |
| titan | b | wal_sync_ms | 6.13 | 13.21 |
| titan | b | wal_appends_per_sync | 4.15 | 4.35 |
| titan | b | wal_segments | 415.00 | 443.00 |
| titan | b | compacting_segments | 0.00 | 0.00 |
| titan | b | apply_lag | 0.00 | 0.00 |
| titan | b | pending_bytes | 6288.00 | 7336.00 |

## What the nodes logged

- europa: 80 lines, 80 at WARN or ERROR, 0 forced purges
  - 80 x openraft::core::raft_core: A message will be ignored because this node is no longer Lea
- hyperion: 63 lines, 63 at WARN or ERROR, 0 forced purges
  - 63 x openraft::core::raft_core: A message will be ignored because this node is no longer Lea
- titan: 14 lines, 14 at WARN or ERROR, 0 forced purges
  - 14 x openraft::core::raft_core: A message will be ignored because this node is no longer Lea

## fio alone, no Shoal unit up

- europa: 6 jobs on nvme0n1 under /optane, 9141 flushes a second of 4 KiB, the first arm's
- titan: 5 jobs on nvme0n1 under /xfs, 592 flushes a second of 28 KiB, the first arm's
- hyperion: 5 jobs on nvme0n1 under /xfs, 583 flushes a second of 24 KiB, the first arm's

| Host | fio leg | Figure | Early | Late | Stepped, s in |
| --- | --- | --- | --- | --- | --- |
| europa | a | flushes | 0 | 0 | - |
| europa | a | ms_flush | - | - | none |
| europa | a | writes | 11734 | 9144 | - |
| europa | a | ms_write | 0.02 | 0.01 | - |
| europa | a | mb | 44.5 | 37.5 | - |
| europa | a | busy | 0.08 | 0.02 | - |
| europa | b | flushes | 0 | 0 | - |
| europa | b | ms_flush | - | - | none |
| europa | b | writes | 9144 | 9144 | - |
| europa | b | ms_write | 0.01 | 0.01 | - |
| europa | b | mb | 37.5 | 37.5 | - |
| europa | b | busy | 0.03 | 0.03 | - |
| hyperion | a | flushes | 585 | 308 | - |
| hyperion | a | ms_flush | 0.33 | 3.70 | 64 |
| hyperion | a | writes | 1170 | 619 | - |
| hyperion | a | ms_write | 0.63 | 3.68 | - |
| hyperion | a | mb | 17.3 | 7.9 | - |
| hyperion | a | busy | 0.63 | 0.99 | - |
| hyperion | b | flushes | 585 | 308 | - |
| hyperion | b | ms_flush | 1.20 | 3.75 | 110 |
| hyperion | b | writes | 1171 | 619 | - |
| hyperion | b | ms_write | 0.67 | 3.64 | - |
| hyperion | b | mb | 14.4 | 8.0 | - |
| hyperion | b | busy | 0.16 | 0.99 | - |
| titan | a | flushes | 595 | 370 | - |
| titan | a | ms_flush | 0.35 | 1.40 | 60 |
| titan | a | writes | 1190 | 742 | - |
| titan | a | ms_write | 0.65 | 2.29 | - |
| titan | a | mb | 20.0 | 12.4 | - |
| titan | a | busy | 0.64 | 0.99 | - |
| titan | b | flushes | 595 | 516 | - |
| titan | b | ms_flush | 1.16 | 3.80 | 96 |
| titan | b | writes | 1190 | 1037 | - |
| titan | b | ms_write | 0.65 | 3.49 | - |
| titan | b | mb | 17.1 | 14.9 | - |
| titan | b | busy | 0.16 | 0.98 | - |
