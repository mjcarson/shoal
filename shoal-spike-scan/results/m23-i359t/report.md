# Item 359: what the cell's slow reads wait on beside a whole scan of Big

From `target/lab/m23-i359t/captures`, 6 rounds. A scan round's side is its scan settings and its windows its scan's: *before* the bench's before window, *during* the scan's ask to its last page. A replay round's side is whether fio replayed the scan's reads, and its windows its fio's: *during* from its start to its end, *before* the 18 s before it. A figure is the median of its seconds in a window.

## Each round

| Round | Side | During, s | Scan partitions a second | Read p99 ms, before to during (W7) | During over before (W7) | Read p99 a second, before to during | During over before, a second |
| --- | --- | --- | --- | --- | --- | --- | --- |
| m23-i359t r1 | the product's defaults | 213 | 14085 | 5.54 to 6.29 | 1.137 | 5.42 to 5.93 | 1.094 |
| m23-i359t r2 | device_time: 1ms, its event cut at 225 s | 234, unfinished | 381 | 5.68 to 6.32 | 1.113 | 5.58 to 5.97 | 1.071 |
| m23-i359t r3 | device_time: 1ms, its event cut at 225 s | 237, unfinished | 382 | 5.41 to 5.71 | 1.057 | 5.12 to 5.31 | 1.038 |
| m23-i359t r4 | the product's defaults | 206 | 14563 | 5.25 to 6.02 | 1.147 | 5.08 to 5.81 | 1.142 |
| m23-i359t r5 | the product's defaults | 246 | 12146 | 5.66 to 5.92 | 1.045 | 5.48 to 5.67 | 1.034 |
| m23-i359t r6 | device_time: 1ms, its event cut at 225 s | 233, unfinished | 350 | 5.09 to 6.13 | 1.205 | 4.58 to 5.84 | 1.276 |

| Round | Member | Get p99 ms, before to during | Get p50 ms | Gets a second | Disk ms a read | Disk reads a second | Disk MiB read a second | Reads in flight | Every request in flight | Disk ms a flush | Shards' cores busy | Busiest shard | Scan bytes on the trickle |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| m23-i359t r1 | europa | 0.26 to 0.29 | 0.05 to 0.05 | 537 to 514 | 0.013 to 0.015 | 79 to 84 | 0.2 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.77 to 0.76 | 0.14 to 0.14 | 0.003 |
| m23-i359t r1 | hyperion | 5.41 to 5.78 | 0.18 to 0.18 | 519 to 496 | 0.846 to 1.182 | 63 to 675 | 0.1 to 6.5 | 0.06 to 0.79 | 2.71 to 3.45 | 2.75 to 2.77 | 1.70 to 1.90 | 0.38 to 0.43 | 0.081 |
| m23-i359t r1 | titan | 6.66 to 7.30 | 0.20 to 0.25 | 351 to 329 | 1.216 to 1.242 | 79 to 669 | 0.1 to 6.1 | 0.12 to 0.83 | 2.91 to 3.53 | 3.06 to 2.87 | 1.58 to 1.79 | 0.37 to 0.42 | 0.112 |
| m23-i359t r2 | europa | 0.28 to 0.25 | 0.05 to 0.05 | 535 to 515 | 0.013 to 0.025 | 86 to 140 | 0.2 to 0.6 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.73 to 0.78 | 0.13 to 0.15 | 0.004 |
| m23-i359t r2 | hyperion | 5.42 to 5.97 | 0.17 to 0.17 | 521 to 496 | 0.808 to 0.885 | 75 to 101 | 0.1 to 0.2 | 0.07 to 0.10 | 2.69 to 2.78 | 2.63 to 2.82 | 1.69 to 1.84 | 0.37 to 0.41 | 0.107 |
| m23-i359t r2 | titan | 6.34 to 7.07 | 0.21 to 0.25 | 353 to 330 | 1.006 to 0.934 | 85 to 122 | 0.2 to 0.3 | 0.12 to 0.13 | 2.86 to 2.79 | 2.97 to 2.81 | 1.71 to 1.83 | 0.38 to 0.42 | 0.132 |
| m23-i359t r3 | europa | 0.28 to 0.25 | 0.05 to 0.05 | 544 to 532 | 0.013 to 0.025 | 77 to 145 | 0.2 to 0.6 | 0.00 to 0.00 | 0.12 to 0.13 | - to - | 0.75 to 0.81 | 0.14 to 0.15 | 0.005 |
| m23-i359t r3 | hyperion | 5.49 to 5.62 | 0.17 to 0.17 | 527 to 513 | 0.941 to 0.794 | 68 to 95 | 0.1 to 0.2 | 0.08 to 0.08 | 2.73 to 2.74 | 2.87 to 2.77 | 1.74 to 1.90 | 0.39 to 0.43 | 0.099 |
| m23-i359t r3 | titan | 5.63 to 5.96 | 0.20 to 0.23 | 356 to 342 | 0.755 to 0.778 | 85 to 122 | 0.1 to 0.3 | 0.07 to 0.10 | 2.65 to 2.73 | 2.54 to 2.69 | 1.66 to 1.82 | 0.35 to 0.39 | 0.100 |
| m23-i359t r4 | europa | 0.27 to 0.29 | 0.05 to 0.05 | 554 to 525 | 0.014 to 0.014 | 69 to 85 | 0.1 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.76 to 0.74 | 0.14 to 0.14 | 0.003 |
| m23-i359t r4 | hyperion | 5.16 to 6.02 | 0.17 to 0.17 | 535 to 505 | 0.892 to 1.201 | 71 to 667 | 0.1 to 6.4 | 0.06 to 0.80 | 2.75 to 3.47 | 2.77 to 2.79 | 1.68 to 1.88 | 0.37 to 0.42 | 0.079 |
| m23-i359t r4 | titan | 5.68 to 6.27 | 0.24 to 0.28 | 362 to 336 | 0.874 to 1.109 | 78 to 690 | 0.1 to 6.5 | 0.07 to 0.79 | 2.60 to 3.40 | 2.42 to 2.64 | 1.74 to 1.91 | 0.40 to 0.45 | 0.093 |
| m23-i359t r5 | europa | 0.29 to 0.29 | 0.05 to 0.05 | 539 to 524 | 0.012 to 0.015 | 85 to 77 | 0.2 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.75 to 0.75 | 0.14 to 0.15 | 0.003 |
| m23-i359t r5 | hyperion | 5.83 to 6.10 | 0.17 to 0.17 | 521 to 504 | 1.045 to 1.192 | 74 to 625 | 0.1 to 5.9 | 0.12 to 0.77 | 2.93 to 3.40 | 2.80 to 2.78 | 1.72 to 1.88 | 0.40 to 0.44 | 0.093 |
| m23-i359t r5 | titan | 5.28 to 6.14 | 0.23 to 0.27 | 355 to 336 | 0.833 to 1.119 | 84 to 718 | 0.1 to 6.7 | 0.07 to 0.80 | 2.93 to 3.45 | 2.71 to 2.73 | 1.66 to 1.85 | 0.38 to 0.43 | 0.088 |
| m23-i359t r6 | europa | 0.30 to 0.28 | 0.05 to 0.05 | 558 to 524 | 0.015 to 0.025 | 86 to 140 | 0.2 to 0.6 | 0.00 to 0.00 | 0.13 to 0.12 | - to - | 0.79 to 0.83 | 0.15 to 0.15 | 0.002 |
| m23-i359t r6 | hyperion | 4.92 to 5.86 | 0.18 to 0.17 | 539 to 508 | 0.816 to 0.863 | 74 to 99 | 0.1 to 0.2 | 0.07 to 0.10 | 2.71 to 2.75 | 2.75 to 2.75 | 1.70 to 1.83 | 0.38 to 0.42 | 0.095 |
| m23-i359t r6 | titan | 5.71 to 6.89 | 0.22 to 0.26 | 359 to 334 | 0.774 to 0.881 | 83 to 122 | 0.1 to 0.3 | 0.07 to 0.12 | 2.69 to 2.77 | 2.68 to 2.72 | 1.68 to 1.83 | 0.39 to 0.43 | 0.117 |

## Each side, the median over its rounds and the range

| Figure | device_time: 1ms, its event cut at 225 s | the product's defaults | Ranges |
| --- | --- | --- | --- |
| The cell's read p99, during over before (client, W7) | 1.11 [1.06, 1.21] | 1.14 [1.05, 1.15] | overlap |
| The cell's read p99 a second, during over before (client) | 1.07 [1.04, 1.28] | 1.09 [1.03, 1.14] | overlap |
| The scan's partitions a second | 381 [350, 382] | 1.41e+04 [1.21e+04, 1.46e+04] | disjoint |
| The scan, s | 234 [233, 237] | 213 [206, 246] | overlap |
| europa: get p99, during over before | 0.879 [0.877, 0.945] | 1.08 [0.982, 1.1] | disjoint |
| hyperion: get p99, during over before | 1.1 [1.02, 1.19] | 1.07 [1.05, 1.17] | overlap |
| titan: get p99, during over before | 1.11 [1.06, 1.21] | 1.1 [1.1, 1.16] | overlap |
| europa: get p99 ms during | 0.249 [0.247, 0.279] | 0.287 [0.286, 0.291] | disjoint |
| hyperion: get p99 ms during | 5.86 [5.62, 5.97] | 6.02 [5.78, 6.1] | overlap |
| titan: get p99 ms during | 6.89 [5.96, 7.07] | 6.27 [6.14, 7.3] | overlap |
| europa: disk ms a read, during over before | 1.93 [1.71, 1.96] | 1.12 [1, 1.2] | disjoint |
| hyperion: disk ms a read, during over before | 1.06 [0.844, 1.1] | 1.35 [1.14, 1.4] | disjoint |
| titan: disk ms a read, during over before | 1.03 [0.929, 1.14] | 1.27 [1.02, 1.34] | overlap |
| europa: disk ms a read during | 0.0251 [0.0248, 0.0254] | 0.0146 [0.0145, 0.0147] | disjoint |
| hyperion: disk ms a read during | 0.863 [0.794, 0.885] | 1.19 [1.18, 1.2] | disjoint |
| titan: disk ms a read during | 0.881 [0.778, 0.934] | 1.12 [1.11, 1.24] | disjoint |
| europa: disk reads in flight during | 0.00297 [0.00296, 0.00395] | 0.000987 [0.000987, 0.000988] | disjoint |
| hyperion: disk reads in flight during | 0.0972 [0.077, 0.0983] | 0.795 [0.767, 0.801] | disjoint |
| titan: disk reads in flight during | 0.117 [0.0962, 0.127] | 0.795 [0.788, 0.832] | disjoint |
| europa: disk requests in flight during | 0.124 [0.124, 0.126] | 0.121 [0.119, 0.123] | disjoint |
| hyperion: disk requests in flight during | 2.75 [2.74, 2.78] | 3.45 [3.4, 3.47] | disjoint |
| titan: disk requests in flight during | 2.77 [2.73, 2.79] | 3.45 [3.4, 3.53] | disjoint |
| europa: disk ms a flush during | - | - | - |
| hyperion: disk ms a flush during | 2.77 [2.75, 2.82] | 2.78 [2.77, 2.79] | overlap |
| titan: disk ms a flush during | 2.72 [2.69, 2.81] | 2.73 [2.64, 2.87] | overlap |
| europa: disk MiB read a second during | 0.607 [0.577, 0.614] | 0.175 [0.163, 0.184] | disjoint |
| hyperion: disk MiB read a second during | 0.235 [0.224, 0.24] | 6.37 [5.91, 6.46] | disjoint |
| titan: disk MiB read a second during | 0.278 [0.277, 0.284] | 6.49 [6.12, 6.69] | disjoint |
| europa: shards' cores busy, during over before | 1.07 [1.05, 1.09] | 0.993 [0.981, 1] | disjoint |
| hyperion: shards' cores busy, during over before | 1.08 [1.07, 1.09] | 1.11 [1.09, 1.12] | overlap |
| titan: shards' cores busy, during over before | 1.09 [1.07, 1.1] | 1.11 [1.1, 1.13] | overlap |
| europa: busiest shard during | 0.148 [0.148, 0.148] | 0.138 [0.138, 0.148] | disjoint |
| hyperion: busiest shard during | 0.418 [0.408, 0.428] | 0.428 [0.419, 0.438] | overlap |
| titan: busiest shard during | 0.418 [0.389, 0.427] | 0.429 [0.418, 0.448] | overlap |
| europa: scan bytes on the trickle | 0.00397 [0.00196, 0.00468] | 0.00293 [0.00289, 0.00327] | overlap |
| hyperion: scan bytes on the trickle | 0.0989 [0.0948, 0.107] | 0.0805 [0.0794, 0.0933] | disjoint |
| titan: scan bytes on the trickle | 0.117 [0.1, 0.132] | 0.0931 [0.088, 0.112] | overlap |
