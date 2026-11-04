# Item 359: what the cell's slow reads wait on beside a whole scan of Big

From `target/lab/m23-i359/captures`, `target/lab/m23-i359r/captures`, 6 rounds. A scan round's side is its scan settings and its windows its scan's: *before* the bench's before window, *during* the scan's ask to its last page. A replay round's side is whether fio replayed the scan's reads, and its windows its fio's: *during* from its start to its end, *before* the 18 s before it. A figure is the median of its seconds in a window.

## Each round

| Round | Side | During, s | Scan partitions a second | Read p99 ms, before to during (W7) | During over before (W7) | Read p99 a second, before to during | During over before, a second |
| --- | --- | --- | --- | --- | --- | --- | --- |
| m23-i359 r1 | the product's defaults | 233 | 12876 | 5.19 to 6.29 | 1.212 | 5.15 to 6.01 | 1.168 |
| m23-i359 r2 | device_reads: 2 | 559, unfinished | 1476 | 5.34 to 5.25 | 0.984 | 5.35 to 5.41 | 1.011 |
| m23-i359r r1 | fio replay | 225 | - | - to - | - | 4.29 to 5.92 | 1.382 |
| m23-i359r r2 | no replay | 223 | - | - to - | - | 3.94 to 5.93 | 1.505 |
| m23-i359r r3 | no replay | 223 | - | - to - | - | 3.89 to 6.14 | 1.578 |
| m23-i359r r4 | fio replay | 225 | - | - to - | - | 3.79 to 6.24 | 1.648 |

| Round | Host | fio reads a second | KiB a read | ms a read, mean | ms a read, p99 |
| --- | --- | --- | --- | --- | --- |
| m23-i359r r1 | hyperion | 583 | 9.3 | 0.920 | 4.751 |
| m23-i359r r1 | titan | 583 | 9.3 | 0.919 | 4.817 |
| m23-i359r r4 | hyperion | 583 | 9.3 | 1.102 | 5.931 |
| m23-i359r r4 | titan | 583 | 9.3 | 1.239 | 6.521 |

| Round | Member | Get p99 ms, before to during | Get p50 ms | Gets a second | Disk ms a read | Disk reads a second | Disk MiB read a second | Reads in flight | Every request in flight | Disk ms a flush | Shards' cores busy | Busiest shard | Scan bytes on the trickle |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| m23-i359 r1 | europa | 0.30 to 0.29 | 0.05 to 0.05 | 545 to 512 | 0.014 to 0.014 | 75 to 78 | 0.2 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.77 to 0.75 | 0.14 to 0.14 | 0.002 |
| m23-i359 r1 | hyperion | 4.95 to 6.05 | 0.18 to 0.17 | 530 to 494 | 0.804 to 1.208 | 68 to 646 | 0.1 to 6.2 | 0.05 to 0.79 | 2.77 to 3.43 | 2.77 to 2.81 | 1.70 to 1.85 | 0.37 to 0.41 | 0.081 |
| m23-i359 r1 | titan | 6.00 to 7.03 | 0.20 to 0.24 | 352 to 329 | 1.045 to 1.245 | 75 to 663 | 0.1 to 6.2 | 0.10 to 0.83 | 2.88 to 3.48 | 2.87 to 2.80 | 1.64 to 1.80 | 0.35 to 0.40 | 0.104 |
| m23-i359 r2 | europa | 0.28 to 0.27 | 0.05 to 0.05 | 550 to 521 | 0.013 to 0.012 | 80 to 49 | 0.2 to 0.1 | 0.00 to 0.00 | 0.13 to 0.12 | - to - | 0.74 to 0.73 | 0.14 to 0.14 | 0.002 |
| m23-i359 r2 | hyperion | 5.08 to 5.44 | 0.18 to 0.16 | 536 to 502 | 0.833 to 1.023 | 75 to 148 | 0.1 to 0.6 | 0.06 to 0.17 | 2.70 to 2.86 | 2.60 to 2.87 | 1.81 to 1.95 | 0.40 to 0.44 | 0.045 |
| m23-i359 r2 | titan | 5.65 to 6.20 | 0.21 to 0.18 | 364 to 333 | 0.846 to 0.948 | 81 to 185 | 0.1 to 1.1 | 0.07 to 0.18 | 2.69 to 2.82 | 2.65 to 2.69 | 1.67 to 1.84 | 0.38 to 0.42 | 0.061 |
| m23-i359r r1 | europa | 0.24 to 0.30 | 0.05 to 0.05 | 546 to 535 | 0.067 to 0.013 | 0 to 125 | 0.0 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.77 to 0.77 | 0.14 to 0.14 | - |
| m23-i359r r1 | hyperion | 4.01 to 6.12 | 0.15 to 0.22 | 538 to 514 | 0.723 to 0.867 | 0 to 733 | 0.0 to 5.7 | 0.00 to 0.64 | 2.63 to 3.22 | 2.66 to 2.63 | 1.72 to 1.73 | 0.41 to 0.41 | - |
| m23-i359r r1 | titan | 4.19 to 6.65 | 0.15 to 0.37 | 349 to 338 | 0.619 to 0.839 | 0 to 747 | 0.0 to 5.8 | 0.00 to 0.63 | 2.60 to 3.23 | 2.57 to 2.67 | 1.72 to 1.71 | 0.39 to 0.40 | - |
| m23-i359r r2 | europa | 0.23 to 0.29 | 0.05 to 0.05 | 545 to 538 | 0.061 to 0.013 | 0 to 131 | 0.0 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.76 to 0.77 | 0.14 to 0.14 | - |
| m23-i359r r2 | hyperion | 3.62 to 6.08 | 0.15 to 0.21 | 537 to 516 | 0.699 to 0.783 | 0 to 145 | 0.0 to 0.3 | 0.00 to 0.11 | 2.63 to 2.72 | 2.66 to 2.66 | 1.71 to 1.73 | 0.37 to 0.38 | - |
| m23-i359r r2 | titan | 3.88 to 6.93 | 0.15 to 0.36 | 347 to 338 | 0.724 to 0.800 | 0 to 161 | 0.0 to 0.3 | 0.00 to 0.13 | 2.62 to 2.78 | 2.63 to 2.73 | 1.70 to 1.71 | 0.38 to 0.38 | - |
| m23-i359r r3 | europa | 0.21 to 0.30 | 0.05 to 0.05 | 540 to 530 | 0.064 to 0.012 | 0 to 124 | 0.0 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.75 to 0.76 | 0.14 to 0.14 | - |
| m23-i359r r3 | hyperion | 3.79 to 6.80 | 0.15 to 0.21 | 531 to 506 | 0.702 to 0.894 | 0 to 142 | 0.0 to 0.3 | 0.00 to 0.14 | 2.69 to 2.77 | 2.83 to 2.73 | 1.73 to 1.74 | 0.40 to 0.40 | - |
| m23-i359r r3 | titan | 3.85 to 6.95 | 0.15 to 0.38 | 345 to 333 | 0.751 to 0.826 | 0 to 158 | 0.0 to 0.3 | 0.00 to 0.13 | 2.60 to 2.72 | 2.60 to 2.59 | 1.69 to 1.70 | 0.37 to 0.38 | - |
| m23-i359r r4 | europa | 0.24 to 0.32 | 0.05 to 0.05 | 537 to 497 | 0.060 to 0.012 | 0 to 116 | 0.0 to 0.2 | 0.00 to 0.00 | 0.12 to 0.12 | - to - | 0.76 to 0.72 | 0.14 to 0.14 | - |
| m23-i359r r4 | hyperion | 3.42 to 6.13 | 0.15 to 0.19 | 528 to 479 | 0.715 to 0.890 | 0 to 728 | 0.0 to 5.7 | 0.00 to 0.67 | 2.72 to 3.31 | 2.88 to 2.78 | 1.70 to 1.63 | 0.36 to 0.36 | - |
| m23-i359r r4 | titan | 3.79 to 7.76 | 0.15 to 0.34 | 344 to 311 | 0.815 to 0.933 | 0 to 738 | 0.0 to 5.7 | 0.00 to 0.81 | 2.62 to 3.46 | 2.59 to 2.81 | 1.70 to 1.65 | 0.38 to 0.37 | - |

## Each side, the median over its rounds and the range

| Figure | device_reads: 2 | fio replay | no replay | the product's defaults | Ranges |
| --- | --- | --- | --- | --- | --- |
| The cell's read p99, during over before (client, W7) | 0.984 [0.984, 0.984] | - | - | 1.21 [1.21, 1.21] | - |
| The cell's read p99 a second, during over before (client) | 1.01 [1.01, 1.01] | 1.51 [1.38, 1.65] | 1.54 [1.51, 1.58] | 1.17 [1.17, 1.17] | - |
| The scan's partitions a second | 1.48e+03 [1.48e+03, 1.48e+03] | - | - | 1.29e+04 [1.29e+04, 1.29e+04] | - |
| The scan, s | 559 [559, 559] | 225 [225, 225] | 223 [223, 223] | 233 [233, 233] | - |
| europa: get p99, during over before | 0.949 [0.949, 0.949] | 1.28 [1.28, 1.29] | 1.36 [1.29, 1.42] | 0.986 [0.986, 0.986] | - |
| hyperion: get p99, during over before | 1.07 [1.07, 1.07] | 1.66 [1.52, 1.79] | 1.74 [1.68, 1.8] | 1.22 [1.22, 1.22] | - |
| titan: get p99, during over before | 1.1 [1.1, 1.1] | 1.82 [1.59, 2.05] | 1.8 [1.79, 1.8] | 1.17 [1.17, 1.17] | - |
| europa: get p99 ms during | 0.266 [0.266, 0.266] | 0.31 [0.304, 0.315] | 0.296 [0.292, 0.3] | 0.291 [0.291, 0.291] | - |
| hyperion: get p99 ms during | 5.44 [5.44, 5.44] | 6.12 [6.12, 6.13] | 6.44 [6.08, 6.8] | 6.05 [6.05, 6.05] | - |
| titan: get p99 ms during | 6.2 [6.2, 6.2] | 7.21 [6.65, 7.76] | 6.94 [6.93, 6.95] | 7.03 [7.03, 7.03] | - |
| europa: disk ms a read, during over before | 0.893 [0.893, 0.893] | 0.196 [0.188, 0.204] | 0.202 [0.194, 0.21] | 1.03 [1.03, 1.03] | - |
| hyperion: disk ms a read, during over before | 1.23 [1.23, 1.23] | 1.22 [1.2, 1.24] | 1.2 [1.12, 1.27] | 1.5 [1.5, 1.5] | - |
| titan: disk ms a read, during over before | 1.12 [1.12, 1.12] | 1.25 [1.15, 1.36] | 1.1 [1.1, 1.1] | 1.19 [1.19, 1.19] | - |
| europa: disk ms a read during | 0.0118 [0.0118, 0.0118] | 0.0125 [0.0123, 0.0127] | 0.0125 [0.0123, 0.0127] | 0.014 [0.014, 0.014] | - |
| hyperion: disk ms a read during | 1.02 [1.02, 1.02] | 0.878 [0.867, 0.89] | 0.838 [0.783, 0.894] | 1.21 [1.21, 1.21] | - |
| titan: disk ms a read during | 0.948 [0.948, 0.948] | 0.886 [0.839, 0.933] | 0.813 [0.8, 0.826] | 1.25 [1.25, 1.25] | - |
| europa: disk reads in flight during | 0.000987 [0.000987, 0.000987] | 0.000988 [0.000988, 0.000988] | 0.000988 [0.000988, 0.000988] | 0.000988 [0.000988, 0.000988] | - |
| hyperion: disk reads in flight during | 0.169 [0.169, 0.169] | 0.654 [0.64, 0.667] | 0.125 [0.113, 0.137] | 0.792 [0.792, 0.792] | - |
| titan: disk reads in flight during | 0.184 [0.184, 0.184] | 0.719 [0.627, 0.812] | 0.13 [0.129, 0.13] | 0.826 [0.826, 0.826] | - |
| europa: disk requests in flight during | 0.122 [0.122, 0.122] | 0.119 [0.117, 0.122] | 0.123 [0.123, 0.123] | 0.119 [0.119, 0.119] | - |
| hyperion: disk requests in flight during | 2.86 [2.86, 2.86] | 3.27 [3.22, 3.31] | 2.74 [2.72, 2.77] | 3.43 [3.43, 3.43] | - |
| titan: disk requests in flight during | 2.82 [2.82, 2.82] | 3.34 [3.23, 3.46] | 2.75 [2.72, 2.78] | 3.48 [3.48, 3.48] | - |
| europa: disk ms a flush during | - | - | - | - | - |
| hyperion: disk ms a flush during | 2.87 [2.87, 2.87] | 2.71 [2.63, 2.78] | 2.7 [2.66, 2.73] | 2.81 [2.81, 2.81] | - |
| titan: disk ms a flush during | 2.69 [2.69, 2.69] | 2.74 [2.67, 2.81] | 2.66 [2.59, 2.73] | 2.8 [2.8, 2.8] | - |
| europa: disk MiB read a second during | 0.106 [0.106, 0.106] | 0.225 [0.222, 0.227] | 0.24 [0.234, 0.246] | 0.166 [0.166, 0.166] | - |
| hyperion: disk MiB read a second during | 0.618 [0.618, 0.618] | 5.72 [5.7, 5.73] | 0.291 [0.291, 0.291] | 6.17 [6.17, 6.17] | - |
| titan: disk MiB read a second during | 1.07 [1.07, 1.07] | 5.74 [5.72, 5.76] | 0.327 [0.32, 0.335] | 6.15 [6.15, 6.15] | - |
| europa: shards' cores busy, during over before | 0.986 [0.986, 0.986] | 0.977 [0.954, 1] | 1.01 [1.01, 1.01] | 0.981 [0.981, 0.981] | - |
| hyperion: shards' cores busy, during over before | 1.08 [1.08, 1.08] | 0.984 [0.962, 1.01] | 1.01 [1.01, 1.01] | 1.09 [1.09, 1.09] | - |
| titan: shards' cores busy, during over before | 1.1 [1.1, 1.1] | 0.981 [0.968, 0.994] | 1 [1, 1] | 1.1 [1.1, 1.1] | - |
| europa: busiest shard during | 0.138 [0.138, 0.138] | 0.138 [0.138, 0.138] | 0.138 [0.138, 0.138] | 0.138 [0.138, 0.138] | - |
| hyperion: busiest shard during | 0.437 [0.437, 0.437] | 0.383 [0.359, 0.408] | 0.389 [0.378, 0.399] | 0.409 [0.409, 0.409] | - |
| titan: busiest shard during | 0.418 [0.418, 0.418] | 0.383 [0.368, 0.398] | 0.379 [0.379, 0.38] | 0.399 [0.399, 0.399] | - |
| europa: scan bytes on the trickle | 0.00185 [0.00185, 0.00185] | - | - | 0.00169 [0.00169, 0.00169] | - |
| hyperion: scan bytes on the trickle | 0.045 [0.045, 0.045] | - | - | 0.0813 [0.0813, 0.0813] | - |
| titan: scan bytes on the trickle | 0.0614 [0.0614, 0.0614] | - | - | 0.104 [0.104, 0.104] | - |

## Each pair of sides, whether their ranges are disjoint

| Figure | Pair | Verdict |
| --- | --- | --- |
| The cell's read p99 a second, during over before (client) | device_reads: 2 / fio replay | - |
| The cell's read p99 a second, during over before (client) | device_reads: 2 / no replay | - |
| The cell's read p99 a second, during over before (client) | device_reads: 2 / the product's defaults | - |
| The cell's read p99 a second, during over before (client) | fio replay / no replay | overlap |
| The cell's read p99 a second, during over before (client) | fio replay / the product's defaults | - |
| The cell's read p99 a second, during over before (client) | no replay / the product's defaults | - |
| europa: get p99, during over before | device_reads: 2 / fio replay | - |
| europa: get p99, during over before | device_reads: 2 / no replay | - |
| europa: get p99, during over before | device_reads: 2 / the product's defaults | - |
| europa: get p99, during over before | fio replay / no replay | overlap |
| europa: get p99, during over before | fio replay / the product's defaults | - |
| europa: get p99, during over before | no replay / the product's defaults | - |
| hyperion: get p99, during over before | device_reads: 2 / fio replay | - |
| hyperion: get p99, during over before | device_reads: 2 / no replay | - |
| hyperion: get p99, during over before | device_reads: 2 / the product's defaults | - |
| hyperion: get p99, during over before | fio replay / no replay | overlap |
| hyperion: get p99, during over before | fio replay / the product's defaults | - |
| hyperion: get p99, during over before | no replay / the product's defaults | - |
| titan: get p99, during over before | device_reads: 2 / fio replay | - |
| titan: get p99, during over before | device_reads: 2 / no replay | - |
| titan: get p99, during over before | device_reads: 2 / the product's defaults | - |
| titan: get p99, during over before | fio replay / no replay | overlap |
| titan: get p99, during over before | fio replay / the product's defaults | - |
| titan: get p99, during over before | no replay / the product's defaults | - |
| europa: disk reads in flight during | device_reads: 2 / fio replay | - |
| europa: disk reads in flight during | device_reads: 2 / no replay | - |
| europa: disk reads in flight during | device_reads: 2 / the product's defaults | - |
| europa: disk reads in flight during | fio replay / no replay | overlap |
| europa: disk reads in flight during | fio replay / the product's defaults | - |
| europa: disk reads in flight during | no replay / the product's defaults | - |
| hyperion: disk reads in flight during | device_reads: 2 / fio replay | - |
| hyperion: disk reads in flight during | device_reads: 2 / no replay | - |
| hyperion: disk reads in flight during | device_reads: 2 / the product's defaults | - |
| hyperion: disk reads in flight during | fio replay / no replay | disjoint |
| hyperion: disk reads in flight during | fio replay / the product's defaults | - |
| hyperion: disk reads in flight during | no replay / the product's defaults | - |
| titan: disk reads in flight during | device_reads: 2 / fio replay | - |
| titan: disk reads in flight during | device_reads: 2 / no replay | - |
| titan: disk reads in flight during | device_reads: 2 / the product's defaults | - |
| titan: disk reads in flight during | fio replay / no replay | disjoint |
| titan: disk reads in flight during | fio replay / the product's defaults | - |
| titan: disk reads in flight during | no replay / the product's defaults | - |
| europa: shards' cores busy, during over before | device_reads: 2 / fio replay | - |
| europa: shards' cores busy, during over before | device_reads: 2 / no replay | - |
| europa: shards' cores busy, during over before | device_reads: 2 / the product's defaults | - |
| europa: shards' cores busy, during over before | fio replay / no replay | disjoint |
| europa: shards' cores busy, during over before | fio replay / the product's defaults | - |
| europa: shards' cores busy, during over before | no replay / the product's defaults | - |
| hyperion: shards' cores busy, during over before | device_reads: 2 / fio replay | - |
| hyperion: shards' cores busy, during over before | device_reads: 2 / no replay | - |
| hyperion: shards' cores busy, during over before | device_reads: 2 / the product's defaults | - |
| hyperion: shards' cores busy, during over before | fio replay / no replay | disjoint |
| hyperion: shards' cores busy, during over before | fio replay / the product's defaults | - |
| hyperion: shards' cores busy, during over before | no replay / the product's defaults | - |
| titan: shards' cores busy, during over before | device_reads: 2 / fio replay | - |
| titan: shards' cores busy, during over before | device_reads: 2 / no replay | - |
| titan: shards' cores busy, during over before | device_reads: 2 / the product's defaults | - |
| titan: shards' cores busy, during over before | fio replay / no replay | disjoint |
| titan: shards' cores busy, during over before | fio replay / the product's defaults | - |
| titan: shards' cores busy, during over before | no replay / the product's defaults | - |
