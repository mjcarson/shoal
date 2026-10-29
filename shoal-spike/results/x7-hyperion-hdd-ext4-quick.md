### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8089 µs |
| fdatasync of a clean file | p50 490.7 µs, p99 519.4 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 18 KiB and 2.00 flushes a rename | p50 16632 µs |
| rename, then the directory's Fsync | 20 KiB and 2.00 flushes a rename | p50 16667 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | refused: Operation not supported (os error 95) | FIEMAP on the target: Ok(Extents { total: 1, shared: 0, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.15 µs, p99 33.42 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Sequential and random direct I/O, round 1 (population from 0.0% to 0.0% of the device, 1 GiB apart (p5 to p95); 16 files of 64 MiB)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| cell | side | MiB/s | ops/s | p50 µs | p99 µs | disk busy | merges/op | file at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write piece=1M depth=1 | seq-write | 187.8 | 187.8 | 3649 | 12356 | 0.982 | 0.093 | 0.0% |
| read piece=1M depth=1 | seq-read | 205.7 | 205.7 | 3653 | 12375 | 0.999 | 0 | 0.0% |
| write piece=1M depth=32 | seq-write | 163.8 | 163.8 | 146244 | 179480 | 0.985 | 0.079 | 0.0% |
| read piece=1M depth=32 | seq-read | 197.6 | 197.6 | 153889 | 175733 | 0.982 | 0 | 0.0% |
| random size=4K depth=1 | random | 0.791 | 202.5 | 5022 | 9518 | 0.988 | 0 | - |
| random size=4K depth=32 | random | 0.996 | 255.0 | 26247 | 413573 | 0.982 | 0 | - |
| random size=1M depth=1 | random | 93.75 | 93.75 | 9397 | 29660 | 0.997 | 0 | - |
| random size=1M depth=32 | random | 105.0 | 105.0 | 88094 | 345422 | 0.992 | 0 | - |
| short size=64K depth=1 | short | 33.75 | 540.0 | 450.1 | 9314 | 0.983 | 0 | - |
| short size=64K depth=4 | short | 152.6 | 2441 | 1147 | 8790 | 0.928 | 0 | - |
| fio w-seq-1M | fio | 211.9 | 205.2 | 39059 | 62128 | 0 | 0 | - |
| fio r-seq-1M | fio | 199.2 | 192.5 | 34341 | 534774 | 0 | 0 | - |
| fio r64-qd32 | fio | 22.58 | 333.6 | 52691 | 434110 | 0 | 0 | - |

### 3. A partial write, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write back)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J | 21512 | 31686 | 28528 | 111831 | 28110 | 111407 | 0 | 0 | 0 | 50040 | 133343 | 17.50 | 4.38 | 2.12 | 280.8 |
| 4K | 1 | J-ssd | 977.9 | 4551 | 29845 | 121646 | 29425 | 121214 | 0 | 0 | 0 | 30870 | 122601 | 9.50 | 2.38 | 1.12 | 333.3 |
| 4K | 6 | J | 36859 | 49515 | 40005 | 43512 | 38827 | 42078 | 0 | 0 | 0 | 74239 | 93023 | 17.50 | 4.38 | 0.875 | 156.4 |
| 4K | 6 | J-ssd | 1508 | 15047 | 65109 | 68235 | 36717 | 61143 | 0 | 0 | 0 | 69741 | 78173 | 9.50 | 2.38 | 0.625 | 165.5 |
| 64K | 1 | J | 22665 | 45209 | 30175 | 36616 | 29614 | 36057 | 0 | 0 | 0 | 58345 | 67876 | 137.5 | 2.15 | 2.12 | 292.9 |
| 64K | 1 | J-ssd | 1053 | 14764 | 30145 | 46402 | 29562 | 44660 | 0 | 0 | 0 | 32929 | 61166 | 69.50 | 1.09 | 1.12 | 292.8 |
| 64K | 6 | J | 37999 | 57852 | 47103 | 48564 | 45270 | 46348 | 0 | 0 | 0 | 85100 | 106413 | 137.5 | 2.15 | 0.875 | 189.1 |
| 64K | 6 | J-ssd | 2074 | 15360 | 80818 | 83104 | 53029 | 78107 | 0 | 0 | 0 | 84959 | 96267 | 69.50 | 1.09 | 0.625 | 206.2 |

### 2. The journal, round 1 (a 4 KiB header block on every record; write cache write back)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | allocated/coalesced | 18.53 | 18.53 | 1.00 | 41737 | 90745 | 90745 | 21.00 | 2.25 | 154.0 |
| 4K | 1 | allocated/each | 17.85 | 17.85 | 1.00 | 41726 | 98893 | 98893 | 21.00 | 2.25 | 155.0 |
| 4K | 1 | appended/coalesced | 18.49 | 18.49 | 1.00 | 41795 | 90924 | 90924 | 36.00 | 2.25 | 183.2 |
| 4K | 1 | appended/each | 20.71 | 20.71 | 1.00 | 41765 | 74404 | 74404 | 35.20 | 2.20 | 185.8 |
| 4K | 1 | ssd-written-ahead/each | 984.3 | 989.9 | 1.00 | 970.7 | 1177 | 4180 | 8.00 | 1.00 | 121.5 |
| 4K | 1 | written-ahead/coalesced | 87.93 | 87.93 | 1.00 | 8367 | 62529 | 62529 | 8.89 | 1.11 | 128.9 |
| 4K | 1 | written-ahead/each | 121.5 | 121.5 | 1.00 | 8360 | 11706 | 11706 | 8.64 | 1.08 | 166.5 |
| 4K | 6 | allocated/coalesced | 51.62 | 8.60 | 6.00 | 182384 | 182385 | 182385 | 10.33 | 0.417 | 34.17 |
| 4K | 6 | allocated/each | 72.25 | 24.08 | 3.00 | 83477 | 113751 | 113751 | 12.67 | 0.667 | 49.74 |
| 4K | 6 | appended/coalesced | 107.0 | 17.83 | 6.00 | 41790 | 98985 | 98985 | 13.00 | 0.375 | 34.15 |
| 4K | 6 | appended/each | 29.20 | 16.22 | 1.80 | 167006 | 278444 | 278444 | 24.00 | 1.22 | 163.8 |
| 4K | 6 | ssd-written-ahead/each | 5178 | 1737 | 3.00 | 1157 | 1225 | 2037 | 8.00 | 0.332 | 39.23 |
| 4K | 6 | written-ahead/coalesced | 670.1 | 117.0 | 6.00 | 8502 | 8855 | 8855 | 8.12 | 0.182 | 24.36 |
| 4K | 6 | written-ahead/each | 695.9 | 121.5 | 6.00 | 8508 | 8862 | 8862 | 8.12 | 0.182 | 26.66 |

### 1. A whole chunk (chunk), round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | A | 12.00 | 112.6 | 0.090 | 717.0 | 66038 | 70.20 | 16669 | 83604 | 91950 | 11.68 | 0.730 | 135.3 | 4.08 | 591.7 | 0.114 |
| 64K | 1 | A+ | 12.00 | 109.4 | 51.30 | 492.8 | 41154 | 69.04 | 16653 | 58530 | 166950 | 14.67 | 0.917 | 119.7 | 4.08 | 545.0 | 0.143 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1383 | 8065 | 0 | 0 | 8571 | 22833 | 102.4 | 6.40 | 69.33 | 1.17 | 190.5 | 1.00 |
| 64K | 6 | A-each | 12.00 | 671.3 | 0.050 | 31496 | 95166 | 317.1 | 33236 | 150473 | 163664 | 40.07 | 2.50 | 92.67 | 1.42 | 444.7 | 0.155 |
| 64K | 6 | A-batch | 12.00 | 644.5 | 0.041 | 4480 | 81494 | 98.74 | 16551 | 142101 | 142101 | 48.88 | 3.06 | 89.00 | 1.08 | 395.0 | 0.189 |
| 64K | 6 | A+-each | 12.00 | 642.6 | 223.2 | 9777 | 86481 | 318.3 | 42459 | 142157 | 153484 | 43.98 | 2.75 | 88.33 | 1.42 | 348.9 | 0.170 |
| 64K | 6 | A+-batch | 12.00 | 651.3 | 211.3 | 2221 | 65485 | 110.3 | 16598 | 100395 | 100395 | 65.86 | 4.12 | 85.33 | 1.08 | 300.4 | 0.254 |
| 64K | 6 | A+-dironly | 12.00 | 686.3 | 246.0 | 2166 | 0.080 | 63.92 | 70863 | 74659 | 74659 | 84.79 | 5.30 | 80.00 | 0.417 | 272.2 | 0.327 |
| 64K | 6 | B-own | 12.00 | 0 | 0 | 2404 | 50209 | 0 | 0 | 52633 | 53243 | 171.1 | 10.70 | 69.33 | 0.500 | 106.4 | 0.660 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2356 | 34133 | 0 | 0 | 36515 | 36515 | 259.3 | 16.21 | 69.33 | 0.333 | 46.38 | 1.00 |
| 1M | 1 | A | 12.00 | 111.7 | 0.090 | 2872 | 63899 | 70.64 | 16663 | 83615 | 175261 | 10.71 | 10.71 | 1092 | 4.08 | 626.9 | 0.156 |
| 1M | 1 | A+ | 12.00 | 111.1 | 52.09 | 2600 | 47391 | 69.48 | 16671 | 66891 | 80456 | 15.00 | 15.00 | 1086 | 4.08 | 568.9 | 0.219 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3482 | 8798 | 0 | 0 | 12323 | 41167 | 68.57 | 68.57 | 1029 | 1.17 | 201.7 | 1.00 |
| 1M | 6 | A-each | 12.00 | 669.7 | 0.050 | 36134 | 149563 | 293.0 | 33242 | 192124 | 223003 | 30.16 | 30.16 | 1051 | 1.42 | 456.3 | 0.169 |
| 1M | 6 | A-batch | 12.00 | 8167 | 0.041 | 16621 | 138950 | 100.0 | 16559 | 185685 | 185685 | 34.02 | 34.02 | 1047 | 1.08 | 418.9 | 0.191 |
| 1M | 6 | A+-each | 12.00 | 684.1 | 217.9 | 16245 | 107327 | 305.5 | 40986 | 158825 | 184592 | 37.30 | 37.30 | 1051 | 1.42 | 440.0 | 0.209 |
| 1M | 6 | A+-batch | 12.00 | 692.3 | 206.5 | 15345 | 96695 | 103.7 | 16575 | 133663 | 133663 | 48.48 | 48.48 | 1044 | 1.08 | 349.6 | 0.272 |
| 1M | 6 | A+-dironly | 12.00 | 662.0 | 218.8 | 10270 | 0.090 | 61.05 | 172815 | 189997 | 189997 | 42.57 | 42.57 | 1040 | 0.417 | 345.6 | 0.239 |
| 1M | 6 | B-own | 12.00 | 0 | 0 | 10121 | 38941 | 0 | 0 | 54401 | 55100 | 139.7 | 139.7 | 1029 | 0.500 | 135.9 | 0.785 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 10337 | 25009 | 0 | 0 | 39768 | 39768 | 178.1 | 178.1 | 1029 | 0.333 | 102.7 | 1.00 |
| 1M | 6 | A+-batch-replace | 12.00 | 301.0 | 178.9 | 16512 | 105039 | 182.6 | 17008 | 143980 | 143980 | 43.19 | 43.19 | 1052 | 1.08 | 347.3 | 0.242 |

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write back)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite | 111.0 | 8349 | 8405 | 8405 | 1.03 | 4.11 | 0.999 |
| 6 | overwrite | 216.0 | 25014 | 25117 | 25117 | 0.361 | 4.33 | 0.972 |

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 0.0% of the device, 0 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | capacity | 0 | 120.0 | no | 56.93 | 63.64 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | idle | 0 | 0 | no | 0 | 0 | 64.23 | 130.9 | 130.9 | 280.0 | 363.4 | 1.03 | 1.10 | 0.442 | 9.00 |
| 8 | arrival-qd1 | 60.00 | 6.00 | **yes** | 691.4 | 691.4 | 42.56 | 117.3 | 117.3 | 223.9 | 317.4 | 1.05 | 1.13 | 0.277 | 12.00 |
| 8 | offset-qd1 | 60.00 | 6.00 | **yes** | 505.7 | 505.7 | 50.28 | 100.1 | 100.1 | 194.2 | 297.7 | 1.03 | 1.26 | 0.425 | 12.00 |
| 8 | offset-ino | 60.00 | 6.00 | **yes** | 800.0 | 800.0 | 39.85 | 89.97 | 89.97 | 230.2 | 283.5 | 1.06 | 1.35 | 0.308 | 12.00 |
| 8 | kernel | 60.00 | 30.00 | **yes** | 325.4 | 383.6 | 52.96 | 111.4 | 111.4 | 259.9 | 358.3 | 1.03 | 1.17 | 0.221 | 13.50 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 0.0% of the device, 0 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 295.9 | 395.8 | 395.8 | 71.37 | 171.7 | 0.398 | 18.00 |
| 10 | 9.01 | 307.3 | 406.4 | 406.4 | 58.38 | 224.2 | 0.205 | 15.75 |
| unbounded | 30.03 | 373.4 | 558.1 | 558.1 | 60.00 | 141.9 | 0.363 | 14.25 |

### 1. A whole chunk (chunk-recycle), round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4 · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | R | 12.00 | 69.93 | 0.100 | 500.1 | 21897 | 68.15 | 27009 | 50040 | 82892 | 19.99 | 1.25 | 85.00 | 3.08 | 418.6 | 0.230 |
| 64K | 1 | A+ | 12.00 | 110.9 | 50.77 | 507.3 | 49449 | 69.04 | 16640 | 66839 | 110450 | 15.06 | 0.942 | 121.7 | 4.08 | 547.4 | 0.173 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1372 | 8077 | 0 | 0 | 8573 | 43679 | 86.96 | 5.43 | 69.33 | 1.17 | 173.7 | 1.00 |
| 64K | 6 | R-batch | 12.00 | 129.4 | 0.041 | 9484 | 31792 | 82.46 | 25671 | 66331 | 66331 | 96.21 | 6.01 | 72.33 | 0.750 | 225.6 | 0.305 |
| 64K | 6 | A+-batch | 12.00 | 655.8 | 203.4 | 2039 | 64502 | 110.9 | 16605 | 92265 | 92265 | 68.20 | 4.26 | 82.67 | 1.08 | 296.3 | 0.216 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2348 | 25351 | 0 | 0 | 27726 | 27726 | 315.4 | 19.71 | 69.33 | 0.333 | 46.43 | 1.00 |
| 1M | 1 | R | 12.00 | 70.87 | 0.090 | 2608 | 24301 | 69.26 | 23054 | 50078 | 58391 | 19.83 | 19.83 | 1049 | 3.08 | 449.4 | 0.714 |
| 1M | 1 | A+ | 12.00 | 111.6 | 53.57 | 2573 | 47396 | 69.40 | 16655 | 66852 | 66879 | 15.49 | 15.49 | 1079 | 4.08 | 594.4 | 0.558 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3415 | 8920 | 0 | 0 | 12318 | 264626 | 27.77 | 27.77 | 1029 | 1.17 | 203.8 | 1.00 |
| 1M | 6 | R-batch | 12.00 | 125.1 | 0.050 | 10151 | 72631 | 82.61 | 28949 | 110990 | 110990 | 54.69 | 54.69 | 1033 | 0.750 | 272.9 | 0.393 |
| 1M | 6 | A+-batch | 12.00 | 661.8 | 206.9 | 27972 | 63538 | 108.3 | 16564 | 112074 | 112074 | 54.37 | 54.37 | 1044 | 1.08 | 337.9 | 0.391 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 10096 | 28299 | 0 | 0 | 44035 | 44035 | 139.1 | 139.1 | 1029 | 0.333 | 100.2 | 1.00 |

