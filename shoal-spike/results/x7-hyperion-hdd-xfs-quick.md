### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 105 · fs device 0 | 5.48 KiB a sync · sync p50 8055 µs |
| fdatasync of a clean file | p50 493.5 µs, p99 525.9 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8227 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8254 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.16 µs, p99 33.21 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Sequential and random direct I/O, round 1 (population from 7.9% to 86.4% of the device, 10240 GiB apart (p5 to p95); 16 files of 64 MiB)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| cell | side | MiB/s | ops/s | p50 µs | p99 µs | disk busy | merges/op | file at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write piece=1M depth=1 | seq-write | 185.1 | 185.1 | 3951 | 12656 | 0.978 | 0 | 31.4% |
| read piece=1M depth=1 | seq-read | 194.8 | 194.8 | 3972 | 12670 | 0.995 | 0 | 31.4% |
| write piece=1M depth=32 | seq-write | 163.0 | 163.0 | 89065 | 241228 | 0.988 | 0 | 31.4% |
| read piece=1M depth=32 | seq-read | 219.5 | 219.5 | 106577 | 661405 | 0.976 | 0 | 31.4% |
| random size=4K depth=1 | random | 0.249 | 63.75 | 15041 | 22988 | 0.996 | 0 | - |
| random size=4K depth=32 | random | 0.557 | 142.5 | 106224 | 415477 | 0.988 | 0 | - |
| random size=1M depth=1 | random | 48.75 | 48.75 | 19521 | 26989 | 0.999 | 0 | - |
| random size=1M depth=32 | random | 50.63 | 50.63 | 173678 | 350293 | 0.994 | 0 | - |
| short size=64K depth=1 | short | 26.37 | 421.9 | 450.2 | 9449 | 0.994 | 0 | - |
| short size=64K depth=4 | short | 177.0 | 2831 | 1138 | 10406 | 0.915 | 0 | - |
| fio w-seq-1M | fio | 144.4 | 138.1 | 52167 | 158335 | 0 | 0 | - |
| fio r-seq-1M | fio | 140.6 | 133.9 | 55837 | 105382 | 0 | 0 | - |
| fio r64-qd32 | fio | 12.48 | 173.3 | 147849 | 700449 | 0 | 0 | - |

### 3. A partial write, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write back)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J | 11477 | 21360 | 13883 | 20189 | 13423 | 19733 | 0 | 0 | 0 | 26914 | 35907 | 16.50 | 4.12 | 2.12 | 288.6 |
| 4K | 1 | J-ssd | 924.9 | 1756 | 8372 | 21547 | 7903 | 15606 | 0 | 0 | 0 | 9359 | 23302 | 8.50 | 2.12 | 1.12 | 290.7 |
| 4K | 6 | J | 16006 | 21870 | 34259 | 37545 | 32879 | 36182 | 0 | 0 | 0 | 47006 | 59411 | 16.50 | 4.12 | 0.875 | 169.0 |
| 4K | 6 | J-ssd | 1603 | 15137 | 30558 | 37997 | 27329 | 27976 | 0 | 0 | 0 | 39594 | 45578 | 8.50 | 2.12 | 0.625 | 201.1 |
| 64K | 1 | J | 12228 | 28358 | 14160 | 23623 | 13563 | 23030 | 0 | 0 | 0 | 26691 | 41863 | 136.5 | 2.13 | 2.12 | 305.1 |
| 64K | 1 | J-ssd | 1063 | 8223 | 9407 | 29524 | 8812 | 27788 | 0 | 0 | 0 | 10650 | 31471 | 68.50 | 1.07 | 1.12 | 318.6 |
| 64K | 6 | J | 18733 | 22445 | 31818 | 33758 | 30132 | 32139 | 0 | 0 | 0 | 50549 | 56199 | 136.5 | 2.13 | 0.875 | 194.1 |
| 64K | 6 | J-ssd | 2016 | 9102 | 39504 | 40077 | 33088 | 37662 | 0 | 0 | 0 | 41956 | 48676 | 68.50 | 1.07 | 0.625 | 217.1 |

### 2. The journal, round 1 (a 4 KiB header block on every record; write cache write back)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | allocated/coalesced | 29.98 | 29.98 | 1.00 | 33352 | 33365 | 33365 | 12.00 | 2.00 | 155.4 |
| 4K | 1 | allocated/each | 28.80 | 28.80 | 1.00 | 33359 | 41538 | 41538 | 12.00 | 2.00 | 154.7 |
| 4K | 1 | appended/coalesced | 28.79 | 28.79 | 1.00 | 33347 | 41660 | 41660 | 12.00 | 2.00 | 184.2 |
| 4K | 1 | appended/each | 28.80 | 28.80 | 1.00 | 33359 | 41579 | 41579 | 12.00 | 2.00 | 192.0 |
| 4K | 1 | ssd-written-ahead/each | 1057 | 1063 | 1.00 | 948.9 | 1042 | 1235 | 8.00 | 1.00 | 119.8 |
| 4K | 1 | written-ahead/coalesced | 111.7 | 111.7 | 1.00 | 8361 | 22082 | 22082 | 8.17 | 1.09 | 131.2 |
| 4K | 1 | written-ahead/each | 115.0 | 115.0 | 1.00 | 8363 | 16070 | 16070 | 8.17 | 1.09 | 131.8 |
| 4K | 6 | allocated/coalesced | 154.3 | 25.72 | 6.00 | 41681 | 41694 | 41694 | 8.67 | 0.333 | 28.43 |
| 4K | 6 | allocated/each | 87.63 | 32.29 | 2.71 | 66704 | 75044 | 75044 | 9.47 | 0.737 | 72.58 |
| 4K | 6 | appended/coalesced | 172.9 | 28.81 | 6.00 | 33346 | 41532 | 41532 | 8.67 | 0.333 | 33.35 |
| 4K | 6 | appended/each | 50.89 | 25.44 | 2.00 | 100022 | 183392 | 183392 | 10.00 | 1.00 | 152.2 |
| 4K | 6 | ssd-written-ahead/each | 4689 | 1579 | 2.99 | 1120 | 4226 | 4315 | 8.00 | 0.333 | 39.82 |
| 4K | 6 | written-ahead/coalesced | 699.6 | 122.1 | 6.00 | 8502 | 8563 | 8563 | 8.03 | 0.182 | 23.83 |
| 4K | 6 | written-ahead/each | 696.9 | 243.4 | 3.00 | 8504 | 8573 | 8573 | 8.03 | 0.348 | 47.47 |

### 1. A whole chunk (chunk), round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | A | 12.00 | 112.8 | 0.080 | 871.3 | 24037 | 63.32 | 8265 | 33349 | 41673 | 28.79 | 1.80 | 76.00 | 4.00 | 598.7 | 0.272 |
| 64K | 1 | A+ | 12.00 | 114.5 | 67.82 | 547.2 | 32457 | 65.42 | 8265 | 41619 | 46442 | 26.17 | 1.64 | 76.00 | 4.00 | 646.5 | 0.247 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1365 | 8215 | 0 | 0 | 8744 | 16723 | 106.0 | 6.62 | 68.33 | 1.17 | 178.4 | 1.00 |
| 64K | 6 | A-each | 12.00 | 608.0 | 0.050 | 11231 | 62184 | 70.16 | 25796 | 108299 | 124913 | 55.37 | 3.46 | 72.67 | 1.83 | 482.9 | 0.173 |
| 64K | 6 | A-batch | 12.00 | 694.6 | 0.040 | 2858 | 40085 | 65.04 | 7221 | 58160 | 58160 | 110.8 | 6.93 | 71.33 | 1.33 | 434.1 | 0.346 |
| 64K | 6 | A+-each | 12.00 | 698.2 | 199.6 | 1714 | 54863 | 79.10 | 31504 | 84034 | 108401 | 65.43 | 4.09 | 72.33 | 1.67 | 405.6 | 0.204 |
| 64K | 6 | A+-batch | 12.00 | 682.8 | 295.4 | 2468 | 29649 | 71.42 | 7230 | 49729 | 49729 | 144.4 | 9.02 | 70.33 | 1.17 | 325.3 | 0.451 |
| 64K | 6 | A+-dironly | 12.00 | 709.4 | 293.4 | 2403 | 0.070 | 65.76 | 23035 | 25036 | 25036 | 240.6 | 15.04 | 69.33 | 0.333 | 227.8 | 0.751 |
| 64K | 6 | B-own | 12.00 | 0 | 0 | 2139 | 18470 | 0 | 0 | 20729 | 21265 | 313.5 | 19.60 | 68.33 | 0.500 | 117.0 | 0.979 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2242 | 23387 | 0 | 0 | 26143 | 26143 | 320.3 | 20.02 | 68.33 | 0.333 | 101.3 | 1.00 |
| 1M | 1 | A | 12.00 | 112.7 | 0.080 | 2999 | 30224 | 64.00 | 8265 | 41668 | 58346 | 24.02 | 24.02 | 1036 | 4.00 | 631.5 | 0.385 |
| 1M | 1 | A+ | 12.00 | 115.6 | 65.97 | 2612 | 38889 | 65.78 | 8262 | 50003 | 50027 | 20.58 | 20.58 | 1036 | 4.00 | 610.3 | 0.330 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3430 | 11754 | 0 | 0 | 15201 | 25922 | 62.38 | 62.38 | 1028 | 1.17 | 208.3 | 1.00 |
| 1M | 6 | A-each | 12.00 | 783.4 | 0.080 | 16156 | 60953 | 66.00 | 55713 | 137276 | 149312 | 46.57 | 46.57 | 1033 | 1.83 | 538.4 | 0.337 |
| 1M | 6 | A-batch | 12.00 | 3703 | 0.051 | 10786 | 85518 | 72.65 | 7250 | 115996 | 115996 | 55.53 | 55.53 | 1031 | 1.33 | 466.1 | 0.401 |
| 1M | 6 | A+-each | 12.00 | 723.9 | 300.6 | 11659 | 84610 | 72.84 | 38058 | 116660 | 165846 | 48.13 | 48.13 | 1033 | 1.67 | 453.0 | 0.348 |
| 1M | 6 | A+-batch | 12.00 | 713.0 | 289.4 | 18686 | 151960 | 63.39 | 7193 | 190910 | 190910 | 45.11 | 45.11 | 1031 | 1.33 | 398.7 | 0.326 |
| 1M | 6 | A+-dironly | 12.00 | 727.2 | 242.0 | 10810 | 0.080 | 56.50 | 50861 | 74136 | 74136 | 85.21 | 85.21 | 1029 | 0.333 | 358.5 | 0.616 |
| 1M | 6 | B-own | 12.00 | 0 | 0 | 10494 | 41301 | 0 | 0 | 51274 | 51848 | 118.3 | 118.3 | 1028 | 0.500 | 142.2 | 0.855 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 10330 | 31097 | 0 | 0 | 47063 | 47063 | 138.4 | 138.4 | 1028 | 0.333 | 120.3 | 1.00 |
| 1M | 6 | A+-batch-replace | 12.00 | 346.9 | 233.2 | 10721 | 51044 | 116.2 | 7251 | 75121 | 75121 | 84.49 | 84.49 | 1033 | 1.50 | 396.6 | 0.611 |

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write back)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite | 117.0 | 8348 | 8406 | 8406 | 1.03 | 4.10 | 0.981 |
| 6 | overwrite | 162.0 | 33349 | 33453 | 33453 | 0.370 | 4.44 | 0.999 |

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | capacity | 0 | 72.00 | no | 109.3 | 117.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | idle | 0 | 0 | no | 0 | 0 | 88.45 | 123.1 | 123.1 | 187.8 | 196.7 | 1.03 | 1.16 | 0.506 | 9.75 |
| 8 | arrival-qd1 | 36.00 | 6.00 | **yes** | 571.4 | 571.4 | 56.57 | 138.1 | 138.1 | 165.5 | 236.5 | 1.04 | 1.20 | 0.293 | 11.25 |
| 8 | offset-qd1 | 36.00 | 6.00 | **yes** | 500.5 | 500.5 | 48.66 | 104.9 | 104.9 | 222.1 | 297.4 | 1.04 | 1.11 | 0.364 | 12.00 |
| 8 | offset-ino | 36.00 | 12.00 | **yes** | 525.0 | 525.0 | 49.54 | 112.4 | 112.4 | 150.2 | 223.4 | 1.04 | 1.15 | 0.257 | 12.75 |
| 8 | kernel | 36.00 | 36.00 | no | 142.0 | 289.1 | 56.38 | 185.6 | 185.6 | 166.5 | 303.6 | 1.16 | 3.02 | 0.224 | 13.50 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 295.9 | 316.6 | 316.6 | 51.61 | 117.9 | 0.325 | 17.25 |
| 10 | 9.01 | 295.9 | 368.5 | 368.5 | 55.62 | 122.0 | 0.387 | 16.50 |
| unbounded | 24.02 | 356.2 | 436.9 | 436.9 | 68.91 | 138.4 | 0.370 | 13.50 |

### 1. A whole chunk (chunk-recycle), round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | R | 12.00 | 70.02 | 0.090 | 503.3 | 18771 | 65.40 | 14959 | 33337 | 41662 | 28.79 | 1.80 | 72.00 | 3.00 | 439.2 | 0.282 |
| 64K | 1 | A+ | 12.00 | 117.0 | 66.11 | 511.3 | 22378 | 63.58 | 8268 | 33039 | 33368 | 34.27 | 2.14 | 76.00 | 4.00 | 585.0 | 0.336 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1387 | 8100 | 0 | 0 | 8638 | 22789 | 101.9 | 6.37 | 68.33 | 1.17 | 179.1 | 1.00 |
| 64K | 6 | R-batch | 12.00 | 127.1 | 0.050 | 1915 | 18140 | 80.71 | 21306 | 41417 | 41417 | 160.5 | 10.03 | 68.67 | 0.667 | 241.1 | 0.258 |
| 64K | 6 | A+-batch | 12.00 | 641.5 | 274.3 | 2397 | 29789 | 64.66 | 7230 | 49745 | 49745 | 144.3 | 9.02 | 70.33 | 1.17 | 329.7 | 0.232 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2346 | 7594 | 0 | 0 | 10112 | 10112 | 621.2 | 38.82 | 68.33 | 0.333 | 88.26 | 1.00 |
| 1M | 1 | R | 12.00 | 71.28 | 0.091 | 2602 | 25371 | 64.02 | 20399 | 49994 | 58348 | 20.89 | 20.89 | 1032 | 3.00 | 478.0 | 0.271 |
| 1M | 1 | A+ | 12.00 | 114.6 | 66.05 | 2612 | 30564 | 64.07 | 8271 | 41689 | 50029 | 22.87 | 22.87 | 1036 | 4.00 | 615.1 | 0.297 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3419 | 9487 | 0 | 0 | 12913 | 15228 | 77.06 | 77.06 | 1028 | 1.17 | 214.6 | 1.00 |
| 1M | 6 | R-batch | 12.00 | 130.3 | 0.041 | 10904 | 62598 | 85.84 | 20187 | 91717 | 91717 | 65.73 | 65.73 | 1029 | 0.667 | 263.6 | 0.451 |
| 1M | 6 | A+-batch | 12.00 | 606.5 | 264.7 | 10489 | 54691 | 71.89 | 7236 | 75050 | 75050 | 80.39 | 80.39 | 1031 | 1.33 | 390.3 | 0.552 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 10442 | 26726 | 0 | 0 | 41584 | 41584 | 145.7 | 145.7 | 1028 | 0.333 | 117.0 | 1.00 |

