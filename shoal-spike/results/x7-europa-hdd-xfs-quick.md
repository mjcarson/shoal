### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 368.3 µs |
| fdatasync of a clean file | p50 50.93 µs, p99 78.28 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 668.6 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 669.1 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.18 µs, p99 16.24 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Sequential and random direct I/O, round 1 (population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95); 16 files of 64 MiB)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| cell | side | MiB/s | ops/s | p50 µs | p99 µs | disk busy | merges/op | file at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write piece=1M depth=1 | seq-write | 151.6 | 151.6 | 2049 | 21922 | 0.988 | 0 | 73.3% |
| read piece=1M depth=1 | seq-read | 151.0 | 151.0 | 6537 | 18109 | 0.984 | 0 | 73.3% |
| write piece=1M depth=32 | seq-write | 147.3 | 147.3 | 82075 | 285369 | 0.994 | 0 | 73.3% |
| read piece=1M depth=32 | seq-read | 165.8 | 165.8 | 134791 | 742042 | 0.979 | 0 | 73.3% |
| random size=4K depth=1 | random | 0.300 | 76.88 | 12079 | 23069 | 0.996 | 0 | - |
| random size=4K depth=32 | random | 1.03 | 264.4 | 57126 | 264859 | 0.984 | 0 | - |
| random size=1M depth=1 | random | 54.38 | 54.38 | 17261 | 27136 | 0.983 | 0 | - |
| random size=1M depth=32 | random | 46.88 | 46.88 | 222870 | 349103 | 0.977 | 0 | - |
| short size=64K depth=1 | short | 22.62 | 361.9 | 477.1 | 10427 | 0.983 | 0 | - |
| short size=64K depth=4 | short | 159.8 | 2558 | 400.0 | 22783 | 0.971 | 0 | - |
| fio w-seq-1M | fio | 228.5 | 221.8 | 28967 | 574620 | 0 | 0 | - |
| fio r-seq-1M | fio | 153.9 | 147.3 | 50594 | 102236 | 0 | 0 | - |
| fio r64-qd32 | fio | 17.75 | 256.6 | 81265 | 446693 | 0 | 0 | - |

### 6. One unit at a random offset, round 1 (µs; queue depth 1; population from 18.3% to 18.3% of the device, 0 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| unit | level | n | open p50 | open p99 | read p50 | read p99 | close p50 | total p50 | total p99 | total p99.9 | dev KiB read/sample |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | cold | 5.00 | 424.1 | 445.7 | 8827 | 14602 | 13.92 | 9241 | 15047 | 15047 | 44.00 |
| 4K | dentry-warm | 20.00 | 58.95 | 73.78 | 8779 | 17631 | 24.54 | 8804 | 17690 | 17690 | 8.00 |
| 4K | open | 20.00 | 0 | 0 | 116.5 | 162.0 | 0 | 116.5 | 162.0 | 162.0 | 4.00 |
| 64K | cold | 5.00 | 456.9 | 706.4 | 9119 | 17654 | 14.30 | 9581 | 18360 | 18360 | 104.0 |
| 64K | dentry-warm | 20.00 | 28.22 | 59.31 | 673.1 | 15008 | 10.86 | 715.8 | 15033 | 15033 | 68.00 |
| 64K | open | 20.00 | 0 | 0 | 233.3 | 269.2 | 0 | 233.3 | 269.2 | 269.2 | 64.00 |

### 5. Listing: the populations built, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| population | files | built in s | files/s |
| --- | --- | --- | --- |
| deep-2K | 2000 | 0.800 | 2501 |
| wide-2K | 2000 | 0.848 | 2358 |

### 5. Listing a placement group, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| population | walk | chunks | cold s | cold µs/chunk | cold dev KiB read/chunk | warm s | warm µs/chunk | warm dev KiB written | hours/16 TiB at 1 MiB | at 4 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deep-2K | names | 2000 | 0.007 | 3.70 | 0.258 | 0.000 | 0.212 | 0 | 0.017 | 0.004 |
| deep-2K | statx | 2000 | 0.021 | 10.38 | 0.610 | 0.002 | 0.932 | 0 | 0.048 | 0.012 |
| deep-2K | statx-ino | 2000 | 0.021 | 10.31 | 0.610 | 0.002 | 1.10 | 0 | 0.048 | 0.012 |
| deep-2K | xattr | 2000 | 0.024 | 11.81 | 0.610 | 0.005 | 2.50 | 0 | 0.055 | 0.014 |
| deep-2K | header-qd32 | 2000 | 0.212 | 105.8 | 4.61 | 0.184 | 91.78 | 0 | 0.493 | 0.123 |
| wide-2K | names | 2000 | 0.032 | 16.21 | 1.10 | 0.006 | 2.88 | 0 | 0.076 | 0.019 |
| wide-2K | statx | 2000 | 0.039 | 19.39 | 1.11 | 0.007 | 3.68 | 0 | 0.090 | 0.023 |
| wide-2K | statx-ino | 2000 | 0.039 | 19.69 | 1.11 | 0.007 | 3.72 | 0 | 0.092 | 0.023 |
| wide-2K | xattr | 2000 | 0.042 | 20.98 | 1.11 | 0.010 | 5.13 | 0 | 0.098 | 0.024 |
| wide-2K | header-qd32 | 2000 | 0.205 | 102.5 | 5.11 | 0.115 | 57.56 | 0 | 0.478 | 0.119 |

### 3. A partial write, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write back)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J | 455.3 | 1072 | 782.7 | 2398 | 458.8 | 2082 | 0 | 0 | 0 | 1285 | 2986 | 16.50 | 4.12 | 2.12 | 158.1 |
| 4K | 1 | J-ssd | 50.75 | 173.6 | 711.6 | 1554 | 413.1 | 1162 | 0 | 0 | 0 | 759.5 | 1707 | 8.50 | 2.12 | 1.12 | 159.3 |
| 4K | 6 | J | 1693 | 2024 | 3295 | 3426 | 1577 | 3047 | 0 | 0 | 0 | 5071 | 5317 | 16.50 | 4.12 | 0.875 | 108.5 |
| 4K | 6 | J-ssd | 85.84 | 217.3 | 3694 | 3890 | 2066 | 3558 | 0 | 0 | 0 | 3909 | 3964 | 8.50 | 2.12 | 0.500 | 99.24 |
| 64K | 1 | J | 525.3 | 3427 | 851.2 | 2037 | 406.9 | 1488 | 0 | 0 | 0 | 1391 | 4318 | 136.5 | 2.13 | 2.12 | 168.7 |
| 64K | 1 | J-ssd | 79.56 | 195.2 | 1016 | 3301 | 584.5 | 1251 | 0 | 0 | 0 | 1095 | 3381 | 68.50 | 1.07 | 1.12 | 148.5 |
| 64K | 6 | J | 21092 | 25595 | 41298 | 61317 | 40182 | 59951 | 0 | 0 | 0 | 64792 | 81611 | 136.5 | 2.13 | 0.875 | 294.4 |
| 64K | 6 | J-ssd | 303.4 | 449.6 | 37410 | 47161 | 36270 | 46579 | 0 | 0 | 0 | 37753 | 47340 | 68.50 | 1.07 | 0.500 | 185.3 |

### 2. The journal, round 1 (a 4 KiB header block on every record; write cache write back)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | allocated/coalesced | 705.3 | 710.7 | 1.00 | 1080 | 4762 | 4763 | 12.00 | 2.00 | 62.08 |
| 4K | 1 | allocated/each | 715.6 | 721.1 | 1.00 | 1071 | 4160 | 4668 | 12.00 | 2.00 | 65.99 |
| 4K | 1 | appended/coalesced | 709.9 | 715.5 | 1.00 | 1090 | 3874 | 4353 | 12.00 | 2.00 | 75.87 |
| 4K | 1 | appended/each | 701.8 | 707.3 | 1.00 | 1091 | 4342 | 4692 | 12.00 | 2.00 | 69.57 |
| 4K | 1 | ssd-written-ahead/each | 25380 | 25386 | 1.00 | 37.22 | 53.23 | 63.68 | 8.00 | 0 | 30.27 |
| 4K | 1 | written-ahead/coalesced | 1009 | 1015 | 1.00 | 837.5 | 1877 | 2319 | 8.02 | 1.01 | 51.58 |
| 4K | 1 | written-ahead/each | 998.5 | 1004 | 1.00 | 820.8 | 1866 | 2506 | 8.02 | 1.01 | 50.52 |
| 4K | 6 | allocated/coalesced | 3948 | 663.5 | 6.00 | 1167 | 3937 | 4032 | 8.67 | 0.333 | 10.65 |
| 4K | 6 | allocated/each | 1615 | 545.6 | 3.02 | 3022 | 6586 | 6641 | 9.33 | 0.667 | 29.58 |
| 4K | 6 | appended/coalesced | 3877 | 651.7 | 6.00 | 1184 | 3956 | 4221 | 8.67 | 0.333 | 12.51 |
| 4K | 6 | appended/each | 954.9 | 329.3 | 3.00 | 5698 | 11531 | 11531 | 9.35 | 0.677 | 53.57 |
| 4K | 6 | ssd-written-ahead/each | 44158 | 12335 | 3.58 | 67.40 | 570.2 | 681.6 | 8.24 | 0 | 14.03 |
| 4K | 6 | written-ahead/coalesced | 5412 | 907.5 | 6.00 | 919.5 | 2344 | 2382 | 8.00 | 0.169 | 9.54 |
| 4K | 6 | written-ahead/each | 4652 | 1606 | 2.92 | 1014 | 2614 | 2847 | 8.00 | 0.345 | 17.74 |

### 1. A whole chunk (chunk), round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | A | 12.00 | 46.91 | 0.040 | 466.6 | 864.2 | 35.77 | 448.8 | 1927 | 3885 | 414.5 | 25.91 | 76.00 | 4.00 | 327.6 | 0.479 |
| 64K | 1 | A+ | 12.00 | 46.85 | 26.48 | 445.1 | 951.9 | 28.77 | 663.0 | 2157 | 3771 | 399.2 | 24.95 | 78.00 | 4.00 | 287.8 | 0.461 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 667.3 | 501.2 | 0 | 0 | 1076 | 1594 | 866.3 | 54.14 | 68.33 | 1.17 | 129.4 | 1.00 |
| 64K | 6 | A-each | 12.00 | 383.1 | 0.020 | 2786 | 21950 | 37.56 | 2214 | 50913 | 52729 | 158.4 | 9.90 | 72.67 | 1.83 | 263.6 | 0.319 |
| 64K | 6 | A-batch | 12.00 | 677.4 | 0.040 | 2174 | 26957 | 38.38 | 1512 | 31353 | 31353 | 220.9 | 13.81 | 72.67 | 1.33 | 355.4 | 0.444 |
| 64K | 6 | A+-each | 12.00 | 525.2 | 155.4 | 1490 | 15764 | 62.58 | 2857 | 20992 | 21079 | 285.3 | 17.83 | 71.33 | 1.58 | 269.2 | 0.574 |
| 64K | 6 | A+-batch | 12.00 | 441.1 | 199.5 | 1415 | 26218 | 88.38 | 901.3 | 28993 | 28993 | 261.7 | 16.36 | 70.33 | 1.17 | 196.2 | 0.526 |
| 64K | 6 | A+-dironly | 12.00 | 452.5 | 156.1 | 1884 | 0.030 | 135.0 | 30268 | 32564 | 32564 | 260.8 | 16.30 | 69.67 | 0.333 | 189.0 | 0.524 |
| 64K | 6 | B-own | 12.00 | 0 | 0 | 1498 | 19995 | 0 | 0 | 21723 | 21841 | 372.3 | 23.27 | 68.33 | 0.500 | 135.6 | 0.749 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 1483 | 11022 | 0 | 0 | 12814 | 12814 | 497.2 | 31.08 | 68.33 | 0.333 | 130.4 | 1.00 |
| 1M | 1 | A | 12.00 | 160.0 | 0.080 | 3157 | 33303 | 169.4 | 670.5 | 37619 | 41846 | 27.24 | 27.24 | 1038 | 4.00 | 797.8 | 0.413 |
| 1M | 1 | A+ | 12.00 | 87.16 | 52.80 | 3214 | 24496 | 110.0 | 621.6 | 28522 | 31148 | 40.26 | 40.26 | 1036 | 4.00 | 604.9 | 0.610 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 2780 | 12041 | 0 | 0 | 14823 | 20299 | 65.98 | 65.98 | 1028 | 1.17 | 522.4 | 1.00 |
| 1M | 6 | A-each | 12.00 | 701.2 | 0.050 | 34199 | 64931 | 90.11 | 2010 | 99620 | 100082 | 62.12 | 62.12 | 1034 | 1.50 | 513.6 | 0.552 |
| 1M | 6 | A-batch | 12.00 | 705.3 | 0.050 | 10154 | 53341 | 84.21 | 1282 | 88699 | 88699 | 79.03 | 79.03 | 1032 | 1.33 | 462.1 | 0.702 |
| 1M | 6 | A+-each | 12.00 | 461.9 | 162.6 | 6896 | 49307 | 117.7 | 30907 | 86076 | 88207 | 81.60 | 81.60 | 1033 | 1.58 | 382.7 | 0.725 |
| 1M | 6 | A+-batch | 12.00 | 452.0 | 249.2 | 9098 | 62537 | 79.21 | 1390 | 77591 | 77591 | 82.92 | 82.92 | 1033 | 1.33 | 383.0 | 0.737 |
| 1M | 6 | A+-dironly | 12.00 | 492.5 | 159.1 | 9297 | 0.030 | 26.97 | 42938 | 56822 | 56822 | 105.8 | 105.8 | 1029 | 0.333 | 247.6 | 0.941 |
| 1M | 6 | B-own | 12.00 | 0 | 0 | 8848 | 50248 | 0 | 0 | 60473 | 60631 | 104.0 | 104.0 | 1028 | 0.500 | 201.8 | 0.925 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 9287 | 44030 | 0 | 0 | 57227 | 57227 | 112.5 | 112.5 | 1028 | 0.333 | 209.2 | 1.00 |
| 1M | 6 | A+-batch-replace | 12.00 | 372.2 | 180.1 | 8821 | 46337 | 92.84 | 1460 | 59553 | 59553 | 108.0 | 108.0 | 1033 | 1.33 | 330.4 | 0.960 |

### 4. Removing chunks, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| size | side | n | cold | unlink p50 µs | unlink p99 µs | removes s | dir sync ms | drain ms | µs/chunk in all | dev KiB/chunk | discard KiB/chunk | cpu µs/chunk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1M | chunks | 50.00 | yes | 24.74 | 9240 | 0.012 | 1.21 | 0.010 | 267.9 | 0.400 | 0 | 45.20 |
| 1M | trees | 50.00 | yes | 43.34 | 60264 | 0.352 | 8.53 | 0.015 | 7203 | 0.480 | 0 | 130.8 |
| 1M | raw | 50.00 | yes | 18.36 | 765.4 | 0.004 | 1.55 | 1.81 | 140.1 | 1.68 | 0 | 35.16 |
| 4M | chunks | 50.00 | yes | 25.05 | 666.0 | 0.003 | 74.24 | 1.63 | 1585 | 1.28 | 0 | 46.91 |
| 4M | trees | 50.00 | yes | 25.73 | 603.4 | 0.014 | 23.06 | 101.8 | 2768 | 4.72 | 0 | 56.90 |
| 4M | raw | 50.00 | yes | 17.68 | 16156 | 0.051 | 1.30 | 0.021 | 1053 | 0.400 | 0 | 45.32 |

### 8. One device, several slices, round 1 (slices on cpus 8/24, 9/25, 10/26, 11/27, 12/28, 13/29, 14/30, 15/31, 1/17, 2/18, 3/19, 4/20, 5/21, 6/22, 7/23, 0/16, blocking thread on the sibling; checksum at 40.5 GiB/s a core)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| cell | MiB/s | ops/s | p50 µs | p99 µs | p99.9 µs | thread CPU mean | thread CPU max | runtime max | sleeps/op | core busy max | process cores | runtime + checksum | sustainable MiB/s | device write MiB/s | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fio r64 | 33.07 | 500.5 | 0 | 341836 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio r4 | 3.12 | 769.5 | 0 | 341836 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio w-seq-1M | 155.3 | 148.7 | 0 | 85459 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| r64 slices=1 | 162.2 | 2595 | 3463 | 151539 | 296689 | 0.062 | 0.062 | 0.017 | 1.02 | 0.074 | 0.063 | 0.021 | 162.2 | 0 | 0 |
| r64 slices=2 | 238.7 | 3819 | 11558 | 127438 | 410610 | 0.041 | 0.046 | 0.015 | 0.980 | 0.074 | 0.082 | 0.018 | 238.7 | 0 | 0 |
| r64-T slices=1 | 374.4 | 5991 | 5188 | 7309 | 7607 | 0.131 | 0.131 | 0.036 | 1.01 | 0.115 | 0.131 | 0.045 | 374.4 | 0 | 0 |
| r64-T slices=2 | 329.2 | 5267 | 5364 | 10410 | 19305 | 0.061 | 0.061 | 0.017 | 1.01 | 0.058 | 0.122 | 0.021 | 329.2 | 0 | 0 |
| r4 slices=1 | 35.85 | 9178 | 3483 | 4379 | 4992 | 0.185 | 0.185 | 0.053 | 0.973 | 0.216 | 0.185 | 0.054 | 35.85 | 0 | 0 |
| r4 slices=2 | 26.00 | 6656 | 8151 | 29026 | 42418 | 0.068 | 0.074 | 0.024 | 1.01 | 0.077 | 0.136 | 0.024 | 26.00 | 0 | 0 |
| r4-T slices=1 | 36.45 | 9330 | 3433 | 3696 | 3753 | 0.193 | 0.193 | 0.055 | 1.00 | 0.143 | 0.193 | 0.056 | 36.45 | 0 | 0 |
| r4-T slices=2 | 25.59 | 6551 | 3802 | 8329 | 16743 | 0.071 | 0.071 | 0.020 | 1.01 | 0.058 | 0.141 | 0.020 | 25.59 | 0 | 0 |
| w1M slices=1 | 90.00 | 90.00 | 60912 | 60976 | 60976 | 0.022 | 0.022 | 0.009 | 3.04 | 0.037 | 0.036 | 0.011 | 90.00 | 113.2 | 127.4 |
| w1M slices=2 | 0 | 0 | 0 | 0 | 0 | 0.007 | 0.007 | 0.003 | 54.00 | 0 | 0.022 | 0.003 | 0 | 64.28 | 71.25 |
| w1M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.014 | 0.014 | 0.007 | 42.00 | 0.037 | 0.025 | 0.007 | 0 | 90.72 | 59.99 |
| w1M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.006 | 0.006 | 0.003 | 39.00 | 0 | 0.019 | 0.003 | 0 | 60.65 | 52.50 |
| w4M slices=1 | 0 | 0 | 0 | 0 | 0 | 0.007 | 0.007 | 0.003 | 37.00 | 0 | 0.010 | 0.003 | 0 | 93.94 | 33.74 |
| w4M slices=2 | 0 | 0 | 0 | 0 | 0 | 0.002 | 0.002 | 0.001 | 20.00 | 0 | 0.005 | 0.001 | 0 | 61.73 | 3.75 |
| w4M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.004 | 0.004 | 0.002 | 20.00 | 0.037 | 0.006 | 0.002 | 0 | 117.6 | 7.50 |
| w4M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.003 | 0.003 | 0.002 | 30.00 | 0 | 0.009 | 0.002 | 0 | 105.4 | 7.50 |
| depth-r64 depth=1 | 129.1 | 2066 | 295.9 | 6600 | 13757 | 0.048 | 0.048 | 0.013 | 1.00 | 0.019 | 0.048 | 0.016 | 129.1 | 0 | 0 |
| depth-r64 depth=32 | 358.5 | 5736 | 4356 | 13622 | 19169 | 0.128 | 0.128 | 0.034 | 1.01 | 0.115 | 0.128 | 0.043 | 358.5 | 0 | 0 |
| depth-r4 depth=1 | 19.12 | 4896 | 124.3 | 984.6 | 6532 | 0.103 | 0.103 | 0.030 | 1.00 | 0.078 | 0.103 | 0.030 | 19.12 | 0 | 0 |
| depth-r4 depth=32 | 28.44 | 7281 | 3594 | 8701 | 14524 | 0.151 | 0.151 | 0.043 | 1.01 | 0.154 | 0.151 | 0.044 | 28.44 | 0 | 0 |
| w1M slices=1 sentinel | 90.00 | 90.00 | 60225 | 61183 | 61183 | 0.012 | 0.012 | 0.005 | 3.00 | 0.037 | 0.019 | 0.007 | 90.00 | 113.4 | 153.7 |

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write back)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite | 1059 | 800.1 | 1839 | 2253 | 1.00 | 4.01 | 0.981 |
| 6 | overwrite | 1515 | 3509 | 12786 | 12813 | 0.392 | 4.06 | 0.972 |

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | capacity | 0 | 72.00 | no | 84.75 | 87.31 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | idle | 0 | 0 | no | 0 | 0 | 14.07 | 29.30 | 29.30 | 14.48 | 29.31 | 0.726 | 0.922 | 0.282 | 20.25 |
| 8 | arrival-qd1 | 36.00 | 36.00 | no | 93.54 | 107.8 | 1.59 | 83.70 | 83.70 | 2.62 | 97.58 | 0.573 | 0.841 | 0.210 | 25.50 |
| 8 | offset-qd1 | 36.00 | 36.00 | no | 93.01 | 106.4 | 1.36 | 85.77 | 85.77 | 2.13 | 86.30 | 0.615 | 1.04 | 0.153 | 25.51 |
| 8 | offset-ino | 36.00 | 36.00 | no | 92.32 | 105.5 | 1.46 | 94.60 | 94.60 | 2.12 | 95.24 | 0.580 | 0.917 | 0.155 | 25.50 |
| 8 | kernel | 36.00 | 36.00 | no | 91.72 | 105.4 | 1.49 | 84.62 | 84.62 | 2.10 | 85.15 | 0.704 | 1.01 | 0.146 | 25.50 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 2.86 | 3.54 | 3.54 | 1.42 | 1.72 | 0.028 | 21.00 |
| 10 | 9.01 | 3.05 | 44.96 | 44.96 | 1.47 | 1.79 | 0.106 | 21.00 |
| unbounded | 81.08 | 87.45 | 111.7 | 111.7 | 12.36 | 112.6 | 0.845 | 20.25 |

### X7 · One executor, an SSD's slice and a disk's, round 1 (SSD: 64 KiB reads at 1000/s and 4 KiB stages at 200/s, open loop; disk: applies in batches of 32, reads 4 deep, 1 MiB chunks renamed; executors on cpus 8 and 9)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| arrangement | SSD read p50 µs | SSD read p99 µs | SSD read p99.9 µs | SSD stage p50 µs | SSD stage p99 µs | SSD executor busy | disk executor busy | disk applies/s | disk reads/s | disk chunks/s | rename p50 µs | dir sync p50 ms | disk executor µs/op | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ssd-alone | 57.28 | 72.87 | 87.91 | 72.62 | 93.88 | 0.039 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| shared | 61.14 | 77.73 | 1377 | 77.55 | 99.66 | 0.049 | 0.049 | 72.00 | 34.50 | 1.50 | 37.67 | 223.9 | 0 | 0.599 |
| separate | 57.42 | 72.50 | 91.42 | 73.36 | 90.58 | 0.039 | 0.007 | 72.00 | 40.50 | 1.50 | 65.70 | 206.8 | 61.20 | 0.638 |

### 1. A whole chunk (chunk-recycle), round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | R | 12.00 | 31.35 | 0.030 | 414.1 | 332.3 | 37.27 | 659.5 | 1470 | 4165 | 529.8 | 33.11 | 72.00 | 3.00 | 280.3 | 0.621 |
| 64K | 1 | A+ | 12.00 | 47.49 | 26.70 | 425.3 | 937.5 | 34.05 | 616.5 | 2122 | 4286 | 394.9 | 24.68 | 76.00 | 4.00 | 350.0 | 0.463 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 764.3 | 497.3 | 0 | 0 | 1245 | 1614 | 853.6 | 53.35 | 68.33 | 1.17 | 149.9 | 1.00 |
| 64K | 6 | R-batch | 12.00 | 148.2 | 0.040 | 2123 | 24686 | 116.0 | 640.8 | 27881 | 27881 | 259.4 | 16.21 | 68.67 | 0.667 | 214.2 | 0.684 |
| 64K | 6 | A+-batch | 12.00 | 689.5 | 264.9 | 1355 | 30925 | 115.7 | 1465 | 34355 | 34355 | 245.5 | 15.34 | 70.33 | 1.17 | 236.7 | 0.648 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 1550 | 19867 | 0 | 0 | 21619 | 21619 | 378.9 | 23.68 | 68.33 | 0.333 | 139.2 | 1.00 |
| 1M | 1 | R | 12.00 | 62.96 | 0.070 | 2607 | 23459 | 118.4 | 656.6 | 27063 | 31238 | 37.91 | 37.91 | 1032 | 3.00 | 655.6 | 0.514 |
| 1M | 1 | A+ | 12.00 | 129.7 | 82.15 | 3145 | 35576 | 163.2 | 685.7 | 39733 | 42268 | 27.64 | 27.64 | 1036 | 4.00 | 808.7 | 0.375 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 2741 | 9891 | 0 | 0 | 12631 | 22841 | 73.78 | 73.78 | 1028 | 1.17 | 449.8 | 1.00 |
| 1M | 6 | R-batch | 12.00 | 151.8 | 0.040 | 9143 | 44136 | 206.8 | 671.8 | 53322 | 53322 | 113.7 | 113.7 | 1029 | 0.667 | 238.3 | 0.858 |
| 1M | 6 | A+-batch | 12.00 | 513.6 | 124.8 | 8731 | 65031 | 75.90 | 1455 | 79400 | 79400 | 82.89 | 82.89 | 1032 | 1.33 | 306.1 | 0.626 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 9072 | 34426 | 0 | 0 | 47640 | 47640 | 132.5 | 132.5 | 1028 | 0.333 | 214.8 | 1.00 |

