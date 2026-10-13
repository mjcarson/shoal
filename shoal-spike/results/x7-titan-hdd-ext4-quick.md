### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8088 µs |
| fdatasync of a clean file | p50 491.8 µs, p99 520.3 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 18 KiB and 2.00 flushes a rename | p50 16620 µs |
| rename, then the directory's Fsync | 20 KiB and 2.00 flushes a rename | p50 16635 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | refused: Operation not supported (os error 95) | FIEMAP on the target: Ok(Extents { total: 1, shared: 0, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.23 µs, p99 33.76 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Sequential and random direct I/O, round 1 (population from 0.0% to 0.0% of the device, 1 GiB apart (p5 to p95); 16 files of 64 MiB)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| cell | side | MiB/s | ops/s | p50 µs | p99 µs | disk busy | merges/op | file at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write piece=1M depth=1 | seq-write | 190.5 | 190.5 | 3507 | 12622 | 0.981 | 0.093 | 0.0% |
| read piece=1M depth=1 | seq-read | 214.1 | 214.1 | 3508 | 12577 | 0.996 | 0 | 0.0% |
| write piece=1M depth=32 | seq-write | 177.7 | 177.7 | 147248 | 178703 | 0.996 | 0.078 | 0.0% |
| read piece=1M depth=32 | seq-read | 207.5 | 207.5 | 149381 | 161336 | 0.982 | 0 | 0.0% |
| random size=4K depth=1 | random | 0.710 | 181.9 | 5894 | 9584 | 0.986 | 0 | - |
| random size=4K depth=32 | random | 1.93 | 493.1 | 26823 | 217118 | 0.979 | 0 | - |
| random size=1M depth=1 | random | 108.8 | 108.8 | 9753 | 14085 | 0.996 | 0 | - |
| random size=1M depth=32 | random | 97.50 | 97.50 | 138256 | 349476 | 0.998 | 0 | - |
| short size=64K depth=1 | short | 31.41 | 502.5 | 464.1 | 8854 | 0.990 | 0 | - |
| short size=64K depth=4 | short | 182.6 | 2921 | 1164 | 7486 | 0.883 | 0 | - |
| fio w-seq-1M | fio | 212.2 | 205.4 | 38535 | 54264 | 0 | 0 | - |
| fio r-seq-1M | fio | 170.8 | 164.1 | 26608 | 633340 | 0 | 0 | - |
| fio r64-qd32 | fio | 23.73 | 352.6 | 50594 | 484442 | 0 | 0 | - |

### 6. One unit at a random offset, round 1 (µs; queue depth 1; population from 21.9% to 21.9% of the device, 0 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| unit | level | n | open p50 | open p99 | read p50 | read p99 | close p50 | total p50 | total p99 | total p99.9 | dev KiB read/sample |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | cold | 5.00 | 767.1 | 1156 | 439.1 | 10094 | 15.92 | 1557 | 10863 | 10863 | 17.60 |
| 4K | dentry-warm | 20.00 | 45.96 | 57.46 | 415.9 | 6721 | 13.81 | 464.9 | 6768 | 6768 | 8.00 |
| 4K | open | 20.00 | 0 | 0 | 341.8 | 6346 | 0 | 341.8 | 6346 | 6346 | 4.00 |
| 64K | cold | 5.00 | 767.6 | 779.9 | 3774 | 5268 | 17.07 | 4554 | 6035 | 6035 | 76.00 |
| 64K | dentry-warm | 20.00 | 50.96 | 67.66 | 514.6 | 7649 | 15.00 | 565.4 | 7716 | 7716 | 68.00 |
| 64K | open | 20.00 | 0 | 0 | 451.8 | 514.7 | 0 | 451.8 | 514.7 | 514.7 | 64.00 |

### 5. Listing: the populations built, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| population | files | built in s | files/s |
| --- | --- | --- | --- |
| deep-2K | 2000 | 0.507 | 3942 |
| wide-2K | 2000 | 0.344 | 5808 |

### 5. Listing a placement group, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| population | walk | chunks | cold s | cold µs/chunk | cold dev KiB read/chunk | warm s | warm µs/chunk | warm dev KiB written | hours/16 TiB at 1 MiB | at 4 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deep-2K | names | 2000 | 0.035 | 17.54 | 0.328 | 0.002 | 0.909 | 0 | 0.082 | 0.020 |
| deep-2K | statx | 2000 | 0.035 | 17.73 | 0.328 | 0.006 | 2.87 | 0 | 0.083 | 0.021 |
| deep-2K | statx-ino | 2000 | 0.036 | 18.12 | 0.328 | 0.007 | 3.47 | 0 | 0.084 | 0.021 |
| deep-2K | xattr | 2000 | 0.043 | 21.35 | 0.328 | 0.013 | 6.66 | 0 | 0.100 | 0.025 |
| deep-2K | header-qd32 | 2000 | 0.131 | 65.66 | 4.33 | 0.078 | 39.20 | 0 | 0.306 | 0.077 |
| wide-2K | names | 2000 | 0.686 | 343.1 | 4.67 | 0.018 | 8.76 | 0 | 1.60 | 0.400 |
| wide-2K | statx | 2000 | 0.692 | 346.1 | 4.67 | 0.022 | 10.95 | 0 | 1.61 | 0.403 |
| wide-2K | statx-ino | 2000 | 0.685 | 342.6 | 4.67 | 0.023 | 11.49 | 0 | 1.60 | 0.399 |
| wide-2K | xattr | 2000 | 0.692 | 346.2 | 4.67 | 0.029 | 14.62 | 0 | 1.61 | 0.403 |
| wide-2K | header-qd32 | 2000 | 0.793 | 396.4 | 8.67 | 0.120 | 59.89 | 0 | 1.85 | 0.462 |

### 3. A partial write, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write back)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J | 12690 | 24948 | 20647 | 20795 | 20223 | 20371 | 0 | 0 | 0 | 33346 | 37427 | 17.50 | 4.38 | 2.12 | 303.6 |
| 4K | 1 | J-ssd | 929.9 | 1828 | 19381 | 30345 | 18952 | 28698 | 0 | 0 | 0 | 20316 | 32173 | 9.50 | 2.38 | 1.12 | 310.0 |
| 4K | 6 | J | 24502 | 37570 | 37917 | 38668 | 36888 | 37261 | 0 | 0 | 0 | 63094 | 69832 | 17.50 | 4.38 | 0.875 | 159.7 |
| 4K | 6 | J-ssd | 1406 | 2033 | 50074 | 50740 | 30570 | 47956 | 0 | 0 | 0 | 51719 | 52184 | 9.50 | 2.38 | 0.625 | 171.1 |
| 64K | 1 | J | 15699 | 32085 | 18711 | 26271 | 18159 | 25712 | 0 | 0 | 0 | 33352 | 52137 | 137.5 | 2.15 | 2.12 | 351.8 |
| 64K | 1 | J-ssd | 1090 | 1954 | 21448 | 40925 | 20880 | 39151 | 0 | 0 | 0 | 22530 | 42879 | 69.50 | 1.09 | 1.12 | 324.5 |
| 64K | 6 | J | 34026 | 41142 | 40869 | 44219 | 38668 | 42365 | 0 | 0 | 0 | 77354 | 82098 | 137.5 | 2.15 | 0.875 | 172.9 |
| 64K | 6 | J-ssd | 2105 | 15293 | 45064 | 55532 | 39246 | 42363 | 0 | 0 | 0 | 57482 | 60448 | 69.50 | 1.09 | 0.625 | 223.6 |

### 2. The journal, round 1 (a 4 KiB header block on every record; write cache write back)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | allocated/coalesced | 20.76 | 20.76 | 1.00 | 41719 | 73962 | 73962 | 20.80 | 2.20 | 183.9 |
| 4K | 1 | allocated/each | 20.77 | 20.77 | 1.00 | 41725 | 73887 | 73887 | 20.80 | 2.20 | 200.4 |
| 4K | 1 | appended/coalesced | 21.46 | 21.46 | 1.00 | 41759 | 65910 | 65910 | 35.20 | 2.20 | 214.5 |
| 4K | 1 | appended/each | 26.69 | 26.69 | 1.00 | 33445 | 57560 | 57560 | 34.67 | 2.17 | 200.4 |
| 4K | 1 | ssd-written-ahead/each | 960.5 | 966.0 | 1.00 | 958.6 | 3812 | 4139 | 8.00 | 1.00 | 122.2 |
| 4K | 1 | written-ahead/coalesced | 116.4 | 116.4 | 1.00 | 8365 | 13744 | 13744 | 8.67 | 1.08 | 134.6 |
| 4K | 1 | written-ahead/each | 88.91 | 88.91 | 1.00 | 8364 | 60326 | 60326 | 8.89 | 1.11 | 140.3 |
| 4K | 6 | allocated/coalesced | 124.5 | 20.76 | 6.00 | 41725 | 74005 | 74005 | 10.13 | 0.367 | 30.54 |
| 4K | 6 | allocated/each | 85.50 | 27.20 | 3.14 | 65412 | 83418 | 83418 | 11.45 | 0.636 | 54.80 |
| 4K | 6 | appended/coalesced | 111.1 | 18.52 | 6.00 | 50129 | 65616 | 65616 | 13.00 | 0.375 | 43.27 |
| 4K | 6 | appended/each | 35.70 | 19.47 | 1.83 | 149953 | 231846 | 231846 | 23.27 | 1.18 | 218.8 |
| 4K | 6 | ssd-written-ahead/each | 4936 | 1658 | 3.00 | 1121 | 3973 | 4007 | 8.00 | 0.333 | 38.74 |
| 4K | 6 | written-ahead/coalesced | 694.4 | 121.2 | 6.00 | 8530 | 8605 | 8605 | 8.12 | 0.181 | 24.24 |
| 4K | 6 | written-ahead/each | 686.7 | 119.9 | 6.00 | 8529 | 8600 | 8601 | 8.11 | 0.181 | 26.91 |

### 1. A whole chunk (chunk), round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | A | 12.00 | 120.1 | 0.230 | 741.5 | 49341 | 84.34 | 16642 | 66909 | 77080 | 14.46 | 0.904 | 136.7 | 4.08 | 639.0 | 0.191 |
| 64K | 1 | A+ | 12.00 | 119.5 | 64.10 | 498.4 | 41139 | 81.34 | 16627 | 58515 | 71001 | 16.58 | 1.04 | 119.7 | 4.08 | 601.2 | 0.219 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1344 | 8060 | 0 | 0 | 8564 | 64458 | 75.59 | 4.72 | 69.33 | 1.17 | 194.1 | 1.00 |
| 64K | 6 | A-each | 12.00 | 754.1 | 0.080 | 31198 | 89309 | 321.5 | 47522 | 157411 | 169349 | 36.69 | 2.29 | 95.00 | 1.58 | 488.1 | 0.070 |
| 64K | 6 | A-batch | 12.00 | 595.2 | 0.049 | 3426 | 123174 | 100.8 | 16611 | 160052 | 160052 | 46.07 | 2.88 | 87.00 | 1.08 | 403.1 | 0.088 |
| 64K | 6 | A+-each | 12.00 | 695.6 | 235.9 | 12665 | 72292 | 291.1 | 40649 | 133763 | 139406 | 47.38 | 2.96 | 89.67 | 1.42 | 377.1 | 0.090 |
| 64K | 6 | A+-batch | 12.00 | 592.4 | 215.4 | 2285 | 64637 | 94.64 | 16578 | 92690 | 92690 | 68.04 | 4.25 | 84.33 | 1.08 | 320.6 | 0.130 |
| 64K | 6 | A+-dironly | 12.00 | 701.0 | 188.0 | 11764 | 0.080 | 68.06 | 66173 | 79751 | 79751 | 86.75 | 5.42 | 80.67 | 0.417 | 310.1 | 0.165 |
| 64K | 6 | B-own | 12.00 | 0 | 0 | 11724 | 14676 | 0 | 0 | 20729 | 21300 | 314.1 | 19.63 | 69.33 | 0.500 | 113.2 | 0.598 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2395 | 10408 | 0 | 0 | 12834 | 12834 | 525.3 | 32.83 | 69.33 | 0.333 | 56.04 | 1.00 |
| 1M | 1 | A | 12.00 | 134.0 | 0.100 | 2951 | 55453 | 94.48 | 16632 | 75209 | 108519 | 12.82 | 12.82 | 1093 | 4.08 | 771.7 | 0.162 |
| 1M | 1 | A+ | 12.00 | 120.6 | 60.29 | 2618 | 47374 | 81.66 | 16647 | 66894 | 99205 | 14.51 | 14.51 | 1087 | 4.08 | 645.7 | 0.184 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3520 | 8396 | 0 | 0 | 11887 | 18672 | 79.01 | 79.01 | 1029 | 1.17 | 216.8 | 1.00 |
| 1M | 6 | A-each | 12.00 | 711.2 | 0.050 | 36135 | 101988 | 291.8 | 44938 | 174480 | 183725 | 35.10 | 35.10 | 1051 | 1.42 | 488.7 | 0.209 |
| 1M | 6 | A-batch | 12.00 | 721.5 | 0.040 | 16237 | 127382 | 119.8 | 16738 | 167103 | 167103 | 38.87 | 38.87 | 1049 | 1.08 | 461.6 | 0.231 |
| 1M | 6 | A+-each | 12.00 | 689.7 | 238.2 | 25873 | 79155 | 312.3 | 42113 | 147246 | 167210 | 42.55 | 42.55 | 1051 | 1.42 | 528.7 | 0.253 |
| 1M | 6 | A+-batch | 12.00 | 600.6 | 223.1 | 16275 | 102367 | 99.85 | 16535 | 140814 | 140814 | 45.10 | 45.10 | 1044 | 1.08 | 347.1 | 0.268 |
| 1M | 6 | A+-dironly | 12.00 | 711.9 | 214.5 | 12823 | 0.080 | 61.77 | 75983 | 91885 | 91885 | 71.85 | 71.85 | 1040 | 0.417 | 369.1 | 0.427 |
| 1M | 6 | B-own | 12.00 | 0 | 0 | 10119 | 34552 | 0 | 0 | 44132 | 44227 | 136.1 | 136.1 | 1029 | 0.500 | 145.2 | 0.809 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 10349 | 25178 | 0 | 0 | 39915 | 39915 | 168.3 | 168.3 | 1029 | 0.333 | 113.3 | 1.00 |
| 1M | 6 | A+-batch-replace | 12.00 | 303.4 | 204.1 | 15133 | 104874 | 252.2 | 16550 | 142824 | 142824 | 45.00 | 45.00 | 1052 | 1.08 | 387.0 | 0.267 |

### 4. Removing chunks, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| size | side | n | cold | unlink p50 µs | unlink p99 µs | removes s | dir sync ms | drain ms | µs/chunk in all | dev KiB/chunk | discard KiB/chunk | cpu µs/chunk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1M | chunks | 50.00 | yes | 75.63 | 1109 | 0.008 | 211.4 | 131.4 | 7018 | 2.56 | 0 | 115.4 |
| 1M | trees | 50.00 | yes | 76.84 | 2556 | 0.013 | 182.7 | 130.3 | 6526 | 2.96 | 0 | 151.1 |
| 1M | raw | 50.00 | yes | 33.93 | 1042 | 0.008 | 163.9 | 129.5 | 6021 | 2.72 | 0 | 55.21 |
| 4M | chunks | 50.00 | yes | 75.65 | 1124 | 0.008 | 189.2 | 137.4 | 6693 | 2.56 | 0 | 116.3 |
| 4M | trees | 50.00 | yes | 77.14 | 2596 | 0.013 | 160.4 | 136.6 | 6193 | 2.64 | 0 | 149.6 |
| 4M | raw | 50.00 | yes | 31.54 | 1072 | 0.010 | 172.2 | 135.8 | 6355 | 2.24 | 0 | 49.82 |

### 8. One device, several slices, round 1 (slices on cpus 2/3, 4/5, 6/7, 0/1, blocking thread on the sibling; checksum at 11.4 GiB/s a core)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| cell | MiB/s | ops/s | p50 µs | p99 µs | p99.9 µs | thread CPU mean | thread CPU max | runtime max | sleeps/op | core busy max | process cores | runtime + checksum | sustainable MiB/s | device write MiB/s | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fio r64 | 27.28 | 407.9 | 0 | 476054 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio r4 | 1.49 | 354.5 | 0 | 549454 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio w-seq-1M | 190.3 | 183.6 | 0 | 69730 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| r64 slices=1 | 194.9 | 3118 | 7587 | 60184 | 114575 | 0.162 | 0.162 | 0.045 | 1.02 | 0.154 | 0.162 | 0.062 | 194.9 | 0 | 0 |
| r64 slices=2 | 115.7 | 1851 | 18101 | 229395 | 234512 | 0.042 | 0.044 | 0.014 | 1.01 | 0.056 | 0.085 | 0.019 | 115.7 | 0.022 | 3.75 |
| r64-T slices=1 | 257.5 | 4119 | 7265 | 12212 | 41307 | 0.210 | 0.210 | 0.059 | 1.02 | 0.226 | 0.210 | 0.081 | 257.5 | 0 | 0 |
| r64-T slices=2 | 204.1 | 3266 | 9010 | 31484 | 44244 | 0.086 | 0.087 | 0.024 | 1.02 | 0.094 | 0.172 | 0.033 | 204.1 | 0 | 0 |
| r4 slices=1 | 23.91 | 6120 | 4727 | 20375 | 28407 | 0.122 | 0.122 | 0.039 | 0.118 | 0.130 | 0.122 | 0.041 | 23.91 | 0 | 0 |
| r4 slices=2 | 25.93 | 6638 | 9073 | 20194 | 29144 | 0.079 | 0.080 | 0.025 | 0.195 | 0.094 | 0.158 | 0.026 | 25.93 | 0 | 0 |
| r4-T slices=1 | 25.11 | 6428 | 4347 | 18889 | 70102 | 0.119 | 0.119 | 0.039 | 0.085 | 0.130 | 0.119 | 0.041 | 25.11 | 0.249 | 0 |
| r4-T slices=2 | 27.68 | 7086 | 4421 | 6014 | 9354 | 0.099 | 0.100 | 0.032 | 0.236 | 0.094 | 0.199 | 0.033 | 27.68 | 0 | 0 |
| w1M slices=1 | 0 | 0 | 0 | 0 | 0 | 0.008 | 0.008 | 0.003 | 28.00 | 0.037 | 0.012 | 0.003 | 0 | 34.31 | 41.25 |
| w1M slices=2 | 45.00 | 45.00 | 167071 | 167071 | 167071 | 0.008 | 0.008 | 0.003 | 2.92 | 0 | 0.026 | 0.005 | 45.00 | 45.74 | 45.00 |
| w1M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.014 | 0.014 | 0.006 | 42.00 | 0.071 | 0.022 | 0.006 | 0 | 68.04 | 26.25 |
| w1M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.006 | 0.008 | 0.003 | 46.00 | 0.037 | 0.019 | 0.003 | 0 | 72.00 | 26.25 |
| w4M slices=1 | 0 | 0 | 0 | 0 | 0 | 0.011 | 0.011 | 0.004 | 45.00 | 0.037 | 0.015 | 0.004 | 0 | 144.3 | 22.51 |
| w4M slices=2 | 0 | 0 | 0 | 0 | 0 | 0.003 | 0.003 | 0.001 | 13.00 | 0.250 | 0.008 | 0.001 | 0 | 120.0 | 3.75 |
| w4M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.006 | 0.006 | 0.001 | 25.00 | 0 | 0.010 | 0.001 | 0 | 232.8 | 0 |
| w4M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.000 | 0.000 | 0 | 0 | 0 | 0.005 | 0 | 0 | 200.1 | 0 |
| depth-r64 depth=1 | 69.73 | 1116 | 441.3 | 9203 | 9837 | 0.062 | 0.062 | 0.016 | 1.00 | 0.058 | 0.062 | 0.022 | 69.73 | 0 | 0 |
| depth-r64 depth=32 | 229.1 | 3666 | 7837 | 36200 | 51133 | 0.184 | 0.184 | 0.052 | 1.02 | 0.189 | 0.184 | 0.072 | 229.1 | 0 | 0 |
| depth-r4 depth=1 | 11.68 | 2991 | 330.2 | 379.0 | 391.7 | 0.154 | 0.154 | 0.042 | 1.00 | 0.154 | 0.155 | 0.043 | 11.68 | 0 | 0 |
| depth-r4 depth=32 | 27.06 | 6928 | 4474 | 6482 | 9676 | 0.123 | 0.123 | 0.041 | 0.067 | 0.130 | 0.123 | 0.043 | 27.06 | 0 | 0 |
| w1M slices=1 sentinel | 22.50 | 22.50 | 133683 | 133683 | 133683 | 0.009 | 0.009 | 0.003 | 3.83 | 0 | 0.015 | 0.005 | 22.50 | 45.79 | 45.00 |

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write back)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite | 117.0 | 8347 | 8409 | 8409 | 1.00 | 4.10 | 0.996 |
| 6 | overwrite | 144.0 | 39834 | 43659 | 43659 | 0.375 | 4.50 | 0.996 |

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 21.9% of the device, 2860 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | capacity | 0 | 120.0 | no | 53.21 | 58.73 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | idle | 0 | 0 | no | 0 | 0 | 74.95 | 84.09 | 84.09 | 233.2 | 283.2 | 1.01 | 3.91 | 0.311 | 9.75 |
| 8 | arrival-qd1 | 60.00 | 6.00 | **yes** | 699.2 | 699.2 | 25.90 | 74.80 | 74.80 | 123.6 | 173.6 | 1.01 | 1.19 | 0.217 | 12.00 |
| 8 | offset-qd1 | 60.00 | 0 | **yes** | 0 | 0 | 36.64 | 76.46 | 76.46 | 223.5 | 273.5 | 1.02 | 1.24 | 0.242 | 11.25 |
| 8 | offset-ino | 60.00 | 0 | **yes** | 0 | 0 | 16.93 | 67.04 | 67.04 | 115.8 | 165.8 | 1.01 | 1.15 | 0.161 | 11.25 |
| 8 | kernel | 60.00 | 24.00 | **yes** | 319.4 | 321.8 | 54.96 | 115.9 | 115.9 | 238.1 | 419.3 | 1.02 | 16.28 | 0.257 | 12.75 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 21.9% of the device, 2860 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 223.7 | 274.0 | 274.0 | 51.51 | 101.4 | 0.269 | 19.50 |
| 10 | 9.01 | 254.0 | 358.8 | 358.8 | 51.14 | 110.1 | 0.271 | 17.25 |
| unbounded | 27.03 | 351.2 | 525.0 | 525.0 | 55.39 | 126.2 | 0.395 | 15.75 |

### X7 · One executor, an SSD's slice and a disk's, round 1 (SSD: 64 KiB reads at 1000/s and 4 KiB stages at 200/s, open loop; disk: applies in batches of 32, reads 4 deep, 1 MiB chunks renamed; executors on cpus 2 and 4)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| arrangement | SSD read p50 µs | SSD read p99 µs | SSD read p99.9 µs | SSD stage p50 µs | SSD stage p99 µs | SSD executor busy | disk executor busy | disk applies/s | disk reads/s | disk chunks/s | rename p50 µs | dir sync p50 ms | disk executor µs/op | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ssd-alone | 362.7 | 1380 | 3352 | 1132 | 4040 | 0.109 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| shared | 375.6 | 1893 | 4998 | 1149 | 2998 | 0.135 | 0.135 | 96.00 | 33.75 | 1.50 | 127.3 | 374.7 | 0 | 0.594 |
| separate | 370.4 | 491.0 | 662.0 | 1148 | 1262 | 0.111 | 0.012 | 48.00 | 35.25 | 2.25 | 104.1 | 216.0 | 137.5 | 0.600 |

### 1. A whole chunk (chunk-recycle), round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg titan hdd ext4 · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | R | 12.00 | 77.63 | 0.100 | 498.9 | 20065 | 73.82 | 26397 | 50033 | 57805 | 21.17 | 1.32 | 86.33 | 3.08 | 457.5 | 0.185 |
| 64K | 1 | A+ | 12.00 | 120.2 | 64.57 | 504.7 | 41125 | 84.98 | 16637 | 58534 | 66854 | 16.70 | 1.04 | 124.3 | 4.08 | 630.5 | 0.146 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1297 | 8063 | 0 | 0 | 8572 | 10135 | 114.1 | 7.13 | 69.33 | 1.17 | 180.5 | 1.00 |
| 64K | 6 | R-batch | 12.00 | 136.8 | 0.040 | 2107 | 48739 | 89.07 | 24292 | 75049 | 75049 | 85.13 | 5.32 | 71.67 | 0.750 | 239.5 | 0.601 |
| 64K | 6 | A+-batch | 12.00 | 611.0 | 281.6 | 2389 | 73834 | 71.06 | 16502 | 108724 | 108724 | 59.92 | 3.74 | 85.33 | 1.08 | 335.2 | 0.423 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2376 | 71567 | 0 | 0 | 73968 | 73968 | 141.8 | 8.86 | 69.33 | 0.333 | 56.75 | 1.00 |
| 1M | 1 | R | 12.00 | 75.73 | 0.091 | 2599 | 21510 | 72.81 | 26556 | 50056 | 58408 | 19.60 | 19.60 | 1047 | 3.08 | 485.2 | 0.259 |
| 1M | 1 | A+ | 12.00 | 118.0 | 64.10 | 2600 | 47326 | 79.80 | 16635 | 66839 | 96116 | 15.03 | 15.03 | 1080 | 4.08 | 635.0 | 0.199 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3443 | 9028 | 0 | 0 | 12529 | 24245 | 75.69 | 75.69 | 1029 | 1.17 | 227.7 | 1.00 |
| 1M | 6 | R-batch | 12.00 | 156.9 | 0.040 | 11777 | 97497 | 79.42 | 24075 | 135172 | 135172 | 47.62 | 47.62 | 1033 | 0.750 | 303.3 | 0.435 |
| 1M | 6 | A+-batch | 12.00 | 688.9 | 244.8 | 15297 | 113906 | 98.06 | 16526 | 152483 | 152483 | 40.76 | 40.76 | 1044 | 1.08 | 373.4 | 0.372 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 14204 | 28647 | 0 | 0 | 66270 | 66270 | 109.4 | 109.4 | 1029 | 0.333 | 127.1 | 1.00 |

