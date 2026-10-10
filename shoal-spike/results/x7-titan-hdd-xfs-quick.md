### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 5.36 KiB a sync · sync p50 8054 µs |
| fdatasync of a clean file | p50 491.3 µs, p99 518.8 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8231 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8242 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.01 µs, p99 33.89 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Sequential and random direct I/O, round 1 (population from 7.9% to 86.4% of the device, 10240 GiB apart (p5 to p95); 16 files of 64 MiB)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| cell | side | MiB/s | ops/s | p50 µs | p99 µs | disk busy | merges/op | file at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write piece=1M depth=1 | seq-write | 177.8 | 177.8 | 3951 | 12998 | 0.980 | 0 | 31.4% |
| read piece=1M depth=1 | seq-read | 185.1 | 185.1 | 3977 | 15758 | 0.991 | 0 | 31.4% |
| write piece=1M depth=32 | seq-write | 165.5 | 165.5 | 100346 | 247273 | 0.989 | 0 | 31.4% |
| read piece=1M depth=32 | seq-read | 182.1 | 182.1 | 165151 | 370142 | 0.988 | 0 | 31.4% |
| random size=4K depth=1 | random | 0.264 | 67.50 | 14931 | 23149 | 0.992 | 0 | - |
| random size=4K depth=32 | random | 0.593 | 151.9 | 85995 | 431600 | 0.994 | 0 | - |
| random size=1M depth=1 | random | 52.50 | 52.50 | 18283 | 26590 | 0.998 | 0 | - |
| random size=1M depth=32 | random | 41.25 | 41.25 | 175418 | 339834 | 0.998 | 0 | - |
| short size=64K depth=1 | short | 26.95 | 431.3 | 468.8 | 9588 | 0.990 | 0 | - |
| short size=64K depth=4 | short | 178.6 | 2858 | 1179 | 7987 | 0.928 | 0 | - |
| fio w-seq-1M | fio | 145.0 | 138.5 | 52691 | 115868 | 0 | 0 | - |
| fio r-seq-1M | fio | 142.0 | 135.4 | 53215 | 89653 | 0 | 0 | - |
| fio r64-qd32 | fio | 12.75 | 178.3 | 154141 | 574620 | 0 | 0 | - |

### 6. One unit at a random offset, round 1 (µs; queue depth 1; population from 94.2% to 94.2% of the device, 0 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| unit | level | n | open p50 | open p99 | read p50 | read p99 | close p50 | total p50 | total p99 | total p99.9 | dev KiB read/sample |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | cold | 5.00 | 1121 | 1162 | 422.4 | 509.4 | 16.15 | 1584 | 1630 | 1630 | 44.00 |
| 4K | dentry-warm | 20.00 | 44.76 | 53.63 | 364.4 | 480.6 | 13.26 | 410.8 | 534.2 | 534.2 | 8.00 |
| 4K | open | 20.00 | 0 | 0 | 342.6 | 368.1 | 0 | 342.6 | 368.1 | 368.1 | 4.00 |
| 64K | cold | 5.00 | 1161 | 1161 | 568.5 | 632.7 | 16.19 | 1730 | 1776 | 1776 | 104.0 |
| 64K | dentry-warm | 20.00 | 45.22 | 53.32 | 474.8 | 579.9 | 13.99 | 520.3 | 625.1 | 625.1 | 68.00 |
| 64K | open | 20.00 | 0 | 0 | 449.0 | 464.2 | 0 | 449.0 | 464.2 | 464.2 | 64.00 |

### 5. Listing: the populations built, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| population | files | built in s | files/s |
| --- | --- | --- | --- |
| deep-2K | 2000 | 2.41 | 829.5 |
| wide-2K | 2000 | 0.706 | 2834 |

### 5. Listing a placement group, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| population | walk | chunks | cold s | cold µs/chunk | cold dev KiB read/chunk | warm s | warm µs/chunk | warm dev KiB written | hours/16 TiB at 1 MiB | at 4 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deep-2K | names | 2000 | 0.040 | 20.03 | 0.322 | 0.001 | 0.640 | 0 | 0.093 | 0.023 |
| deep-2K | statx | 2000 | 0.059 | 29.34 | 0.666 | 0.005 | 2.58 | 0 | 0.137 | 0.034 |
| deep-2K | statx-ino | 2000 | 0.059 | 29.53 | 0.666 | 0.006 | 3.13 | 0 | 0.138 | 0.034 |
| deep-2K | xattr | 2000 | 0.066 | 33.12 | 0.666 | 0.013 | 6.75 | 0 | 0.154 | 0.039 |
| deep-2K | header-qd32 | 2000 | 0.356 | 178.0 | 4.67 | 0.214 | 107.0 | 0 | 0.830 | 0.207 |
| wide-2K | names | 2000 | 0.083 | 41.59 | 1.10 | 0.012 | 6.22 | 0 | 0.194 | 0.048 |
| wide-2K | statx | 2000 | 0.098 | 49.18 | 1.10 | 0.017 | 8.32 | 0 | 0.229 | 0.057 |
| wide-2K | statx-ino | 2000 | 0.101 | 50.33 | 1.10 | 0.018 | 8.87 | 0 | 0.235 | 0.059 |
| wide-2K | xattr | 2000 | 0.108 | 53.95 | 1.10 | 0.025 | 12.35 | 0 | 0.251 | 0.063 |
| wide-2K | header-qd32 | 2000 | 0.224 | 112.2 | 5.10 | 0.140 | 69.91 | 0 | 0.523 | 0.131 |

### 3. A partial write, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write back)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J | 10867 | 16387 | 14258 | 41895 | 13792 | 41429 | 0 | 0 | 0 | 30552 | 52120 | 16.50 | 4.12 | 2.12 | 302.4 |
| 4K | 1 | J-ssd | 993.5 | 1850 | 8561 | 22385 | 8097 | 20765 | 0 | 0 | 0 | 9562 | 24236 | 8.50 | 2.12 | 1.12 | 301.4 |
| 4K | 6 | J | 10952 | 15832 | 26053 | 30945 | 24734 | 29654 | 0 | 0 | 0 | 34586 | 46699 | 16.50 | 4.12 | 0.875 | 188.6 |
| 4K | 6 | J-ssd | 1458 | 1962 | 31684 | 48095 | 17043 | 45597 | 0 | 0 | 0 | 32698 | 50055 | 8.50 | 2.12 | 0.625 | 172.1 |
| 64K | 1 | J | 10916 | 28523 | 14742 | 23417 | 14147 | 22828 | 0 | 0 | 0 | 25042 | 38535 | 136.5 | 2.13 | 2.12 | 323.2 |
| 64K | 1 | J-ssd | 1093 | 3885 | 10313 | 46007 | 9724 | 44253 | 0 | 0 | 0 | 12090 | 47916 | 68.50 | 1.07 | 1.12 | 323.1 |
| 64K | 6 | J | 16699 | 23068 | 26998 | 40096 | 25270 | 38528 | 0 | 0 | 0 | 50063 | 56797 | 136.5 | 2.13 | 0.875 | 198.3 |
| 64K | 6 | J-ssd | 2082 | 2500 | 32812 | 39712 | 22448 | 34813 | 0 | 0 | 0 | 34893 | 42209 | 68.50 | 1.07 | 0.625 | 217.1 |

### 2. The journal, round 1 (a 4 KiB header block on every record; write cache write back)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | allocated/coalesced | 24.01 | 24.01 | 1.00 | 41678 | 41689 | 41689 | 12.00 | 2.00 | 192.3 |
| 4K | 1 | allocated/each | 28.79 | 28.79 | 1.00 | 33361 | 41582 | 41582 | 12.00 | 2.00 | 173.2 |
| 4K | 1 | appended/coalesced | 24.02 | 24.02 | 1.00 | 41684 | 41690 | 41690 | 12.00 | 2.00 | 233.2 |
| 4K | 1 | appended/each | 24.00 | 24.00 | 1.00 | 41680 | 41698 | 41698 | 12.00 | 2.00 | 239.8 |
| 4K | 1 | ssd-written-ahead/each | 998.1 | 1004 | 1.00 | 915.8 | 3813 | 3834 | 8.00 | 1.00 | 121.3 |
| 4K | 1 | written-ahead/coalesced | 111.8 | 111.8 | 1.00 | 8386 | 21308 | 21308 | 8.17 | 1.09 | 140.5 |
| 4K | 1 | written-ahead/each | 110.8 | 110.8 | 1.00 | 8384 | 23078 | 23078 | 8.17 | 1.09 | 183.8 |
| 4K | 6 | allocated/coalesced | 144.0 | 24.00 | 6.00 | 41694 | 41706 | 41706 | 8.67 | 0.333 | 34.03 |
| 4K | 6 | allocated/each | 81.49 | 30.02 | 2.71 | 66700 | 91456 | 91456 | 9.47 | 0.737 | 76.49 |
| 4K | 6 | appended/coalesced | 138.4 | 23.07 | 6.00 | 41689 | 49943 | 49943 | 8.67 | 0.333 | 38.30 |
| 4K | 6 | appended/each | 37.74 | 20.59 | 1.83 | 141708 | 241430 | 241430 | 10.18 | 1.09 | 171.6 |
| 4K | 6 | ssd-written-ahead/each | 4984 | 1678 | 2.99 | 1071 | 3927 | 3937 | 8.00 | 0.333 | 39.53 |
| 4K | 6 | written-ahead/coalesced | 622.8 | 109.3 | 6.00 | 8649 | 9341 | 9341 | 8.03 | 0.183 | 25.64 |
| 4K | 6 | written-ahead/each | 653.1 | 228.6 | 3.00 | 8643 | 9338 | 9338 | 8.03 | 0.349 | 53.03 |

### 1. A whole chunk (chunk), round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | A | 12.00 | 123.9 | 0.151 | 837.9 | 32347 | 70.18 | 8265 | 41622 | 83098 | 23.99 | 1.50 | 76.00 | 4.00 | 650.7 | 0.305 |
| 64K | 1 | A+ | 12.00 | 122.7 | 69.95 | 512.8 | 24338 | 69.00 | 8256 | 33357 | 66336 | 25.71 | 1.61 | 76.00 | 4.00 | 629.6 | 0.327 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1430 | 8128 | 0 | 0 | 8671 | 56607 | 78.60 | 4.91 | 68.33 | 1.17 | 190.0 | 1.00 |
| 64K | 6 | A-each | 12.00 | 653.3 | 0.050 | 2991 | 55996 | 107.6 | 16604 | 66572 | 75169 | 84.62 | 5.29 | 72.00 | 1.50 | 440.8 | 0.266 |
| 64K | 6 | A-batch | 12.00 | 754.2 | 0.041 | 2935 | 62844 | 86.03 | 7201 | 83028 | 83028 | 96.18 | 6.01 | 71.33 | 1.33 | 433.4 | 0.303 |
| 64K | 6 | A+-each | 12.00 | 756.9 | 306.9 | 1834 | 50751 | 80.60 | 24223 | 82079 | 108372 | 68.56 | 4.28 | 72.33 | 1.67 | 437.7 | 0.216 |
| 64K | 6 | A+-batch | 12.00 | 725.7 | 249.1 | 1940 | 88609 | 68.37 | 7250 | 108127 | 108127 | 80.09 | 5.01 | 70.33 | 1.17 | 352.0 | 0.252 |
| 64K | 6 | A+-dironly | 12.00 | 751.5 | 319.4 | 2312 | 0.080 | 59.35 | 22960 | 25024 | 25024 | 241.6 | 15.10 | 69.33 | 0.333 | 262.6 | 0.761 |
| 64K | 6 | B-own | 12.00 | 0 | 0 | 2292 | 39235 | 0 | 0 | 41549 | 42228 | 229.9 | 14.37 | 68.33 | 0.500 | 151.2 | 0.724 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2279 | 24352 | 0 | 0 | 27154 | 27154 | 317.6 | 19.85 | 68.33 | 0.333 | 105.2 | 1.00 |
| 1M | 1 | A | 12.00 | 119.6 | 0.081 | 3087 | 30172 | 68.66 | 8262 | 41695 | 57576 | 22.17 | 22.17 | 1036 | 4.00 | 661.2 | 0.369 |
| 1M | 1 | A+ | 12.00 | 123.9 | 78.28 | 2713 | 38695 | 70.01 | 8267 | 49992 | 65752 | 21.19 | 21.19 | 1036 | 4.00 | 646.9 | 0.352 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3553 | 10661 | 0 | 0 | 14197 | 46033 | 60.12 | 60.12 | 1028 | 1.17 | 219.3 | 1.00 |
| 1M | 6 | A-each | 12.00 | 753.8 | 0.050 | 24460 | 67980 | 98.86 | 47048 | 142229 | 149518 | 45.08 | 45.08 | 1033 | 1.92 | 549.8 | 0.492 |
| 1M | 6 | A-batch | 12.00 | 758.5 | 0.050 | 10988 | 70410 | 85.85 | 7221 | 91762 | 91762 | 65.77 | 65.77 | 1031 | 1.33 | 483.6 | 0.718 |
| 1M | 6 | A+-each | 12.00 | 688.6 | 314.5 | 11615 | 82454 | 77.62 | 44281 | 135380 | 166229 | 43.70 | 43.70 | 1033 | 1.67 | 470.5 | 0.477 |
| 1M | 6 | A+-batch | 12.00 | 649.5 | 246.5 | 10570 | 54424 | 64.77 | 7218 | 75084 | 75084 | 80.46 | 80.46 | 1031 | 1.33 | 425.0 | 0.878 |
| 1M | 6 | A+-dironly | 12.00 | 621.6 | 295.8 | 10934 | 0.070 | 56.78 | 42254 | 58360 | 58360 | 103.5 | 103.5 | 1029 | 0.333 | 331.6 | 1.13 |
| 1M | 6 | B-own | 12.00 | 0 | 0 | 10045 | 43885 | 0 | 0 | 55706 | 56222 | 113.4 | 113.4 | 1028 | 0.500 | 150.4 | 1.24 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 10492 | 54466 | 0 | 0 | 70692 | 70692 | 91.64 | 91.64 | 1028 | 0.333 | 129.1 | 1.00 |
| 1M | 6 | A+-batch-replace | 12.00 | 477.5 | 236.8 | 11610 | 45367 | 112.1 | 7196 | 66810 | 66810 | 89.98 | 89.98 | 1033 | 1.50 | 449.1 | 0.982 |

### 4. Removing chunks, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| size | side | n | cold | unlink p50 µs | unlink p99 µs | removes s | dir sync ms | drain ms | µs/chunk in all | dev KiB/chunk | discard KiB/chunk | cpu µs/chunk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1M | chunks | 50.00 | yes | 64.01 | 23021 | 0.031 | 298.2 | 83.28 | 8256 | 0.640 | 0 | 98.94 |
| 1M | trees | 50.00 | yes | 61.22 | 2015 | 0.018 | 277.9 | 91.47 | 7744 | 0.560 | 0 | 126.8 |
| 1M | raw | 50.00 | yes | 19.68 | 1832 | 0.007 | 313.2 | 91.59 | 8232 | 0.720 | 0 | 36.60 |
| 4M | chunks | 50.00 | yes | 63.95 | 1324 | 0.009 | 197.4 | 199.9 | 8135 | 1.28 | 0 | 96.93 |
| 4M | trees | 50.00 | yes | 61.97 | 2048 | 0.018 | 275.5 | 91.59 | 7701 | 0.560 | 0 | 129.7 |
| 4M | raw | 50.00 | yes | 19.49 | 1864 | 0.007 | 281.3 | 91.76 | 7595 | 0.720 | 0 | 36.00 |

### 8. One device, several slices, round 1 (slices on cpus 2/3, 4/5, 6/7, 0/1, blocking thread on the sibling; checksum at 11.4 GiB/s a core)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| cell | MiB/s | ops/s | p50 µs | p99 µs | p99.9 µs | thread CPU mean | thread CPU max | runtime max | sleeps/op | core busy max | process cores | runtime + checksum | sustainable MiB/s | device write MiB/s | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fio r64 | 22.89 | 337.6 | 0 | 591397 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio r4 | 1.50 | 355.7 | 0 | 608174 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio w-seq-1M | 201.0 | 194.2 | 0 | 56885 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| r64 slices=1 | 188.4 | 3015 | 6630 | 78906 | 112833 | 0.163 | 0.163 | 0.046 | 0.988 | 0.208 | 0.164 | 0.062 | 188.4 | 0 | 0 |
| r64 slices=2 | 198.3 | 3173 | 16439 | 63670 | 90170 | 0.069 | 0.072 | 0.023 | 0.977 | 0.093 | 0.139 | 0.032 | 198.3 | 0 | 0 |
| r64-T slices=1 | 261.0 | 4176 | 7542 | 10099 | 12392 | 0.210 | 0.210 | 0.060 | 1.01 | 0.180 | 0.210 | 0.082 | 261.0 | 0 | 0 |
| r64-T slices=2 | 204.5 | 3272 | 8763 | 39234 | 58699 | 0.084 | 0.087 | 0.025 | 1.02 | 0.075 | 0.168 | 0.034 | 204.5 | 0 | 0 |
| r4 slices=1 | 28.34 | 7256 | 4370 | 4858 | 5092 | 0.128 | 0.128 | 0.043 | 0.063 | 0.115 | 0.128 | 0.045 | 28.34 | 0 | 0 |
| r4 slices=2 | 23.99 | 6141 | 9716 | 26969 | 40245 | 0.061 | 0.061 | 0.022 | 0.127 | 0.057 | 0.122 | 0.023 | 23.99 | 0 | 0 |
| r4-T slices=1 | 26.70 | 6834 | 4546 | 9413 | 16609 | 0.124 | 0.124 | 0.041 | 0.081 | 0.132 | 0.125 | 0.043 | 26.70 | 0 | 0 |
| r4-T slices=2 | 21.20 | 5426 | 5331 | 16787 | 31614 | 0.059 | 0.059 | 0.019 | 0.150 | 0.057 | 0.118 | 0.020 | 21.20 | 0.015 | 0 |
| w1M slices=1 | 0 | 0 | 0 | 0 | 0 | 0.003 | 0.003 | 0.001 | 12.00 | 0 | 0.006 | 0.001 | 0 | 15.21 | 11.25 |
| w1M slices=2 | 45.00 | 45.00 | 116837 | 116837 | 116837 | 0.012 | 0.012 | 0.005 | 7.25 | 0.037 | 0.040 | 0.007 | 45.00 | 88.24 | 63.75 |
| w1M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.008 | 0.008 | 0.003 | 31.00 | 0.037 | 0.011 | 0.003 | 0 | 86.94 | 18.75 |
| w1M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.008 | 0.008 | 0.004 | 50.00 | 0.074 | 0.024 | 0.004 | 0 | 90.75 | 41.25 |
| w4M slices=1 | 0 | 0 | 0 | 0 | 0 | 0.013 | 0.013 | 0.005 | 58.00 | 0.037 | 0.019 | 0.005 | 0 | 121.6 | 56.25 |
| w4M slices=2 | 0 | 0 | 0 | 0 | 0 | 0.004 | 0.006 | 0.002 | 46.00 | 0 | 0.013 | 0.002 | 0 | 131.6 | 11.25 |
| w4M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.010 | 0.010 | 0.004 | 75.00 | 0.037 | 0.018 | 0.004 | 0 | 148.8 | 7.50 |
| w4M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.009 | 0.010 | 0.004 | 93.00 | 0 | 0.025 | 0.004 | 0 | 300.4 | 0 |
| depth-r64 depth=1 | 57.42 | 918.8 | 451.7 | 8911 | 12968 | 0.051 | 0.051 | 0.014 | 1.00 | 0.038 | 0.052 | 0.018 | 57.42 | 0 | 0 |
| depth-r64 depth=32 | 237.0 | 3791 | 7338 | 29539 | 57534 | 0.192 | 0.192 | 0.054 | 1.02 | 0.192 | 0.192 | 0.075 | 237.0 | 0 | 0 |
| depth-r4 depth=1 | 11.71 | 2998 | 331.4 | 368.0 | 388.5 | 0.155 | 0.155 | 0.042 | 1.00 | 0.083 | 0.155 | 0.043 | 11.71 | 0 | 0 |
| depth-r4 depth=32 | 28.07 | 7187 | 4115 | 9952 | 23553 | 0.131 | 0.131 | 0.043 | 0.081 | 0.115 | 0.132 | 0.046 | 28.07 | 0 | 0 |
| w1M slices=1 sentinel | 0 | 0 | 0 | 0 | 0 | 0.007 | 0.007 | 0.003 | 22.00 | 0 | 0.011 | 0.003 | 0 | 36.67 | 33.75 |

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write back)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite | 114.0 | 8349 | 8440 | 8440 | 1.03 | 4.11 | 0.984 |
| 6 | overwrite | 216.0 | 25008 | 25124 | 25124 | 0.361 | 4.33 | 0.990 |

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | capacity | 0 | 72.00 | no | 106.9 | 109.3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | idle | 0 | 0 | no | 0 | 0 | 77.09 | 105.7 | 105.7 | 174.1 | 250.2 | 1.04 | 1.21 | 0.389 | 10.50 |
| 8 | arrival-qd1 | 36.00 | 6.00 | **yes** | 692.6 | 692.6 | 51.49 | 110.6 | 110.6 | 151.6 | 217.4 | 1.06 | 1.22 | 0.227 | 11.25 |
| 8 | offset-qd1 | 36.00 | 6.00 | **yes** | 714.2 | 714.2 | 59.92 | 82.02 | 82.02 | 158.5 | 180.7 | 1.04 | 1.30 | 0.179 | 11.25 |
| 8 | offset-ino | 36.00 | 6.00 | **yes** | 718.3 | 718.3 | 50.68 | 100.6 | 100.6 | 149.3 | 199.3 | 1.05 | 1.21 | 0.345 | 11.25 |
| 8 | kernel | 36.00 | 18.00 | **yes** | 380.6 | 386.6 | 75.76 | 263.0 | 263.0 | 320.5 | 391.0 | 1.06 | 1.31 | 0.231 | 9.75 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 292.1 | 309.3 | 309.3 | 57.75 | 110.4 | 0.364 | 17.25 |
| 10 | 9.01 | 355.1 | 458.0 | 458.0 | 51.11 | 158.9 | 0.323 | 15.00 |
| unbounded | 24.02 | 365.1 | 447.2 | 447.2 | 72.15 | 149.1 | 0.410 | 13.50 |

### X7 · One executor, an SSD's slice and a disk's, round 1 (SSD: 64 KiB reads at 1000/s and 4 KiB stages at 200/s, open loop; disk: applies in batches of 32, reads 4 deep, 1 MiB chunks renamed; executors on cpus 2 and 4)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| arrangement | SSD read p50 µs | SSD read p99 µs | SSD read p99.9 µs | SSD stage p50 µs | SSD stage p99 µs | SSD executor busy | disk executor busy | disk applies/s | disk reads/s | disk chunks/s | rename p50 µs | dir sync p50 ms | disk executor µs/op | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ssd-alone | 363.4 | 1358 | 3296 | 1132 | 4019 | 0.113 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| shared | 375.6 | 506.7 | 2689 | 1153 | 1337 | 0.134 | 0.134 | 48.00 | 30.75 | 1.50 | 113.4 | 202.6 | 0 | 0.622 |
| separate | 364.7 | 490.2 | 556.3 | 1134 | 1279 | 0.110 | 0.009 | 48.00 | 38.25 | 1.50 | 99.77 | 118.0 | 107.0 | 0.621 |

### 1. A whole chunk (chunk-recycle), round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | R | 12.00 | 72.99 | 0.090 | 512.0 | 8253 | 73.44 | 9689 | 16699 | 33252 | 47.97 | 3.00 | 72.00 | 3.00 | 510.0 | 0.435 |
| 64K | 1 | A+ | 12.00 | 119.9 | 69.14 | 514.0 | 32643 | 72.62 | 8271 | 41678 | 57682 | 25.27 | 1.58 | 76.00 | 4.00 | 618.8 | 0.229 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 1362 | 8171 | 0 | 0 | 8714 | 13070 | 110.2 | 6.89 | 68.33 | 1.17 | 192.6 | 1.00 |
| 64K | 6 | R-batch | 12.00 | 136.9 | 0.040 | 1979 | 29753 | 77.39 | 16761 | 49742 | 49742 | 160.4 | 10.02 | 68.67 | 0.667 | 274.1 | 0.468 |
| 64K | 6 | A+-batch | 12.00 | 761.3 | 314.2 | 1998 | 125251 | 69.53 | 58397 | 225112 | 225112 | 41.18 | 2.57 | 130.3 | 1.17 | 374.8 | 0.120 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 2308 | 21087 | 0 | 0 | 23928 | 23928 | 342.4 | 21.40 | 68.33 | 0.333 | 109.7 | 1.00 |
| 1M | 1 | R | 12.00 | 71.75 | 0.081 | 2603 | 19303 | 67.48 | 14972 | 33345 | 65917 | 26.21 | 26.21 | 1032 | 3.00 | 476.8 | 0.408 |
| 1M | 1 | A+ | 12.00 | 116.1 | 69.39 | 2577 | 30581 | 70.17 | 8270 | 41678 | 50024 | 24.42 | 24.42 | 1036 | 4.00 | 636.6 | 0.380 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 3482 | 10302 | 0 | 0 | 13764 | 38475 | 64.29 | 64.29 | 1028 | 1.17 | 212.1 | 1.00 |
| 1M | 6 | R-batch | 12.00 | 145.5 | 0.041 | 10962 | 36583 | 84.51 | 19454 | 66676 | 66676 | 90.63 | 90.63 | 1029 | 0.667 | 279.0 | 0.800 |
| 1M | 6 | A+-batch | 12.00 | 767.1 | 304.6 | 10422 | 54630 | 68.27 | 7224 | 75102 | 75102 | 80.34 | 80.34 | 1031 | 1.33 | 421.2 | 0.709 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 10348 | 40379 | 0 | 0 | 56322 | 56322 | 113.3 | 113.3 | 1028 | 0.333 | 128.5 | 1.00 |

