### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 403.5 µs |
| fdatasync of a clean file | p50 51.22 µs, p99 55.94 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 18 KiB and 2.00 flushes a rename | p50 1610 µs |
| rename, then the directory's Fsync | 20 KiB and 2.00 flushes a rename | p50 1754 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | refused: Operation not supported (os error 95) | FIEMAP on the target: Ok(Extents { total: 1, shared: 0, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.32 µs, p99 15.65 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Sequential and random direct I/O, round 1 (population from 0.0% to 100.0% of the device, 5589 GiB apart (p5 to p95); 16 files of 64 MiB)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| cell | side | MiB/s | ops/s | p50 µs | p99 µs | disk busy | merges/op | file at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write piece=1M depth=1 | seq-write | 196.6 | 196.6 | 2027 | 15296 | 0.988 | 0.058 | 0.0% |
| read piece=1M depth=1 | seq-read | 192.3 | 192.3 | 5014 | 12951 | 0.986 | 0 | 0.0% |
| write piece=1M depth=32 | seq-write | 220.5 | 220.5 | 69845 | 154041 | 0.995 | 0.056 | 0.0% |
| read piece=1M depth=32 | seq-read | 222.5 | 222.5 | 139240 | 152007 | 0.981 | 0 | 0.0% |
| random size=4K depth=1 | random | 0.630 | 161.3 | 5872 | 19245 | 0.979 | 0 | - |
| random size=4K depth=32 | random | 2.42 | 620.6 | 16748 | 168925 | 0.988 | 0 | - |
| random size=1M depth=1 | random | 76.88 | 76.88 | 11185 | 32676 | 0.987 | 0 | - |
| random size=1M depth=32 | random | 80.63 | 80.63 | 119926 | 346302 | 0.985 | 0 | - |
| short size=64K depth=1 | short | 30.94 | 495.0 | 361.8 | 10476 | 0.972 | 0 | - |
| short size=64K depth=4 | short | 225.4 | 3606 | 507.6 | 13715 | 0.981 | 0 | - |
| fio w-seq-1M | fio | 230.3 | 223.5 | 37487 | 50070 | 0 | 0 | - |
| fio r-seq-1M | fio | 213.0 | 206.2 | 27132 | 541065 | 0 | 0 | - |
| fio r64-qd32 | fio | 24.57 | 365.6 | 12255 | 1702887 | 0 | 0 | - |

### 6. One unit at a random offset, round 1 (µs; queue depth 1; population from 53.2% to 53.2% of the device, 0 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| unit | level | n | open p50 | open p99 | read p50 | read p99 | close p50 | total p50 | total p99 | total p99.9 | dev KiB read/sample |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | cold | 5.00 | 412.1 | 12434 | 20052 | 33273 | 14.85 | 24573 | 33628 | 33628 | 16.80 |
| 4K | dentry-warm | 20.00 | 58.25 | 70.48 | 5547 | 20385 | 25.12 | 5606 | 20407 | 20407 | 8.00 |
| 4K | open | 20.00 | 0 | 0 | 167.2 | 192.7 | 0 | 167.2 | 192.7 | 192.7 | 4.00 |
| 64K | cold | 5.00 | 393.3 | 405.5 | 2398 | 30946 | 10.26 | 2763 | 31335 | 31335 | 76.00 |
| 64K | dentry-warm | 20.00 | 27.53 | 56.99 | 706.4 | 17770 | 10.34 | 729.7 | 17798 | 17798 | 68.00 |
| 64K | open | 20.00 | 0 | 0 | 256.2 | 310.8 | 0 | 256.2 | 310.8 | 310.8 | 64.00 |

### 5. Listing: the populations built, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| population | files | built in s | files/s |
| --- | --- | --- | --- |
| deep-2K | 2000 | 0.113 | 17709 |
| wide-2K | 2000 | 0.201 | 9937 |

### 5. Listing a placement group, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| population | walk | chunks | cold s | cold µs/chunk | cold dev KiB read/chunk | warm s | warm µs/chunk | warm dev KiB written | hours/16 TiB at 1 MiB | at 4 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deep-2K | names | 2000 | 0.023 | 11.68 | 0.266 | 0.001 | 0.304 | 0 | 0.054 | 0.014 |
| deep-2K | statx | 2000 | 0.013 | 6.51 | 0.328 | 0.002 | 0.995 | 0 | 0.030 | 0.008 |
| deep-2K | statx-ino | 2000 | 0.013 | 6.75 | 0.328 | 0.002 | 1.14 | 0 | 0.031 | 0.008 |
| deep-2K | xattr | 2000 | 0.016 | 8.03 | 0.328 | 0.005 | 2.47 | 0 | 0.037 | 0.009 |
| deep-2K | header-qd32 | 2000 | 0.069 | 34.44 | 4.33 | 0.066 | 33.19 | 0 | 0.161 | 0.040 |
| wide-2K | names | 2000 | 0.238 | 119.2 | 4.67 | 0.007 | 3.67 | 0 | 0.555 | 0.139 |
| wide-2K | statx | 2000 | 0.221 | 110.7 | 4.67 | 0.009 | 4.69 | 0 | 0.516 | 0.129 |
| wide-2K | statx-ino | 2000 | 0.226 | 112.8 | 4.67 | 0.009 | 4.72 | 0 | 0.526 | 0.131 |
| wide-2K | xattr | 2000 | 0.228 | 114.2 | 4.67 | 0.012 | 6.06 | 0 | 0.532 | 0.133 |
| wide-2K | header-qd32 | 2000 | 0.281 | 140.3 | 8.67 | 0.075 | 37.46 | 0 | 0.654 | 0.164 |

### 3. A partial write, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write back)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J | 632.1 | 2617 | 920.7 | 2425 | 547.2 | 2117 | 0 | 0 | 0 | 1513 | 4365 | 17.50 | 4.38 | 2.12 | 187.6 |
| 4K | 1 | J-ssd | 50.02 | 240.0 | 877.7 | 2467 | 433.7 | 2177 | 0 | 0 | 0 | 939.0 | 2516 | 9.50 | 2.38 | 1.12 | 182.7 |
| 4K | 6 | J | 1464 | 2330 | 2486 | 2930 | 823.3 | 1824 | 0 | 0 | 0 | 3846 | 4394 | 17.50 | 4.38 | 1.00 | 148.2 |
| 4K | 6 | J-ssd | 129.0 | 206.2 | 2296 | 4555 | 961.9 | 4238 | 0 | 0 | 0 | 2499 | 4705 | 9.50 | 2.38 | 0.500 | 105.3 |
| 64K | 1 | J | 556.3 | 2048 | 1020 | 2000 | 572.3 | 1462 | 0 | 0 | 0 | 1618 | 3389 | 137.5 | 2.15 | 2.12 | 138.3 |
| 64K | 1 | J-ssd | 78.61 | 163.8 | 838.0 | 2348 | 410.0 | 671.1 | 0 | 0 | 0 | 915.9 | 2426 | 69.50 | 1.09 | 1.12 | 139.0 |
| 64K | 6 | J | 16686 | 18633 | 42980 | 53095 | 41559 | 51642 | 0 | 0 | 0 | 58331 | 71715 | 137.5 | 2.15 | 0.688 | 249.0 |
| 64K | 6 | J-ssd | 321.4 | 439.5 | 38744 | 41110 | 37591 | 40594 | 0 | 0 | 0 | 39066 | 41279 | 69.50 | 1.09 | 0.500 | 181.2 |

### 2. The journal, round 1 (a 4 KiB header block on every record; write cache write back)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | allocated/coalesced | 436.9 | 436.9 | 1.00 | 1892 | 5654 | 5738 | 20.05 | 2.01 | 76.61 |
| 4K | 1 | allocated/each | 438.3 | 438.3 | 1.00 | 1876 | 5303 | 5695 | 20.04 | 2.01 | 73.79 |
| 4K | 1 | appended/coalesced | 445.1 | 445.1 | 1.00 | 1865 | 5509 | 6535 | 32.27 | 2.01 | 98.25 |
| 4K | 1 | appended/each | 446.9 | 446.9 | 1.00 | 1769 | 5239 | 5654 | 32.26 | 2.01 | 101.8 |
| 4K | 1 | ssd-written-ahead/each | 25250 | 25255 | 1.00 | 37.20 | 53.44 | 64.47 | 8.00 | 0 | 30.25 |
| 4K | 1 | written-ahead/coalesced | 1033 | 1039 | 1.00 | 817.3 | 1806 | 2187 | 8.08 | 1.01 | 50.32 |
| 4K | 1 | written-ahead/each | 1058 | 1064 | 1.00 | 782.4 | 1853 | 2174 | 8.08 | 1.01 | 48.52 |
| 4K | 6 | allocated/coalesced | 2515 | 424.8 | 6.00 | 1951 | 6194 | 6194 | 10.01 | 0.335 | 16.74 |
| 4K | 6 | allocated/each | 2528 | 426.9 | 6.00 | 1955 | 5967 | 5967 | 10.01 | 0.335 | 15.29 |
| 4K | 6 | appended/coalesced | 2485 | 419.7 | 6.00 | 1954 | 5763 | 5763 | 12.05 | 0.335 | 16.28 |
| 4K | 6 | appended/each | 831.9 | 329.5 | 2.62 | 7029 | 11946 | 13712 | 17.30 | 0.770 | 50.82 |
| 4K | 6 | ssd-written-ahead/each | 48354 | 14624 | 3.31 | 67.59 | 419.5 | 513.8 | 8.31 | 0 | 14.22 |
| 4K | 6 | written-ahead/coalesced | 5649 | 947.0 | 6.00 | 874.9 | 2144 | 3174 | 8.01 | 0.168 | 8.82 |
| 4K | 6 | written-ahead/each | 5482 | 919.2 | 6.00 | 901.5 | 1781 | 2023 | 8.01 | 0.168 | 10.35 |

### 1. A whole chunk (chunk), round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | A | 12.00 | 45.69 | 0.030 | 472.5 | 1589 | 34.01 | 1462 | 4391 | 7352 | 215.5 | 13.47 | 136.3 | 4.08 | 318.8 | 0.296 |
| 64K | 1 | A+ | 12.00 | 47.10 | 22.72 | 827.8 | 1886 | 33.34 | 1905 | 5556 | 7593 | 179.3 | 11.21 | 119.7 | 4.08 | 310.4 | 0.246 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 743.5 | 521.7 | 0 | 0 | 1222 | 2573 | 728.4 | 45.53 | 69.33 | 1.17 | 121.9 | 1.00 |
| 64K | 6 | A-each | 12.00 | 689.2 | 0.050 | 4750 | 52635 | 987.4 | 3941 | 74441 | 76758 | 87.01 | 5.44 | 97.00 | 1.58 | 450.1 | 0.142 |
| 64K | 6 | A-batch | 12.00 | 10113 | 0.030 | 33606 | 29336 | 156.2 | 2367 | 73587 | 73587 | 81.58 | 5.10 | 89.00 | 1.08 | 482.6 | 0.133 |
| 64K | 6 | A+-each | 12.00 | 485.8 | 141.9 | 1401 | 42991 | 870.0 | 4085 | 65851 | 79922 | 104.2 | 6.51 | 89.67 | 1.42 | 334.0 | 0.170 |
| 64K | 6 | A+-batch | 12.00 | 630.3 | 286.3 | 1316 | 45105 | 200.4 | 2843 | 50492 | 50492 | 134.9 | 8.43 | 82.67 | 0.917 | 324.3 | 0.221 |
| 64K | 6 | A+-dironly | 12.00 | 456.1 | 127.1 | 1208 | 0.091 | 64.06 | 41164 | 43340 | 43340 | 156.7 | 9.80 | 79.67 | 0.417 | 213.4 | 0.256 |
| 64K | 6 | B-own | 12.00 | 0 | 0 | 1867 | 8029 | 0 | 0 | 9638 | 9831 | 622.2 | 38.89 | 69.33 | 0.500 | 166.5 | 1.02 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 1830 | 8070 | 0 | 0 | 9832 | 9832 | 611.3 | 38.21 | 69.33 | 0.333 | 148.6 | 1.00 |
| 1M | 1 | A | 12.00 | 150.7 | 0.081 | 2750 | 36363 | 184.3 | 2779 | 42359 | 47503 | 23.54 | 23.54 | 1093 | 4.08 | 814.4 | 0.311 |
| 1M | 1 | A+ | 12.00 | 155.6 | 68.02 | 2834 | 30890 | 190.9 | 2883 | 36930 | 45383 | 26.78 | 26.78 | 1088 | 4.08 | 913.0 | 0.353 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 2752 | 10236 | 0 | 0 | 13037 | 15610 | 75.78 | 75.78 | 1029 | 1.17 | 440.5 | 1.00 |
| 1M | 6 | A-each | 12.00 | 689.0 | 0.050 | 15380 | 94480 | 978.6 | 45650 | 157212 | 177167 | 40.81 | 40.81 | 1055 | 1.58 | 567.2 | 0.303 |
| 1M | 6 | A-batch | 12.00 | 669.9 | 0.040 | 17141 | 84631 | 106.3 | 2338 | 108387 | 108387 | 59.68 | 59.68 | 1049 | 1.08 | 439.6 | 0.444 |
| 1M | 6 | A+-each | 12.00 | 634.1 | 215.5 | 13660 | 64481 | 408.6 | 4725 | 84067 | 99131 | 71.98 | 71.98 | 1050 | 1.42 | 372.8 | 0.535 |
| 1M | 6 | A+-batch | 12.00 | 663.6 | 224.3 | 15671 | 69014 | 74.53 | 1964 | 87643 | 87643 | 71.52 | 71.52 | 1044 | 1.08 | 350.1 | 0.532 |
| 1M | 6 | A+-dironly | 12.00 | 682.9 | 211.4 | 9883 | 0.030 | 31.41 | 74939 | 90692 | 90692 | 68.75 | 68.75 | 1040 | 0.417 | 367.5 | 0.511 |
| 1M | 6 | B-own | 12.00 | 0 | 0 | 8844 | 43078 | 0 | 0 | 59100 | 59341 | 114.1 | 114.1 | 1029 | 0.500 | 222.8 | 0.848 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 9020 | 31647 | 0 | 0 | 44660 | 44660 | 134.5 | 134.5 | 1029 | 0.333 | 147.1 | 1.00 |
| 1M | 6 | A+-batch-replace | 12.00 | 289.2 | 178.8 | 15082 | 64010 | 187.3 | 2104 | 83459 | 83459 | 74.29 | 74.29 | 1052 | 1.08 | 351.4 | 0.552 |

### 4. Removing chunks, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| size | side | n | cold | unlink p50 µs | unlink p99 µs | removes s | dir sync ms | drain ms | µs/chunk in all | dev KiB/chunk | discard KiB/chunk | cpu µs/chunk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1M | chunks | 50.00 | yes | 59.99 | 1662 | 0.006 | 1.34 | 3.34 | 217.3 | 3.20 | 0 | 92.73 |
| 1M | trees | 50.00 | yes | 49.02 | 44347 | 0.149 | 2.31 | 3.36 | 3084 | 3.60 | 0 | 116.3 |
| 1M | raw | 50.00 | yes | 13.03 | 35923 | 0.128 | 1.90 | 2.63 | 2658 | 3.36 | 0 | 50.54 |
| 4M | chunks | 50.00 | yes | 41.39 | 35929 | 0.096 | 2.55 | 3.69 | 2047 | 3.20 | 0 | 94.06 |
| 4M | trees | 50.00 | yes | 32.12 | 43946 | 0.103 | 2.91 | 2.67 | 2165 | 3.44 | 0 | 100.5 |
| 4M | raw | 50.00 | yes | 16.30 | 21874 | 0.096 | 2.57 | 3.32 | 2029 | 3.04 | 0 | 60.39 |

### 8. One device, several slices, round 1 (slices on cpus 8/24, 9/25, 10/26, 11/27, 12/28, 13/29, 14/30, 15/31, 1/17, 2/18, 3/19, 4/20, 5/21, 6/22, 7/23, 0/16, blocking thread on the sibling; checksum at 40.5 GiB/s a core)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| cell | MiB/s | ops/s | p50 µs | p99 µs | p99.9 µs | thread CPU mean | thread CPU max | runtime max | sleeps/op | core busy max | process cores | runtime + checksum | sustainable MiB/s | device write MiB/s | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fio r64 | 32.48 | 491.9 | 0 | 396362 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio r4 | 3.26 | 805.6 | 0 | 250610 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| fio w-seq-1M | 222.0 | 215.2 | 0 | 49545 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| r64 slices=1 | 223.6 | 3578 | 4380 | 99886 | 224755 | 0.081 | 0.081 | 0.022 | 1.02 | 0.040 | 0.081 | 0.028 | 223.6 | 0 | 0 |
| r64 slices=2 | 246.3 | 3941 | 12834 | 85348 | 179081 | 0.040 | 0.048 | 0.016 | 0.957 | 0.058 | 0.080 | 0.019 | 246.3 | 0 | 0 |
| r64-T slices=1 | 398.9 | 6383 | 4955 | 6567 | 6934 | 0.138 | 0.138 | 0.038 | 1.01 | 0.135 | 0.138 | 0.048 | 398.9 | 0 | 0 |
| r64-T slices=2 | 326.4 | 5222 | 5760 | 10584 | 16740 | 0.060 | 0.061 | 0.016 | 1.01 | 0.057 | 0.120 | 0.020 | 326.4 | 0 | 0 |
| r4 slices=1 | 34.50 | 8831 | 3601 | 3805 | 3869 | 0.182 | 0.182 | 0.052 | 1.01 | 0.143 | 0.182 | 0.053 | 34.50 | 0 | 0 |
| r4 slices=2 | 20.76 | 5316 | 11337 | 24350 | 43208 | 0.056 | 0.056 | 0.018 | 1.01 | 0.093 | 0.111 | 0.018 | 20.76 | 0 | 0 |
| r4-T slices=1 | 34.30 | 8781 | 3615 | 3785 | 3818 | 0.181 | 0.181 | 0.052 | 1.01 | 0.140 | 0.181 | 0.053 | 34.30 | 0 | 0 |
| r4-T slices=2 | 23.44 | 6000 | 5264 | 8298 | 13093 | 0.068 | 0.068 | 0.019 | 1.01 | 0.075 | 0.135 | 0.020 | 23.44 | 0 | 0 |
| w1M slices=1 | 67.50 | 67.50 | 76131 | 78615 | 78615 | 0.011 | 0.011 | 0.005 | 2.17 | 0 | 0.020 | 0.007 | 67.50 | 68.88 | 90.12 |
| w1M slices=2 | 22.50 | 22.50 | 125208 | 125208 | 125208 | 0.007 | 0.009 | 0.004 | 7.67 | 0 | 0.023 | 0.004 | 22.50 | 68.42 | 48.75 |
| w1M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.007 | 0.007 | 0.003 | 53.00 | 0.036 | 0.011 | 0.003 | 0 | 52.94 | 26.22 |
| w1M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.004 | 0.005 | 0.002 | 29.00 | 0 | 0.015 | 0.002 | 0 | 22.94 | 26.24 |
| w4M slices=1 | 0 | 0 | 0 | 0 | 0 | 0.005 | 0.005 | 0.002 | 17.00 | 0 | 0.008 | 0.002 | 0 | 90.47 | 22.51 |
| w4M slices=2 | 0 | 0 | 0 | 0 | 0 | 0.001 | 0.001 | 0.000 | 5.00 | 0 | 0.002 | 0.000 | 0 | 68.64 | 0 |
| w4M-T slices=1 | 0 | 0 | 0 | 0 | 0 | 0.003 | 0.003 | 0.001 | 35.00 | 0 | 0.006 | 0.001 | 0 | 250.2 | 0 |
| w4M-T slices=2 | 0 | 0 | 0 | 0 | 0 | 0.001 | 0.001 | 0.000 | 23.00 | 0 | 0.006 | 0.000 | 0 | 221.2 | 0 |
| depth-r64 depth=1 | 142.4 | 2278 | 258.3 | 6802 | 9687 | 0.056 | 0.056 | 0.015 | 1.00 | 0.038 | 0.056 | 0.019 | 142.4 | 0 | 0 |
| depth-r64 depth=32 | 371.7 | 5948 | 4586 | 9544 | 17303 | 0.129 | 0.129 | 0.036 | 1.01 | 0.118 | 0.129 | 0.044 | 371.7 | 0 | 0 |
| depth-r4 depth=1 | 34.61 | 8859 | 111.3 | 139.3 | 172.2 | 0.182 | 0.182 | 0.052 | 1.00 | 0.140 | 0.182 | 0.053 | 34.61 | 0 | 0 |
| depth-r4 depth=32 | 35.09 | 8983 | 3542 | 3713 | 3749 | 0.185 | 0.185 | 0.053 | 1.01 | 0.160 | 0.185 | 0.054 | 35.09 | 0 | 0 |
| w1M slices=1 sentinel | 45.00 | 45.00 | 82095 | 82095 | 82095 | 0.012 | 0.012 | 0.005 | 2.75 | 0 | 0.020 | 0.006 | 45.00 | 68.69 | 67.56 |

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write back)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite | 1041 | 816.7 | 1801 | 1826 | 1.00 | 4.02 | 0.975 |
| 6 | overwrite | 1458 | 2572 | 61100 | 61232 | 0.337 | 4.07 | 0.966 |

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 53.3% of the device, 2976 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | capacity | 0 | 96.00 | no | 64.25 | 66.18 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | idle | 0 | 0 | no | 0 | 0 | 14.24 | 29.07 | 29.07 | 15.31 | 30.07 | 0.784 | 0.944 | 0.297 | 20.25 |
| 8 | arrival-qd1 | 48.00 | 42.00 | **yes** | 60.35 | 65.80 | 1.28 | 44.72 | 44.72 | 2.42 | 44.88 | 0.745 | 0.891 | 0.232 | 32.26 |
| 8 | offset-qd1 | 48.00 | 42.00 | **yes** | 55.76 | 65.50 | 1.24 | 45.61 | 45.61 | 2.24 | 45.84 | 0.557 | 0.811 | 0.259 | 33.75 |
| 8 | offset-ino | 48.00 | 42.00 | **yes** | 58.65 | 64.07 | 1.40 | 43.25 | 43.25 | 2.23 | 43.51 | 0.595 | 0.909 | 0.233 | 32.25 |
| 8 | kernel | 48.00 | 42.00 | **yes** | 58.92 | 63.57 | 1.23 | 43.31 | 43.31 | 2.18 | 59.13 | 0.567 | 0.886 | 0.230 | 30.01 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 53.3% of the device, 2976 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 3.12 | 20.47 | 20.47 | 1.60 | 19.53 | 0.077 | 21.00 |
| 10 | 9.01 | 3.34 | 50.38 | 50.38 | 1.50 | 3.23 | 0.117 | 21.00 |
| unbounded | 102.1 | 63.80 | 94.34 | 94.34 | 6.04 | 50.34 | 0.899 | 21.00 |

### X7 · One executor, an SSD's slice and a disk's, round 1 (SSD: 64 KiB reads at 1000/s and 4 KiB stages at 200/s, open loop; disk: applies in batches of 32, reads 4 deep, 1 MiB chunks renamed; executors on cpus 8 and 9)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| arrangement | SSD read p50 µs | SSD read p99 µs | SSD read p99.9 µs | SSD stage p50 µs | SSD stage p99 µs | SSD executor busy | disk executor busy | disk applies/s | disk reads/s | disk chunks/s | rename p50 µs | dir sync p50 ms | disk executor µs/op | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ssd-alone | 57.32 | 72.75 | 90.34 | 72.33 | 87.57 | 0.039 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| shared | 61.93 | 10240 | 21946 | 78.57 | 8491 | 0.051 | 0.051 | 96.00 | 65.25 | 3.00 | 37.09 | 147.4 | 0 | 0.584 |
| separate | 57.61 | 74.13 | 91.72 | 73.29 | 92.19 | 0.039 | 0.010 | 96.00 | 63.75 | 3.00 | 66.33 | 198.3 | 62.32 | 0.580 |

### 1. A whole chunk (chunk-recycle), round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg europa hdd ext4 · **quick: not a measurement**

| size | writers | side | n | open | alloc | write | fdatasync | rename | dir sync | cycle p50 | cycle p99/max | chunks/s | MiB/s | dev KiB/chunk | flushes/chunk | cpu µs/chunk | A÷B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | R | 12.00 | 31.57 | 0.040 | 739.4 | 417.6 | 37.53 | 1126 | 2594 | 4198 | 352.6 | 22.04 | 86.33 | 3.08 | 269.1 | 0.498 |
| 64K | 1 | A+ | 12.00 | 45.74 | 22.46 | 707.5 | 1599 | 34.65 | 2558 | 5396 | 6668 | 191.4 | 11.96 | 121.7 | 4.08 | 292.4 | 0.270 |
| 64K | 1 | B-own | 12.00 | 0 | 0 | 767.2 | 526.5 | 0 | 0 | 1293 | 2241 | 708.0 | 44.25 | 69.33 | 1.17 | 143.6 | 1.00 |
| 64K | 6 | R-batch | 12.00 | 149.4 | 0.040 | 1481 | 35145 | 188.4 | 1232 | 38628 | 38628 | 158.1 | 9.88 | 71.67 | 0.750 | 241.9 | 0.262 |
| 64K | 6 | A+-batch | 12.00 | 506.8 | 134.9 | 1700 | 47534 | 49.93 | 1624 | 53242 | 53242 | 123.0 | 7.69 | 83.33 | 1.08 | 264.3 | 0.204 |
| 64K | 6 | B-batch | 12.00 | 0 | 0 | 1286 | 8752 | 0 | 0 | 10061 | 10061 | 604.4 | 37.77 | 69.33 | 0.333 | 95.53 | 1.00 |
| 1M | 1 | R | 12.00 | 92.49 | 0.071 | 2974 | 24229 | 184.3 | 2500 | 30217 | 34998 | 34.38 | 34.38 | 1047 | 3.08 | 861.2 | 0.499 |
| 1M | 1 | A+ | 12.00 | 106.1 | 56.95 | 2693 | 26255 | 171.5 | 2498 | 31920 | 40203 | 31.54 | 31.54 | 1078 | 4.08 | 817.4 | 0.458 |
| 1M | 1 | B-own | 12.00 | 0 | 0 | 2666 | 11419 | 0 | 0 | 14076 | 21062 | 68.84 | 68.84 | 1029 | 1.17 | 454.8 | 1.00 |
| 1M | 6 | R-batch | 12.00 | 151.6 | 0.050 | 10821 | 76830 | 159.9 | 1911 | 94232 | 94232 | 68.15 | 68.15 | 1033 | 0.750 | 311.9 | 0.634 |
| 1M | 6 | A+-batch | 12.00 | 694.4 | 201.2 | 14767 | 79484 | 82.33 | 1644 | 97097 | 97097 | 65.51 | 65.51 | 1042 | 1.08 | 362.7 | 0.610 |
| 1M | 6 | B-batch | 12.00 | 0 | 0 | 9083 | 48410 | 0 | 0 | 61425 | 61425 | 107.4 | 107.4 | 1029 | 0.333 | 236.3 | 1.00 |

