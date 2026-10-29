### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.53 KiB a sync · sync p50 965.0 µs |
| fdatasync of a clean file | p50 112.2 µs, p99 121.4 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2067 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2093 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.98 µs, p99 32.86 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 2 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 132.5 | 0.080 | 357.1 | 2228 | 70.92 | 5870 | 8938 | 31644 | 81.95 | 4184 | 65.38 | 4.18 | 120.0 | 81.24 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 81.81 | 0.080 | 192.7 | 2810 | 73.71 | 6087 | 7675 | 14784 | 119.3 | 150.2 | 2.35 | 4.39 | 0 | 0.000 | 1.46 |
| 64K | 1 | class-fresh | 600.0 | 110.4 | 83.88 | 208.2 | 2936 | 63.03 | 2043 | 5453 | 10988 | 177.7 | 77.94 | 1.22 | 2.00 | 0 | 0.000 | 2.17 |
| 64K | 8 | today | 600.0 | 41186 | 0.090 | 1854 | 6841 | 86.01 | 18188 | 69931 | 93710 | 125.7 | 4186 | 65.40 | 2.20 | 475.0 | 67.07 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 12650 | 0.091 | 399.9 | 3176 | 96.79 | 9132 | 25769 | 37874 | 327.2 | 147.2 | 2.30 | 1.99 | 456.0 | 33.28 | 2.60 |
| 64K | 8 | class-fresh | 600.0 | 245.1 | 198.0 | 427.9 | 5521 | 88.95 | 7324 | 14709 | 26636 | 542.2 | 73.80 | 1.15 | 0.997 | 0 | 0.000 | 4.31 |
| 256K | 1 | today | 512.0 | 206.0 | 0.090 | 1419 | 2964 | 73.98 | 6175 | 11175 | 31078 | 78.85 | 4385 | 17.13 | 4.41 | 71.00 | 84.56 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 79.64 | 0.080 | 393.9 | 3077 | 91.13 | 7404 | 10866 | 14717 | 105.0 | 534.8 | 2.09 | 4.48 | 0 | 3.12 | 1.33 |
| 256K | 1 | class-fresh | 512.0 | 110.9 | 84.12 | 433.3 | 3175 | 63.66 | 2061 | 5932 | 9020 | 164.0 | 271.5 | 1.06 | 2.00 | 0 | 0.000 | 2.08 |
| 256K | 8 | today | 512.0 | 41972 | 0.090 | 2204 | 7561 | 83.10 | 17612 | 71897 | 90933 | 123.2 | 4377 | 17.10 | 2.25 | 403.0 | 75.69 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12744 | 0.100 | 779.8 | 3289 | 84.24 | 8940 | 25742 | 36927 | 323.3 | 532.4 | 2.08 | 2.00 | 390.0 | 36.62 | 2.62 |
| 256K | 8 | class-fresh | 512.0 | 256.6 | 186.0 | 1109 | 5614 | 85.31 | 6262 | 13624 | 21202 | 554.8 | 267.2 | 1.04 | 0.984 | 0 | 0.000 | 4.50 |
| 1M | 1 | today | 128.0 | 197.1 | 0.090 | 2430 | 2464 | 72.96 | 6210 | 12370 | 29703 | 75.96 | 5146 | 5.02 | 4.45 | 14.00 | 77.10 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 60.71 | 0.080 | 1361 | 2875 | 74.23 | 6237 | 8895 | 14501 | 102.2 | 2073 | 2.02 | 4.48 | 0 | 12.47 | 1.35 |
| 1M | 1 | class-fresh | 128.0 | 111.3 | 84.28 | 1386 | 2941 | 63.97 | 2077 | 6678 | 9407 | 146.8 | 1041 | 1.02 | 2.00 | 0 | 0.000 | 1.93 |
| 1M | 8 | today | 128.0 | 45348 | 0.090 | 3767 | 9673 | 86.64 | 16403 | 77735 | 99033 | 115.4 | 5144 | 5.02 | 2.19 | 98.00 | 67.06 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 21210 | 0.080 | 3214 | 9201 | 114.5 | 7013 | 42236 | 51275 | 208.0 | 2067 | 2.02 | 1.88 | 95.00 | 34.41 | 1.80 |
| 1M | 8 | class-fresh | 128.0 | 335.5 | 205.8 | 4954 | 8963 | 106.4 | 6046 | 20899 | 32016 | 374.0 | 1035 | 1.01 | 0.922 | 0 | 0.000 | 3.24 |
| 4M | 1 | today | 32.00 | 126.0 | 0.090 | 6163 | 2974 | 72.94 | 7105 | 15847 | 22409 | 64.66 | 8218 | 2.01 | 4.62 | 0 | 76.85 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 84.35 | 0.090 | 6183 | 3051 | 72.47 | 6951 | 15568 | 29272 | 64.14 | 8216 | 2.01 | 4.50 | 1.00 | 62.60 | 0.992 |
| 4M | 1 | class-fresh | 32.00 | 111.8 | 81.09 | 5280 | 2912 | 64.67 | 2069 | 10546 | 12582 | 92.72 | 4110 | 1.00 | 2.00 | 0 | 0.000 | 1.43 |
| 4M | 8 | today | 32.00 | 55441 | 0.080 | 15309 | 18647 | 128.5 | 9139 | 93006 | 140946 | 85.74 | 8212 | 2.00 | 2.00 | 21.00 | 57.52 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 47721 | 0.070 | 14509 | 16358 | 103.5 | 12928 | 77345 | 153135 | 85.83 | 8214 | 2.01 | 2.12 | 21.00 | 62.88 | 1.00 |
| 4M | 8 | class-fresh | 32.00 | 669.6 | 210.7 | 24835 | 23484 | 96.84 | 6013 | 50939 | 53628 | 156.1 | 4107 | 1.00 | 0.906 | 0 | 0.000 | 1.82 |

