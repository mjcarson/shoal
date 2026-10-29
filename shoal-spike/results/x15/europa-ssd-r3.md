### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.13 µs |
| fdatasync of a clean file | p50 10.74 µs, p99 16.40 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 71.88 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 74.53 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.23 µs, p99 16.55 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 3 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 44.10 | 0.050 | 368.0 | 21.68 | 29.44 | 428.7 | 1005 | 5405 | 499.3 | 4188 | 65.44 | 0 | 194.0 | 13.22 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 39.93 | 0.050 | 68.80 | 20.19 | 31.05 | 124.9 | 301.3 | 658.6 | 3121 | 147.3 | 2.30 | 0 | 8.00 | 2.06 | 6.25 |
| 64K | 1 | class-fresh | 600.0 | 42.48 | 29.09 | 68.44 | 48.53 | 26.94 | 59.41 | 275.2 | 325.5 | 3498 | 78.30 | 1.22 | 0 | 0 | 0.000 | 7.01 |
| 64K | 8 | today | 600.0 | 14052 | 0.040 | 368.4 | 53.06 | 43.91 | 1058 | 15701 | 20290 | 561.4 | 4185 | 65.39 | 0 | 504.0 | 15.28 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 876.6 | 0.040 | 130.7 | 90.21 | 60.57 | 200.4 | 1376 | 1776 | 6106 | 143.7 | 2.25 | 0 | 467.0 | 2.04 | 10.88 |
| 64K | 8 | class-fresh | 600.0 | 124.3 | 96.48 | 239.9 | 109.0 | 51.08 | 169.5 | 884.7 | 1796 | 8324 | 71.87 | 1.12 | 0 | 0 | 0.000 | 14.83 |
| 256K | 1 | today | 512.0 | 43.47 | 0.040 | 454.4 | 19.79 | 29.89 | 439.8 | 1086 | 5330 | 478.9 | 4380 | 17.11 | 0 | 161.0 | 15.28 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 32.41 | 0.050 | 194.4 | 16.63 | 35.57 | 144.2 | 435.6 | 725.3 | 2200 | 533.0 | 2.08 | 0 | 4.00 | 3.05 | 4.59 |
| 256K | 1 | class-fresh | 512.0 | 42.65 | 28.77 | 144.8 | 50.89 | 27.13 | 54.20 | 349.7 | 397.7 | 2780 | 270.2 | 1.06 | 0 | 0 | 0.000 | 5.80 |
| 256K | 8 | today | 512.0 | 13969 | 0.040 | 547.8 | 33.97 | 31.01 | 1475 | 15991 | 20446 | 553.1 | 4378 | 17.10 | 0 | 426.0 | 12.35 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 1384 | 0.040 | 322.9 | 26.67 | 29.20 | 563.8 | 2375 | 3041 | 3629 | 530.6 | 2.07 | 0 | 408.0 | 3.05 | 6.56 |
| 256K | 8 | class-fresh | 512.0 | 121.6 | 100.7 | 499.3 | 419.9 | 43.67 | 192.7 | 1430 | 2043 | 5375 | 265.9 | 1.04 | 0 | 0 | 0.000 | 9.72 |
| 1M | 1 | today | 128.0 | 41.40 | 0.050 | 747.3 | 19.89 | 30.18 | 427.7 | 1344 | 5754 | 440.4 | 5144 | 5.02 | 0 | 38.00 | 13.24 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 41.68 | 0.050 | 761.0 | 19.23 | 33.55 | 119.7 | 984.6 | 2190 | 971.0 | 2067 | 2.02 | 0 | 9.00 | 5.11 | 2.20 |
| 1M | 1 | class-fresh | 128.0 | 42.38 | 24.98 | 439.9 | 51.00 | 27.84 | 54.29 | 640.8 | 690.6 | 1522 | 1037 | 1.01 | 0 | 0 | 0.000 | 3.46 |
| 1M | 8 | today | 128.0 | 14857 | 0.040 | 1328 | 21.69 | 29.42 | 2858 | 18791 | 25604 | 475.1 | 5146 | 5.03 | 0 | 103.0 | 12.33 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 4443 | 0.031 | 1511 | 19.49 | 27.17 | 1609 | 7597 | 10273 | 1151 | 2066 | 2.02 | 0 | 100.0 | 5.11 | 2.42 |
| 1M | 8 | class-fresh | 128.0 | 134.1 | 109.6 | 1724 | 1390 | 54.56 | 102.8 | 3784 | 5562 | 2066 | 1035 | 1.01 | 0 | 0 | 0.000 | 4.35 |
| 4M | 1 | today | 32.00 | 36.10 | 0.040 | 1903 | 19.77 | 31.09 | 432.5 | 2508 | 6353 | 315.4 | 8218 | 2.01 | 0 | 7.00 | 14.27 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 38.27 | 0.050 | 2093 | 19.81 | 31.85 | 455.8 | 2724 | 7120 | 283.8 | 8216 | 2.01 | 0 | 8.00 | 14.22 | 0.900 |
| 4M | 1 | class-fresh | 32.00 | 46.30 | 25.36 | 1730 | 60.04 | 29.73 | 66.63 | 1962 | 2455 | 502.0 | 4108 | 1.00 | 0 | 0 | 0.000 | 1.59 |
| 4M | 8 | today | 32.00 | 15214 | 0.030 | 5469 | 21.37 | 28.48 | 6092 | 26094 | 31631 | 332.8 | 8212 | 2.00 | 0 | 22.00 | 13.23 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 15286 | 0.030 | 4988 | 21.36 | 28.71 | 4514 | 20919 | 31074 | 335.7 | 8211 | 2.00 | 0 | 21.00 | 13.23 | 1.01 |
| 4M | 8 | class-fresh | 32.00 | 219.2 | 180.7 | 8835 | 6903 | 59.73 | 134.5 | 14933 | 15672 | 532.8 | 4107 | 1.00 | 0 | 0 | 0.000 | 1.60 |

