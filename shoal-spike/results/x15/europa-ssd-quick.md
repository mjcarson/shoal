### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.38 µs |
| fdatasync of a clean file | p50 10.95 µs, p99 16.71 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 74.15 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 75.85 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.45 µs, p99 16.51 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs · **quick: not a measurement**

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 12.00 | 36.10 | 0.030 | 350.5 | 28.05 | 38.77 | 403.3 | 873.5 | 2079 | 1144 | 4175 | 65.24 | 0 | 0 | 16.28 | 1.00 |
| 64K | 1 | class-pool | 12.00 | 41.98 | 0.040 | 63.96 | 21.32 | 31.10 | 150.6 | 323.7 | 396.3 | 3026 | 142.5 | 2.23 | 0 | 0 | 1.01 | 2.65 |
| 64K | 1 | class-fresh | 12.00 | 42.84 | 25.41 | 61.41 | 46.53 | 26.95 | 60.08 | 265.5 | 654.4 | 3242 | 73.00 | 1.14 | 0 | 0 | 0.000 | 2.84 |
| 64K | 8 | today | 12.00 | 100.4 | 0.030 | 202.8 | 52.48 | 51.01 | 423.2 | 1527 | 10976 | 1032 | 4174 | 65.22 | 0 | 4.00 | 14.26 | 1.00 |
| 64K | 8 | class-pool | 12.00 | 91.85 | 0.030 | 148.9 | 59.63 | 45.60 | 227.7 | 920.6 | 1151 | 6673 | 141.0 | 2.20 | 0 | 4.00 | 2.04 | 6.46 |
| 64K | 8 | class-fresh | 12.00 | 364.9 | 198.8 | 249.2 | 130.6 | 48.30 | 106.5 | 1120 | 1143 | 6981 | 70.71 | 1.10 | 0 | 0 | 0.000 | 6.76 |
| 1M | 1 | today | 12.00 | 39.46 | 0.040 | 714.7 | 21.03 | 36.93 | 440.6 | 1204 | 4492 | 628.5 | 5136 | 5.02 | 0 | 1.00 | 22.97 | 1.00 |
| 1M | 1 | class-pool | 12.00 | 46.49 | 0.040 | 759.5 | 27.15 | 46.39 | 303.6 | 1162 | 5509 | 647.1 | 2064 | 2.02 | 0 | 0 | 6.52 | 1.03 |
| 1M | 1 | class-fresh | 12.00 | 76.57 | 42.78 | 492.3 | 294.2 | 62.70 | 108.4 | 1246 | 11720 | 329.5 | 1033 | 1.01 | 0 | 0 | 0.000 | 0.524 |
| 1M | 8 | today | 12.00 | 167.6 | 0.050 | 2367 | 61.44 | 109.6 | 1764 | 4711 | 17139 | 552.2 | 5134 | 5.01 | 0 | 4.00 | 29.75 | 1.00 |
| 1M | 8 | class-pool | 12.00 | 134.3 | 0.030 | 2076 | 34.30 | 38.17 | 1657 | 3617 | 6139 | 1352 | 2062 | 2.01 | 0 | 4.00 | 15.88 | 2.45 |
| 1M | 8 | class-fresh | 12.00 | 426.5 | 189.7 | 1785 | 1391 | 64.90 | 101.9 | 4417 | 4443 | 1790 | 1032 | 1.01 | 0 | 0 | 0.000 | 3.24 |

