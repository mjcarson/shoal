### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 893.7 µs |
| fdatasync of a clean file | p50 113.6 µs, p99 130.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2092 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2092 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.01 µs, p99 33.15 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.55 | 338.3 | - | - | - |
| fold | 14.09 | 277.2 | - | - | - |
| crc+fold | 7.74 | 505.0 | - | - | - |
| fold-4k | 14.71 | 265.5 | - | - | - |
| crc+fold-4k | 8.01 | 487.7 | - | - | - |
| copy | 6.80 | 574.0 | - | - | - |
| decode-21-data | 5.13 | 761.6 | - | - | - |
| decode-21-parity | 5.06 | 771.9 | - | - | - |
| decode-42-data | 3.03 | 1290 | - | - | - |
| decode-42-parity | 3.02 | 1295 | - | - | - |
| pipeline-copy | 3.38 | 1157 | - | - | - |
| pipeline-21 | 2.23 | 1750 | - | - | - |
| pipeline-42 | 1.30 | 3013 | - | - | - |
| summary-42 | 829.7 | 28.25 | - | - | - |
| summary-42-4k | 14510 | 1.62 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

