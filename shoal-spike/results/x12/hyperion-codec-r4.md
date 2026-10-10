### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 850.7 µs |
| fdatasync of a clean file | p50 112.3 µs, p99 138.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2117 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2126 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.45 µs, p99 34.19 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.56 | 337.9 | - | - | - |
| fold | 14.12 | 276.7 | - | - | - |
| crc+fold | 7.75 | 504.3 | - | - | - |
| fold-4k | 14.72 | 265.3 | - | - | - |
| crc+fold-4k | 8.00 | 488.2 | - | - | - |
| copy | 6.48 | 603.1 | - | - | - |
| decode-21-data | 5.01 | 780.0 | - | - | - |
| decode-21-parity | 5.03 | 776.5 | - | - | - |
| decode-42-data | 3.01 | 1299 | - | - | - |
| decode-42-parity | 2.99 | 1308 | - | - | - |
| pipeline-copy | 3.18 | 1228 | - | - | - |
| pipeline-21 | 2.21 | 1765 | - | - | - |
| pipeline-42 | 1.29 | 3018 | - | - | - |
| summary-42 | 829.1 | 28.27 | - | - | - |
| summary-42-4k | 14467 | 1.62 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

