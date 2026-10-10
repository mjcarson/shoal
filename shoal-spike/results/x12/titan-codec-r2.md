### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 862.9 µs |
| fdatasync of a clean file | p50 112.7 µs, p99 128.0 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2067 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2086 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.05 µs, p99 33.24 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.42 | 342.2 | - | - | - |
| fold | 13.87 | 281.5 | - | - | - |
| crc+fold | 7.69 | 508.1 | - | - | - |
| fold-4k | 14.70 | 265.8 | - | - | - |
| crc+fold-4k | 8.00 | 488.5 | - | - | - |
| copy | 6.70 | 583.2 | - | - | - |
| decode-21-data | 5.08 | 768.7 | - | - | - |
| decode-21-parity | 5.04 | 774.6 | - | - | - |
| decode-42-data | 3.01 | 1297 | - | - | - |
| decode-42-parity | 3.04 | 1284 | - | - | - |
| pipeline-copy | 3.40 | 1148 | - | - | - |
| pipeline-21 | 2.22 | 1763 | - | - | - |
| pipeline-42 | 1.29 | 3017 | - | - | - |
| summary-42 | 818.2 | 28.65 | - | - | - |
| summary-42-4k | 14478 | 1.62 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

