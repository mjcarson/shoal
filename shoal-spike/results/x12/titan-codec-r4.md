### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 101 · fs device 0 | 4.00 KiB a sync · sync p50 871.4 µs |
| fdatasync of a clean file | p50 112.2 µs, p99 125.4 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2132 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2142 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.13 µs, p99 33.35 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.57 | 337.6 | - | - | - |
| fold | 13.91 | 280.9 | - | - | - |
| crc+fold | 7.68 | 508.7 | - | - | - |
| fold-4k | 14.71 | 265.5 | - | - | - |
| crc+fold-4k | 7.99 | 489.1 | - | - | - |
| copy | 6.45 | 605.8 | - | - | - |
| decode-21-data | 5.02 | 778.8 | - | - | - |
| decode-21-parity | 4.97 | 786.2 | - | - | - |
| decode-42-data | 3.00 | 1303 | - | - | - |
| decode-42-parity | 2.97 | 1314 | - | - | - |
| pipeline-copy | 3.15 | 1242 | - | - | - |
| pipeline-21 | 2.22 | 1761 | - | - | - |
| pipeline-42 | 1.29 | 3020 | - | - | - |
| summary-42 | 816.7 | 28.70 | - | - | - |
| summary-42-4k | 14492 | 1.62 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

