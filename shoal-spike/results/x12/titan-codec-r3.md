### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 852.1 µs |
| fdatasync of a clean file | p50 112.2 µs, p99 145.7 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2201 µs |
| rename, then the directory's Fsync | 3 KiB and 1.05 flushes a rename | p50 2197 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 30.15 µs, p99 155.2 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 3 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.22 | 348.2 | - | - | - |
| fold | 13.90 | 281.0 | - | - | - |
| crc+fold | 7.69 | 508.2 | - | - | - |
| fold-4k | 14.70 | 265.8 | - | - | - |
| crc+fold-4k | 8.00 | 488.5 | - | - | - |
| copy | 6.51 | 600.0 | - | - | - |
| decode-21-data | 5.04 | 774.6 | - | - | - |
| decode-21-parity | 5.02 | 777.8 | - | - | - |
| decode-42-data | 2.96 | 1320 | - | - | - |
| decode-42-parity | 2.98 | 1309 | - | - | - |
| pipeline-copy | 3.27 | 1193 | - | - | - |
| pipeline-21 | 2.22 | 1763 | - | - | - |
| pipeline-42 | 1.28 | 3042 | - | - | - |
| summary-42 | 816.9 | 28.69 | - | - | - |
| summary-42-4k | 14482 | 1.62 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

