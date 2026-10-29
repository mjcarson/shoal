### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.33 µs |
| fdatasync of a clean file | p50 10.86 µs, p99 16.63 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 74.28 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 75.30 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.40 µs, p99 16.34 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 51.23 | 76.24 | - | - | - |
| fold | 23.50 | 166.2 | - | - | - |
| crc+fold | 29.56 | 132.1 | - | - | - |
| fold-4k | 48.55 | 80.46 | - | - | - |
| crc+fold-4k | 31.02 | 125.9 | - | - | - |
| copy | 28.33 | 137.9 | - | - | - |
| decode-21-data | 17.48 | 223.5 | - | - | - |
| decode-21-parity | 17.60 | 221.9 | - | - | - |
| decode-42-data | 9.27 | 421.5 | - | - | - |
| decode-42-parity | 9.28 | 420.9 | - | - | - |
| pipeline-copy | 21.33 | 183.2 | - | - | - |
| pipeline-21 | 8.75 | 446.3 | - | - | - |
| pipeline-42 | 4.55 | 858.0 | - | - | - |
| summary-42 | 5240 | 4.47 | - | - | - |
| summary-42-4k | 71317 | 0.329 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

