### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.32 µs |
| fdatasync of a clean file | p50 10.81 µs, p99 16.74 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 73.42 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 75.16 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.45 µs, p99 17.21 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 49.20 | 79.39 | - | - | - |
| fold | 23.47 | 166.4 | - | - | - |
| crc+fold | 28.93 | 135.0 | - | - | - |
| fold-4k | 47.62 | 82.04 | - | - | - |
| crc+fold-4k | 30.86 | 126.6 | - | - | - |
| copy | 28.27 | 138.2 | - | - | - |
| decode-21-data | 17.45 | 223.9 | - | - | - |
| decode-21-parity | 17.40 | 224.5 | - | - | - |
| decode-42-data | 9.16 | 426.7 | - | - | - |
| decode-42-parity | 9.21 | 423.9 | - | - | - |
| pipeline-copy | 21.25 | 183.8 | - | - | - |
| pipeline-21 | 8.77 | 445.6 | - | - | - |
| pipeline-42 | 4.61 | 846.5 | - | - | - |
| summary-42 | 5049 | 4.64 | - | - | - |
| summary-42-4k | 71019 | 0.330 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

