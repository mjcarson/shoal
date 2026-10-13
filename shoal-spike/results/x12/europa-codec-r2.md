### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa codec

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.21 µs |
| fdatasync of a clean file | p50 10.75 µs, p99 16.59 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 73.53 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 75.20 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.39 µs, p99 16.62 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa codec

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 50.51 | 77.34 | - | - | - |
| fold | 23.63 | 165.3 | - | - | - |
| crc+fold | 29.66 | 131.7 | - | - | - |
| fold-4k | 48.47 | 80.59 | - | - | - |
| crc+fold-4k | 30.93 | 126.3 | - | - | - |
| copy | 28.32 | 138.0 | - | - | - |
| decode-21-data | 17.60 | 221.9 | - | - | - |
| decode-21-parity | 17.60 | 221.9 | - | - | - |
| decode-42-data | 9.25 | 422.2 | - | - | - |
| decode-42-parity | 9.32 | 419.1 | - | - | - |
| pipeline-copy | 21.64 | 180.5 | - | - | - |
| pipeline-21 | 8.81 | 443.5 | - | - | - |
| pipeline-42 | 4.53 | 863.1 | - | - | - |
| summary-42 | 5160 | 4.54 | - | - | - |
| summary-42-4k | 72241 | 0.324 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

