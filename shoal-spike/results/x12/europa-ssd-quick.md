### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.27 µs |
| fdatasync of a clean file | p50 10.82 µs, p99 16.55 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 74.50 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 74.94 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.36 µs, p99 16.06 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs · **quick: not a measurement**

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 46.11 | 84.72 | - | - | - |
| fold | 23.78 | 164.3 | - | - | - |
| crc+fold | 29.15 | 134.0 | - | - | - |
| copy | 28.30 | 138.0 | - | - | - |
| decode-21-data | 17.80 | 219.4 | - | - | - |
| decode-21-parity | 17.65 | 221.3 | - | - | - |
| decode-42-data | 9.27 | 421.4 | - | - | - |
| decode-42-parity | 9.36 | 417.5 | - | - | - |
| pipeline-copy | 21.73 | 179.8 | - | - | - |
| pipeline-21 | 8.65 | 451.7 | - | - | - |
| pipeline-42 | 4.57 | 854.8 | - | - | - |
| summary-42 | 5192 | 4.51 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs · **quick: not a measurement**

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 181.5 | 5.38 | 5.79 | 4159 | 8309 | 0 | 0 |
| layout=2+1 | unit | 5529 | 0.177 | 0.216 | 68.04 | 128.1 | 0 | 0 |
| layout=4+2 | chunk | 111.0 | 8.96 | 9.28 | 4157 | 16471 | 0 | 0 |
| layout=4+2 | unit | 4139 | 0.238 | 0.280 | 68.63 | 256.1 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 3.1% to 94.0% of the device, 237 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs · **quick: not a measurement**

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 0.087 | 0.169 | 0.027 | 0 | 0 | 0.051 |
| layout=4+2 piece=1M | fixed-50 | 51.05 | 1.02 | 3.00 | 0/0 | 0 | 0.715 | 0.993 | 0.026 | 28.20 | 13.43 | 0.064 |
| layout=4+2 piece=1M | idle | 2119 | 0 | 135.0 | 33/33 | 0 | 0.541 | 1.09 | 0.198 | 29.01 | 12.91 | 0.834 |
| layout=4+2 piece=1M | unbounded | 2249 | 0 | 143.0 | 35/35 | 0 | 1.69 | 2.89 | 0.202 | 28.69 | 12.97 | 0.854 |
| layout=4+2 piece=256K | idle | 1882 | 0 | 120.0 | 30/30 | 0 | 0.512 | 1.00 | 0.202 | 28.94 | 12.93 | 0.810 |
| layout=4+2 piece=1M | cpu | 22410 | 0 | 1245 | 358/358 | 0 | 0.561 | 1.37 | 0.225 | 26.93 | 13.46 | 0.042 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 3.1% to 94.0% of the device, 237 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs · **quick: not a measurement**

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 0.084 | 0.165 | 0.096 | 0.025 | 0 | 0.047 |
| role=dest layout=4+2 piece=1M | fixed-100 | 99.00 | 99.10 | 0.991 | 40.04 | 0.865 | 1.18 | 1.07 | 0.161 | 0 | 0.074 |
| role=dest layout=4+2 piece=1M | idle | 1298 | 1300 | 0 | 3.06 | 0.554 | 1.12 | 1.07 | 0.217 | 0 | 0.605 |
| role=dest layout=4+2 piece=1M | unbounded | 1800 | 1802 | 0 | 7.46 | 5.94 | 7.85 | 6.52 | 0.334 | 0 | 0.724 |
| role=local layout=copy piece=1M | unbounded | 1177 | 2358 | 0 | 13.51 | 6.37 | 10.86 | 6.41 | 0.408 | 0 | 0.890 |
| role=local layout=2+1 piece=1M | unbounded | 789.0 | 2381 | 0 | 20.17 | 4.91 | 8.34 | 4.98 | 0.559 | 0 | 0.890 |
| role=local layout=4+2 piece=1M | unbounded | 489.0 | 2444 | 0 | 32.71 | 5.84 | 10.93 | 7.27 | 1.82 | 0 | 0.903 |

