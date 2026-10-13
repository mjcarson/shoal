### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs net

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.32 µs |
| fdatasync of a clean file | p50 10.92 µs, p99 16.69 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 73.77 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 75.06 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.50 µs, p99 16.82 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild across hosts, round 2 (survivors from 172.16.2.4:13400 and 172.16.2.5:13400, plain TCP; bound 112 MiB/s ÷ k)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs net

| side | rebuilt MiB/s | received MiB/s | bound MiB/s | chunk p50 ms | read p99 ms | write p99 ms | mismatches | refused |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4+2 | 26.40 | 104.1 | 28.00 | 301.5 | 0.524 | 1.27 | 0 | 0 |
| 2+1 | 51.80 | 102.9 | 56.00 | 152.3 | 1.10 | 1.43 | 0 | 0 |
| copy | 84.20 | 83.88 | 112.0 | 94.29 | 1.41 | 1.62 | 0 | 0 |
| none | 0 | 0 | 0 | 0 | 0.084 | 0.163 | 0 | 0 |

