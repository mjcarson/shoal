### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs net

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.20 µs |
| fdatasync of a clean file | p50 10.76 µs, p99 16.43 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 72.37 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 74.28 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.43 µs, p99 17.18 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild across hosts, round 1 (survivors from 172.16.2.4:13400 and 172.16.2.5:13400, plain TCP; bound 112 MiB/s ÷ k)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs net

| side | rebuilt MiB/s | received MiB/s | bound MiB/s | chunk p50 ms | read p99 ms | write p99 ms | mismatches | refused |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| none | 0 | 0 | 0 | 0 | 0.085 | 0.165 | 0 | 0 |
| copy | 83.60 | 83.28 | 112.0 | 95.81 | 1.41 | 1.82 | 0 | 0 |
| 2+1 | 53.00 | 105.3 | 56.00 | 150.6 | 1.09 | 1.33 | 0 | 0 |
| 4+2 | 26.80 | 105.7 | 28.00 | 295.3 | 0.524 | 0.940 | 0 | 0 |

