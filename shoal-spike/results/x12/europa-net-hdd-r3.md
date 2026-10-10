### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs net

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1100 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 53.81 µs |
| fdatasync of a clean file | p50 10.29 µs, p99 24.08 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7752 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7684 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.18 µs, p99 20.47 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild across hosts, round 3 (survivors from 172.16.2.4:13400 and 172.16.2.5:13400, plain TCP; bound 112 MiB/s ÷ k)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs net

| side | rebuilt MiB/s | received MiB/s | bound MiB/s | chunk p50 ms | read p99 ms | write p99 ms | mismatches | refused |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| none | 0 | 0 | 0 | 0 | 48.90 | 48.23 | 0 | 0 |
| copy | 45.00 | 44.64 | 112.0 | 175.2 | 148.0 | 145.9 | 0 | 0 |
| 2+1 | 38.80 | 77.68 | 56.00 | 199.8 | 115.7 | 138.4 | 0 | 0 |
| 4+2 | 21.80 | 85.68 | 28.00 | 350.1 | 111.5 | 133.0 | 0 | 0 |

