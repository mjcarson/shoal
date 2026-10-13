### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 105 · fs device 0 | 5.48 KiB a sync · sync p50 8059 µs |
| fdatasync of a clean file | p50 496.6 µs, p99 525.2 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8231 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8252 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.20 µs, p99 33.26 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### 5. Listing: the populations built, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| population | files | built in s | files/s |
| --- | --- | --- | --- |
| deep-1M | 1000000 | 325.6 | 3071 |
| wide-1M | 1000000 | 226.1 | 4423 |

### 5. Listing a placement group, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| population | walk | chunks | cold s | cold µs/chunk | cold dev KiB read/chunk | warm s | warm µs/chunk | warm dev KiB written | hours/16 TiB at 1 MiB | at 4 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deep-1M | names | 1000000 | 42.15 | 42.15 | 0.313 | 0.471 | 0.471 | 0 | 0.196 | 0.049 |
| deep-1M | statx-ino | 1000000 | 55.71 | 55.71 | 0.571 | 2.63 | 2.63 | 0 | 0.260 | 0.065 |
| deep-1M | xattr | 1000000 | 57.84 | 57.84 | 0.571 | 5.38 | 5.38 | 0 | 0.270 | 0.067 |
| deep-1M | header-qd32 | 1000000 | 217.0 | 217.0 | 4.57 | 141.9 | 141.9 | 0 | 1.01 | 0.253 |
| wide-1M | names | 1000000 | 86.59 | 86.59 | 1.04 | 5.19 | 5.19 | 0 | 0.404 | 0.101 |
| wide-1M | statx-ino | 1000000 | 94.23 | 94.23 | 1.04 | 7.67 | 7.67 | 0 | 0.439 | 0.110 |
| wide-1M | header-qd32 | 1000000 | 229.7 | 229.7 | 5.04 | 128.1 | 128.1 | 0 | 1.07 | 0.268 |

