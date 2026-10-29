### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8090 µs |
| fdatasync of a clean file | p50 491.1 µs, p99 524.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 18 KiB and 2.00 flushes a rename | p50 16632 µs |
| rename, then the directory's Fsync | 20 KiB and 2.00 flushes a rename | p50 16658 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | refused: Operation not supported (os error 95) | FIEMAP on the target: Ok(Extents { total: 1, shared: 0, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.08 µs, p99 33.02 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### 5. Listing: the populations built, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4

| population | files | built in s | files/s |
| --- | --- | --- | --- |
| deep-1M | 1000000 | 45.66 | 21900 |
| wide-1M | 1000000 | 133.6 | 7486 |

### 5. Listing a placement group, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · ext4 at /hdd on /dev/sda1 = sda1 (rw,relatime,rw) · fs block 4096 B · leg hyperion hdd ext4

| population | walk | chunks | cold s | cold µs/chunk | cold dev KiB read/chunk | warm s | warm µs/chunk | warm dev KiB written | hours/16 TiB at 1 MiB | at 4 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deep-1M | names | 1000000 | 26.96 | 26.96 | 0.316 | 0.707 | 0.707 | 0 | 0.126 | 0.031 |
| deep-1M | statx-ino | 1000000 | 35.26 | 35.26 | 0.317 | 3.06 | 3.06 | 0 | 0.164 | 0.041 |
| deep-1M | xattr | 1000000 | 39.02 | 39.02 | 0.317 | 5.51 | 5.51 | 0 | 0.182 | 0.045 |
| deep-1M | header-qd32 | 1000000 | 65.17 | 65.17 | 4.32 | 29.77 | 29.77 | 0 | 0.304 | 0.076 |
| wide-1M | names | 229959 (stopped at the cap) | 1800 | 7828 | 6.33 | 0 | 0 | 0 | 36.48 | 9.12 |
| wide-1M | statx-ino | 0 (stopped at the cap) | 1800 | 1800133511 | 1469784 | 0 | 0 | 0 | 8389230 | 2097308 |
| wide-1M | header-qd32 | 0 (stopped at the cap) | 1800 | 1800153198 | 1453156 | 0 | 0 | 0 | 8389322 | 2097330 |

