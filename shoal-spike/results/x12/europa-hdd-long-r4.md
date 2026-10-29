### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs long

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 55.48 µs |
| fdatasync of a clean file | p50 22.04 µs, p99 37.55 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7771 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7661 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.16 µs, p99 15.72 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs long

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | idle-ceil-20 | 18.74 | 0 | 72.00 | 0/0 | 0 | 63.51 | 65.15 | 0.650 | 68.62 | 18.13 | 0.510 |
| layout=4+2 piece=1M | idle | 58.32 | 0 | 224.0 | 2/2 | 0 | 68.47 | 68.80 | 0.617 | 37.95 | 16.07 | 0.846 |
| layout=4+2 piece=1M | fixed-10 | 9.99 | 0.999 | 38.00 | 2/2 | 0 | 66.42 | 76.28 | 0.647 | 62.42 | 18.16 | 0.453 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 19.00 | 0/0 | 0 | 63.53 | 70.77 | 0.663 | 63.39 | 18.01 | 0.417 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 50.44 | 60.78 | 0.688 | 0 | 0 | 0.346 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs long

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | idle | 25.24 | 25.27 | 0 | 141.8 | 78.34 | 96.47 | 0.832 | 0.600 | 0 | 0.814 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.90 | 9.91 | 0.991 | 400.3 | 83.82 | 101.6 | 0.857 | 0.666 | 0 | 0.514 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 55.26 | 74.71 | 0.933 | 0.666 | 0 | 0.332 |

