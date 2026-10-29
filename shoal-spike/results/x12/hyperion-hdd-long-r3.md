### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs long

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1112 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.45 µs |
| fdatasync of a clean file | p50 24.20 µs, p99 37.30 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8262 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.14 µs, p99 33.20 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs long

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 98.20 | 97.84 | 0.081 | 0 | 0 | 0.395 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 19.00 | 0/0 | 0 | 99.39 | 102.4 | 0.079 | 118.1 | 47.71 | 0.413 |
| layout=4+2 piece=1M | fixed-10 | 9.98 | 0.998 | 38.00 | 0/0 | 0 | 114.3 | 114.0 | 0.077 | 116.2 | 47.39 | 0.430 |
| layout=4+2 piece=1M | idle | 45.71 | 0 | 175.0 | 2/2 | 0 | 114.0 | 114.8 | 0.155 | 116.1 | 47.40 | 0.788 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.09 | 0 | 66.00 | 0/0 | 0 | 89.14 | 97.86 | 0.063 | 116.7 | 47.43 | 0.520 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs long

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 86.01 | 102.2 | 14.78 | 0.058 | 0 | 0.410 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.76 | 9.77 | 0.977 | 408.3 | 132.7 | 146.2 | 14.83 | 0.149 | 0 | 0.560 |
| role=dest layout=4+2 piece=1M | idle | 18.48 | 18.50 | 0 | 191.7 | 108.3 | 115.2 | 14.85 | 0.227 | 0 | 0.767 |

