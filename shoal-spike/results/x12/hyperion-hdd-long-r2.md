### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs long

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1112 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.83 µs |
| fdatasync of a clean file | p50 24.04 µs, p99 50.20 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.10 µs, p99 33.02 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A deep scrub beside the foreground, round 2 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs long

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | idle-ceil-20 | 16.52 | 0 | 64.00 | 0/0 | 0 | 133.9 | 136.1 | 0.080 | 118.3 | 47.88 | 0.524 |
| layout=4+2 piece=1M | idle | 43.98 | 0 | 172.0 | 2/2 | 0 | 127.8 | 122.2 | 0.114 | 117.0 | 47.97 | 0.776 |
| layout=4+2 piece=1M | fixed-10 | 9.98 | 0.998 | 38.00 | 0/0 | 0 | 116.3 | 111.3 | 0.084 | 117.3 | 48.07 | 0.436 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 19.00 | 0/0 | 0 | 93.36 | 107.1 | 0.080 | 124.4 | 47.80 | 0.418 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 95.21 | 90.42 | 0.084 | 0 | 0 | 0.395 |

### X12 · A rebuild beside the foreground, round 2 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs long

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | idle | 18.43 | 18.45 | 0 | 183.4 | 131.0 | 129.9 | 14.84 | 0.225 | 0 | 0.769 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.69 | 9.70 | 0.970 | 408.3 | 128.5 | 149.0 | 14.88 | 0.179 | 0 | 0.554 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 85.57 | 104.7 | 14.80 | 0.081 | 0 | 0.408 |

