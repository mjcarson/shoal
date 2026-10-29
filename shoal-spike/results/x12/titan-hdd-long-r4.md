### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs long

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1112 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.42 µs |
| fdatasync of a clean file | p50 23.80 µs, p99 36.57 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.17 µs, p99 33.25 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs long

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | idle-ceil-20 | 16.65 | 0 | 64.00 | 0/0 | 0 | 123.0 | 154.8 | 0.087 | 116.2 | 47.83 | 0.513 |
| layout=4+2 piece=1M | idle | 43.87 | 0 | 169.0 | 2/2 | 0 | 111.4 | 136.8 | 0.146 | 116.4 | 47.73 | 0.775 |
| layout=4+2 piece=1M | fixed-10 | 9.94 | 0.994 | 38.00 | 2/2 | 0 | 122.2 | 153.3 | 0.089 | 116.0 | 47.75 | 0.443 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 19.00 | 0/0 | 0 | 103.5 | 123.4 | 0.086 | 117.9 | 47.82 | 0.430 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 111.1 | 138.3 | 0.073 | 0 | 0 | 0.399 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs long

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | idle | 18.58 | 18.60 | 0 | 191.7 | 123.6 | 139.0 | 14.75 | 0.169 | 0 | 0.767 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.67 | 9.68 | 0.968 | 408.3 | 149.8 | 208.5 | 14.74 | 0.164 | 0 | 0.554 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 98.03 | 139.0 | 14.70 | 0.079 | 0 | 0.394 |

