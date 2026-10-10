### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs long

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1112 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.50 µs |
| fdatasync of a clean file | p50 23.86 µs, p99 46.18 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8242 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8260 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.91 µs, p99 33.10 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs long

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 105.1 | 109.6 | 0.094 | 0 | 0 | 0.400 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 19.00 | 0/0 | 0 | 109.2 | 109.0 | 0.089 | 116.7 | 48.52 | 0.422 |
| layout=4+2 piece=1M | fixed-10 | 9.99 | 0.999 | 38.00 | 0/0 | 0 | 121.5 | 129.9 | 0.091 | 117.3 | 48.94 | 0.432 |
| layout=4+2 piece=1M | idle | 45.26 | 0 | 175.0 | 2/2 | 0 | 119.0 | 129.2 | 0.151 | 116.6 | 48.59 | 0.788 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.04 | 0 | 66.00 | 0/0 | 0 | 101.2 | 123.5 | 0.094 | 116.7 | 48.62 | 0.510 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs long

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 104.6 | 112.7 | 14.74 | 0.082 | 0 | 0.401 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.64 | 9.65 | 0.965 | 408.4 | 154.9 | 181.8 | 14.74 | 0.176 | 0 | 0.568 |
| role=dest layout=4+2 piece=1M | idle | 18.19 | 18.21 | 0 | 191.7 | 148.9 | 117.5 | 14.73 | 0.224 | 0 | 0.771 |

