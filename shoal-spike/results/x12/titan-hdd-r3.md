### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.95 µs |
| fdatasync of a clean file | p50 24.20 µs, p99 46.06 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8241 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8258 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.03 µs, p99 33.29 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 3 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.57 | 337.7 | - | - | - |
| fold | 13.91 | 280.8 | - | - | - |
| crc+fold | 7.76 | 503.6 | - | - | - |
| copy | 6.64 | 588.0 | - | - | - |
| decode-21-data | 5.12 | 763.4 | - | - | - |
| decode-21-parity | 5.12 | 763.4 | - | - | - |
| decode-42-data | 3.04 | 1284 | - | - | - |
| decode-42-parity | 3.03 | 1289 | - | - | - |
| pipeline-copy | 3.20 | 1222 | - | - | - |
| pipeline-21 | 2.23 | 1750 | - | - | - |
| pipeline-42 | 1.30 | 3007 | - | - | - |
| summary-42 | 840.7 | 27.88 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 3 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 7.70 | 125.0 | 175.0 | 4157 | 8306 | 0 | 0 |
| layout=2+1 | unit | 20.40 | 47.59 | 128.8 | 71.29 | 128.0 | 0 | 0 |
| layout=4+2 | chunk | 5.60 | 175.0 | 241.6 | 4122 | 16400 | 0 | 0 |
| layout=4+2 | unit | 19.20 | 52.13 | 88.32 | 69.92 | 258.3 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 90.78 | 102.1 | 0.081 | 0 | 0 | 0.395 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 89.34 | 100.3 | 0.097 | 119.7 | 49.28 | 0.395 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 96.89 | 100.5 | 0.075 | 115.4 | 48.09 | 0.429 |
| layout=4+2 piece=1M | fixed-20 | 19.17 | 0.958 | 18.00 | 0/0 | 0 | 151.5 | 142.4 | 0.089 | 116.5 | 48.38 | 0.456 |
| layout=4+2 piece=1M | fixed-40 | 32.23 | 0.806 | 31.00 | 0/0 | 0 | 162.6 | 154.1 | 0.085 | 116.7 | 48.29 | 0.552 |
| layout=4+2 piece=1M | idle | 45.99 | 0 | 43.00 | 2/2 | 0 | 112.9 | 113.4 | 0.058 | 115.6 | 48.20 | 0.774 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.22 | 0 | 16.00 | 2/2 | 0 | 105.0 | 100.3 | 0.092 | 117.0 | 48.69 | 0.486 |
| layout=4+2 piece=1M | unbounded | 88.29 | 0 | 83.00 | 0/0 | 0 | 724.9 | 604.4 | 0.219 | 116.1 | 48.01 | 0.839 |
| layout=4+2 piece=256K | idle | 43.04 | 0 | 39.00 | 0/0 | 0 | 102.6 | 117.3 | 0.060 | 115.4 | 47.92 | 0.786 |
| layout=4+2 piece=4M | idle | 52.65 | 0 | 51.00 | 0/0 | 0 | 112.8 | 137.1 | 0.134 | 115.6 | 48.12 | 0.781 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 66.21 | 74.09 | 14.68 | 0.100 | 0 | 0.362 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.65 | 9.66 | 0.966 | 408.3 | 91.39 | 148.3 | 14.75 | 0.089 | 0 | 0.527 |
| role=dest layout=4+2 piece=1M | fixed-20 | 16.85 | 16.87 | 0.843 | 233.2 | 114.9 | 169.8 | 14.76 | 0.205 | 0 | 0.591 |
| role=dest layout=4+2 piece=1M | fixed-40 | 22.40 | 22.42 | 0.561 | 175.0 | 231.7 | 229.8 | 14.75 | 0.253 | 0 | 0.672 |
| role=dest layout=4+2 piece=1M | fixed-80 | 28.15 | 28.18 | 0.352 | 133.3 | 430.1 | 414.0 | 15.27 | 0.191 | 0 | 0.784 |
| role=dest layout=4+2 piece=1M | idle | 21.55 | 21.57 | 0 | 150.1 | 87.90 | 127.7 | 14.73 | 0.183 | 0 | 0.769 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 19.45 | 19.47 | 0 | 175.0 | 82.40 | 144.6 | 14.87 | 0.227 | 0 | 0.701 |
| role=dest layout=4+2 piece=1M | unbounded | 58.20 | 58.26 | 0 | 116.7 | 623.5 | 624.2 | 14.77 | 0.247 | 0 | 0.793 |
| role=local layout=copy piece=1M | idle | 14.00 | 28.23 | 0 | 250.1 | 88.26 | 100.6 | 14.77 | 0.130 | 0 | 0.777 |
| role=local layout=copy piece=1M | unbounded | 32.10 | 64.16 | 0 | 241.6 | 494.3 | 562.5 | 14.78 | 0.155 | 0 | 0.834 |
| role=local layout=2+1 piece=1M | idle | 10.80 | 32.58 | 0 | 283.4 | 90.96 | 126.2 | 14.70 | 0.079 | 0 | 0.783 |
| role=local layout=2+1 piece=1M | unbounded | 25.80 | 77.78 | 0 | 283.5 | 625.4 | 624.5 | 14.78 | 0.212 | 0 | 0.850 |
| role=local layout=4+2 piece=1M | idle | 7.75 | 38.64 | 0 | 466.8 | 98.44 | 295.5 | 14.76 | 0.166 | 0 | 0.778 |
| role=local layout=4+2 piece=1M | unbounded | 16.60 | 83.38 | 0 | 458.3 | 740.2 | 1068 | 14.74 | 0.164 | 0 | 0.845 |

