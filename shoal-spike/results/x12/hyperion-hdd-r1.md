### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.50 µs |
| fdatasync of a clean file | p50 23.79 µs, p99 41.57 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.96 µs, p99 33.04 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.57 | 337.7 | - | - | - |
| fold | 14.06 | 277.8 | - | - | - |
| crc+fold | 7.76 | 503.4 | - | - | - |
| copy | 6.70 | 582.8 | - | - | - |
| decode-21-data | 5.22 | 748.3 | - | - | - |
| decode-21-parity | 5.21 | 750.2 | - | - | - |
| decode-42-data | 3.05 | 1280 | - | - | - |
| decode-42-parity | 3.06 | 1278 | - | - | - |
| pipeline-copy | 3.20 | 1220 | - | - | - |
| pipeline-21 | 2.23 | 1749 | - | - | - |
| pipeline-42 | 1.30 | 3005 | - | - | - |
| summary-42 | 834.2 | 28.09 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 7.90 | 125.0 | 175.0 | 4147 | 8226 | 0 | 0 |
| layout=2+1 | unit | 21.60 | 46.37 | 85.47 | 71.74 | 128.0 | 0 | 0 |
| layout=4+2 | chunk | 5.40 | 175.1 | 241.8 | 4167 | 16552 | 0 | 0 |
| layout=4+2 | unit | 19.70 | 49.40 | 78.26 | 68.35 | 258.6 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 105.0 | 88.46 | 0.074 | 0 | 0 | 0.402 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 241.3 | 187.8 | 0.084 | 120.0 | 51.98 | 0.432 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 118.6 | 114.4 | 0.056 | 116.9 | 48.52 | 0.437 |
| layout=4+2 piece=1M | fixed-20 | 19.27 | 0.963 | 18.00 | 0/0 | 0 | 141.0 | 127.2 | 0.077 | 118.0 | 48.41 | 0.495 |
| layout=4+2 piece=1M | fixed-40 | 32.73 | 0.818 | 31.00 | 0/0 | 0 | 175.8 | 123.0 | 0.114 | 116.6 | 48.88 | 0.551 |
| layout=4+2 piece=1M | idle | 50.85 | 0 | 47.00 | 0/0 | 0 | 95.86 | 116.1 | 0.182 | 116.7 | 48.39 | 0.792 |
| layout=4+2 piece=1M | idle-ceil-20 | 18.27 | 0 | 17.00 | 2/2 | 0 | 101.2 | 84.58 | 0.067 | 114.8 | 48.51 | 0.415 |
| layout=4+2 piece=1M | unbounded | 81.58 | 0 | 79.00 | 2/2 | 0 | 722.7 | 689.7 | 0.174 | 118.3 | 48.73 | 0.842 |
| layout=4+2 piece=256K | idle | 48.13 | 0 | 45.00 | 2/2 | 0 | 102.2 | 103.8 | 0.145 | 118.0 | 48.64 | 0.804 |
| layout=4+2 piece=4M | idle | 57.06 | 0 | 52.00 | 0/0 | 0 | 109.0 | 117.1 | 0.160 | 115.7 | 48.85 | 0.802 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 70.58 | 74.17 | 14.77 | 0.053 | 0 | 0.342 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.90 | 9.91 | 0.991 | 391.8 | 81.94 | 91.62 | 14.85 | 0.246 | 0 | 0.501 |
| role=dest layout=4+2 piece=1M | fixed-20 | 17.05 | 17.07 | 0.853 | 233.3 | 84.36 | 104.5 | 14.89 | 0.162 | 0 | 0.603 |
| role=dest layout=4+2 piece=1M | fixed-40 | 22.70 | 22.72 | 0.568 | 166.8 | 204.7 | 198.3 | 15.13 | 0.198 | 0 | 0.678 |
| role=dest layout=4+2 piece=1M | fixed-80 | 29.40 | 29.43 | 0.368 | 125.1 | 354.4 | 365.0 | 14.93 | 0.237 | 0 | 0.788 |
| role=dest layout=4+2 piece=1M | idle | 22.60 | 22.62 | 0 | 150.0 | 78.51 | 125.1 | 14.90 | 0.241 | 0 | 0.770 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 19.55 | 19.57 | 0 | 191.6 | 69.06 | 92.56 | 14.88 | 0.228 | 0 | 0.707 |
| role=dest layout=4+2 piece=1M | unbounded | 57.95 | 58.01 | 0 | 125.0 | 354.8 | 482.7 | 15.27 | 0.248 | 0 | 0.798 |
| role=local layout=copy piece=1M | idle | 14.60 | 29.23 | 0 | 250.0 | 77.88 | 105.5 | 14.90 | 0.078 | 0 | 0.789 |
| role=local layout=copy piece=1M | unbounded | 33.00 | 66.46 | 0 | 225.1 | 488.0 | 396.6 | 14.85 | 0.197 | 0 | 0.851 |
| role=local layout=2+1 piece=1M | idle | 12.40 | 37.24 | 0 | 299.9 | 74.45 | 96.30 | 14.90 | 0.082 | 0 | 0.793 |
| role=local layout=2+1 piece=1M | unbounded | 25.60 | 76.88 | 0 | 291.7 | 578.3 | 537.8 | 14.88 | 0.240 | 0 | 0.840 |
| role=local layout=4+2 piece=1M | idle | 8.40 | 42.79 | 0 | 441.6 | 124.1 | 105.7 | 14.86 | 0.186 | 0 | 0.801 |
| role=local layout=4+2 piece=1M | unbounded | 17.40 | 85.88 | 0 | 450.0 | 595.2 | 780.3 | 14.91 | 0.159 | 0 | 0.844 |

