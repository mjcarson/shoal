### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.64 KiB a sync · sync p50 28.00 µs |
| fdatasync of a clean file | p50 23.73 µs, p99 36.21 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8243 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8263 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.01 µs, p99 32.49 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.55 | 338.2 | - | - | - |
| fold | 14.07 | 277.7 | - | - | - |
| crc+fold | 7.72 | 506.0 | - | - | - |
| copy | 6.62 | 589.7 | - | - | - |
| decode-21-data | 5.07 | 769.7 | - | - | - |
| decode-21-parity | 5.10 | 765.5 | - | - | - |
| decode-42-data | 3.03 | 1291 | - | - | - |
| decode-42-parity | 3.07 | 1271 | - | - | - |
| pipeline-copy | 3.22 | 1215 | - | - | - |
| pipeline-21 | 2.22 | 1756 | - | - | - |
| pipeline-42 | 1.29 | 3020 | - | - | - |
| summary-42 | 825.7 | 28.39 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 4 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 19.00 | 48.64 | 88.06 | 71.81 | 256.7 | 0 | 0 |
| layout=4+2 | chunk | 5.40 | 183.3 | 250.1 | 4180 | 16716 | 0 | 0 |
| layout=2+1 | unit | 22.20 | 46.88 | 64.47 | 68.31 | 128.6 | 0 | 0 |
| layout=2+1 | chunk | 7.60 | 125.1 | 175.1 | 4185 | 8416 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=4M | idle | 53.65 | 0 | 50.00 | 2/2 | 0 | 120.4 | 106.3 | 0.115 | 115.0 | 49.32 | 0.776 |
| layout=4+2 piece=256K | idle | 44.44 | 0 | 41.00 | 0/0 | 0 | 100.8 | 126.7 | 0.175 | 118.1 | 52.32 | 0.775 |
| layout=4+2 piece=1M | unbounded | 81.68 | 0 | 78.00 | 0/0 | 0 | 722.4 | 511.8 | 0.202 | 115.3 | 51.94 | 0.830 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.82 | 0 | 16.00 | 0/0 | 0 | 84.70 | 109.1 | 0.079 | 120.3 | 51.56 | 0.464 |
| layout=4+2 piece=1M | idle | 45.54 | 0 | 45.00 | 0/0 | 0 | 109.9 | 102.4 | 0.116 | 116.3 | 51.96 | 0.783 |
| layout=4+2 piece=1M | fixed-40 | 33.13 | 0.828 | 31.00 | 0/0 | 0 | 165.0 | 154.8 | 0.069 | 117.2 | 51.47 | 0.530 |
| layout=4+2 piece=1M | fixed-20 | 19.62 | 0.981 | 18.00 | 0/0 | 0 | 110.5 | 96.58 | 0.094 | 119.1 | 51.51 | 0.416 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 118.8 | 84.25 | 0.066 | 116.0 | 51.64 | 0.379 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 85.14 | 106.3 | 0.078 | 118.4 | 50.94 | 0.364 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 81.30 | 85.71 | 0.100 | 0 | 0 | 0.347 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 16.40 | 82.28 | 0 | 466.6 | 608.3 | 787.9 | 14.86 | 0.185 | 0 | 0.847 |
| role=local layout=4+2 piece=1M | idle | 8.45 | 42.74 | 0 | 458.3 | 87.30 | 108.4 | 14.77 | 0.095 | 0 | 0.794 |
| role=local layout=2+1 piece=1M | unbounded | 25.40 | 76.42 | 0 | 291.6 | 534.1 | 573.6 | 14.76 | 0.190 | 0 | 0.863 |
| role=local layout=2+1 piece=1M | idle | 12.20 | 37.04 | 0 | 283.3 | 68.44 | 73.81 | 14.96 | 0.104 | 0 | 0.791 |
| role=local layout=copy piece=1M | unbounded | 33.60 | 67.62 | 0 | 216.8 | 376.0 | 437.9 | 14.89 | 0.143 | 0 | 0.852 |
| role=local layout=copy piece=1M | idle | 15.40 | 30.68 | 0 | 241.7 | 101.5 | 82.93 | 14.83 | 0.078 | 0 | 0.803 |
| role=dest layout=4+2 piece=1M | unbounded | 60.00 | 60.06 | 0 | 116.7 | 346.1 | 446.1 | 14.80 | 0.238 | 0 | 0.812 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 20.45 | 20.47 | 0 | 175.1 | 63.50 | 81.31 | 14.83 | 0.189 | 0 | 0.721 |
| role=dest layout=4+2 piece=1M | idle | 23.25 | 23.27 | 0 | 166.7 | 80.56 | 77.40 | 14.84 | 0.201 | 0 | 0.776 |
| role=dest layout=4+2 piece=1M | fixed-80 | 30.50 | 30.53 | 0.382 | 125.0 | 314.2 | 426.4 | 14.82 | 0.228 | 0 | 0.800 |
| role=dest layout=4+2 piece=1M | fixed-40 | 22.85 | 22.87 | 0.572 | 166.7 | 133.2 | 143.7 | 14.90 | 0.212 | 0 | 0.686 |
| role=dest layout=4+2 piece=1M | fixed-20 | 16.75 | 16.77 | 0.838 | 233.3 | 141.8 | 161.8 | 14.93 | 0.210 | 0 | 0.615 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.85 | 9.86 | 0.986 | 400.2 | 79.56 | 88.82 | 14.92 | 0.094 | 0 | 0.504 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 86.69 | 150.7 | 14.76 | 0.091 | 0 | 0.335 |

