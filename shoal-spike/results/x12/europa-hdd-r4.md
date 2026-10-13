### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.64 KiB a sync · sync p50 53.66 µs |
| fdatasync of a clean file | p50 10.33 µs, p99 20.66 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7741 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7665 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.20 µs, p99 19.84 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 49.94 | 78.23 | - | - | - |
| fold | 22.68 | 172.2 | - | - | - |
| crc+fold | 28.80 | 135.6 | - | - | - |
| copy | 28.73 | 136.0 | - | - | - |
| decode-21-data | 17.49 | 223.3 | - | - | - |
| decode-21-parity | 17.48 | 223.4 | - | - | - |
| decode-42-data | 9.29 | 420.6 | - | - | - |
| decode-42-parity | 9.28 | 420.9 | - | - | - |
| pipeline-copy | 21.49 | 181.7 | - | - | - |
| pipeline-21 | 8.68 | 450.0 | - | - | - |
| pipeline-42 | 4.51 | 865.9 | - | - | - |
| summary-42 | 4201 | 5.58 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 4 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 22.40 | 44.28 | 69.31 | 68.30 | 256.9 | 0 | 0 |
| layout=4+2 | chunk | 5.80 | 158.4 | 225.1 | 4175 | 16895 | 0 | 0 |
| layout=2+1 | unit | 29.70 | 33.69 | 55.25 | 68.94 | 128.0 | 0 | 0 |
| layout=2+1 | chunk | 8.70 | 108.5 | 142.1 | 4163 | 8341 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=4M | idle | 72.67 | 0 | 68.00 | 2/2 | 0 | 84.43 | 98.99 | 0.675 | 33.13 | 15.51 | 0.875 |
| layout=4+2 piece=256K | idle | 57.23 | 0 | 53.00 | 0/0 | 0 | 89.19 | 79.16 | 0.555 | 33.33 | 14.32 | 0.822 |
| layout=4+2 piece=1M | unbounded | 77.78 | 0 | 74.00 | 0/0 | 0 | 266.4 | 328.0 | 0.554 | 32.42 | 15.58 | 0.878 |
| layout=4+2 piece=1M | idle-ceil-20 | 19.07 | 0 | 18.00 | 0/0 | 0 | 60.19 | 63.31 | 0.659 | 56.89 | 17.37 | 0.503 |
| layout=4+2 piece=1M | idle | 57.41 | 0 | 54.00 | 0/0 | 0 | 62.61 | 66.92 | 0.579 | 35.12 | 15.93 | 0.847 |
| layout=4+2 piece=1M | fixed-40 | 34.63 | 0.866 | 32.00 | 0/0 | 0 | 116.7 | 127.7 | 0.611 | 52.70 | 15.34 | 0.556 |
| layout=4+2 piece=1M | fixed-20 | 19.87 | 0.993 | 19.00 | 0/0 | 0 | 78.03 | 72.13 | 0.640 | 48.80 | 15.92 | 0.472 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 67.22 | 67.13 | 0.676 | 66.31 | 17.32 | 0.434 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 73.78 | 67.09 | 0.682 | 62.12 | 22.95 | 0.416 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 57.28 | 53.94 | 0.671 | 0 | 0 | 0.337 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 16.00 | 80.08 | 0 | 483.2 | 316.9 | 405.9 | 0.705 | 0.580 | 0 | 0.896 |
| role=local layout=4+2 piece=1M | idle | 9.60 | 48.65 | 0 | 416.7 | 58.32 | 67.41 | 0.880 | 0.579 | 0 | 0.845 |
| role=local layout=2+1 piece=1M | unbounded | 25.15 | 75.82 | 0 | 316.3 | 330.3 | 318.0 | 0.686 | 0.603 | 0 | 0.898 |
| role=local layout=2+1 piece=1M | idle | 13.40 | 40.49 | 0 | 275.3 | 69.19 | 70.87 | 0.808 | 0.625 | 0 | 0.841 |
| role=local layout=copy piece=1M | unbounded | 36.20 | 72.67 | 0 | 208.9 | 228.8 | 290.2 | 0.756 | 0.549 | 0 | 0.890 |
| role=local layout=copy piece=1M | idle | 17.30 | 34.73 | 0 | 216.6 | 67.35 | 60.88 | 0.823 | 0.612 | 0 | 0.834 |
| role=dest layout=4+2 piece=1M | unbounded | 62.40 | 62.46 | 0 | 117.2 | 190.6 | 258.9 | 0.808 | 0.551 | 0 | 0.885 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 23.80 | 23.82 | 0 | 158.5 | 100.9 | 78.18 | 0.783 | 0.608 | 0 | 0.771 |
| role=dest layout=4+2 piece=1M | idle | 26.85 | 26.88 | 0 | 141.1 | 64.56 | 77.17 | 0.755 | 0.560 | 0 | 0.821 |
| role=dest layout=4+2 piece=1M | fixed-80 | 27.90 | 27.93 | 0.349 | 141.3 | 107.2 | 99.92 | 0.776 | 0.611 | 0 | 0.830 |
| role=dest layout=4+2 piece=1M | fixed-40 | 25.30 | 25.32 | 0.633 | 150.4 | 68.66 | 186.8 | 0.793 | 0.572 | 0 | 0.741 |
| role=dest layout=4+2 piece=1M | fixed-20 | 17.95 | 17.97 | 0.898 | 216.7 | 84.92 | 82.08 | 0.776 | 0.629 | 0 | 0.633 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.95 | 9.96 | 0.996 | 399.8 | 66.46 | 68.48 | 0.899 | 0.632 | 0 | 0.510 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 37.09 | 49.22 | 0.925 | 0.672 | 0 | 0.289 |

