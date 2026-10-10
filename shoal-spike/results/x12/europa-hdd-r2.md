### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.80 KiB a sync · sync p50 54.02 µs |
| fdatasync of a clean file | p50 10.37 µs, p99 21.80 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7887 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7667 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.38 µs, p99 28.34 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 51.07 | 76.48 | - | - | - |
| fold | 22.68 | 172.3 | - | - | - |
| crc+fold | 29.06 | 134.4 | - | - | - |
| copy | 28.57 | 136.7 | - | - | - |
| decode-21-data | 17.45 | 223.8 | - | - | - |
| decode-21-parity | 17.45 | 223.8 | - | - | - |
| decode-42-data | 9.24 | 422.9 | - | - | - |
| decode-42-parity | 9.31 | 419.8 | - | - | - |
| pipeline-copy | 21.35 | 183.0 | - | - | - |
| pipeline-21 | 8.78 | 445.1 | - | - | - |
| pipeline-42 | 4.59 | 850.7 | - | - | - |
| summary-42 | 4580 | 5.12 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 2 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 22.60 | 43.62 | 64.49 | 68.30 | 256.3 | 0 | 0 |
| layout=4+2 | chunk | 5.90 | 166.3 | 217.0 | 4178 | 16678 | 0 | 0 |
| layout=2+1 | unit | 30.30 | 33.24 | 51.71 | 68.22 | 128.4 | 0 | 0 |
| layout=2+1 | chunk | 8.70 | 108.4 | 141.7 | 4198 | 8353 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 2 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=4M | idle | 71.27 | 0 | 68.00 | 0/0 | 0 | 106.1 | 106.1 | 0.590 | 33.29 | 15.28 | 0.871 |
| layout=4+2 piece=256K | idle | 57.34 | 0 | 54.00 | 0/0 | 0 | 83.14 | 87.93 | 0.597 | 34.91 | 14.00 | 0.838 |
| layout=4+2 piece=1M | unbounded | 75.47 | 0 | 73.00 | 2/2 | 0 | 283.7 | 264.5 | 0.538 | 30.88 | 16.12 | 0.885 |
| layout=4+2 piece=1M | idle-ceil-20 | 18.87 | 0 | 18.00 | 0/0 | 0 | 59.35 | 84.12 | 0.659 | 46.96 | 17.50 | 0.508 |
| layout=4+2 piece=1M | idle | 58.21 | 0 | 55.00 | 0/0 | 0 | 64.48 | 67.23 | 0.598 | 36.47 | 16.31 | 0.851 |
| layout=4+2 piece=1M | fixed-40 | 35.08 | 0.877 | 33.00 | 0/0 | 0 | 85.37 | 216.4 | 0.669 | 50.31 | 17.37 | 0.573 |
| layout=4+2 piece=1M | fixed-20 | 19.62 | 0.981 | 18.00 | 0/0 | 0 | 84.83 | 77.91 | 0.643 | 69.41 | 17.03 | 0.465 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 72.52 | 66.30 | 0.669 | 62.11 | 15.39 | 0.462 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 63.54 | 66.30 | 0.637 | 86.63 | 14.59 | 0.418 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 62.76 | 79.53 | 0.639 | 0 | 0 | 0.344 |

### X12 · A rebuild beside the foreground, round 2 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 15.80 | 79.68 | 0 | 483.6 | 418.0 | 343.1 | 0.725 | 0.546 | 0 | 0.903 |
| role=local layout=4+2 piece=1M | idle | 9.80 | 49.15 | 0 | 391.7 | 57.75 | 57.94 | 0.790 | 0.586 | 0 | 0.843 |
| role=local layout=2+1 piece=1M | unbounded | 25.00 | 75.07 | 0 | 308.2 | 279.0 | 285.6 | 0.698 | 0.571 | 0 | 0.900 |
| role=local layout=2+1 piece=1M | idle | 13.85 | 41.84 | 0 | 275.0 | 61.93 | 57.94 | 0.744 | 0.566 | 0 | 0.854 |
| role=local layout=copy piece=1M | unbounded | 36.25 | 72.72 | 0 | 216.7 | 255.9 | 274.4 | 0.736 | 0.566 | 0 | 0.898 |
| role=local layout=copy piece=1M | idle | 17.90 | 35.73 | 0 | 208.3 | 52.46 | 54.57 | 0.762 | 0.614 | 0 | 0.832 |
| role=dest layout=4+2 piece=1M | unbounded | 62.70 | 62.76 | 0 | 118.1 | 254.2 | 288.3 | 0.762 | 0.569 | 0 | 0.885 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 23.30 | 23.32 | 0 | 158.7 | 64.92 | 73.53 | 0.775 | 0.611 | 0 | 0.769 |
| role=dest layout=4+2 piece=1M | idle | 26.15 | 26.18 | 0 | 149.7 | 80.72 | 85.46 | 0.786 | 0.607 | 0 | 0.816 |
| role=dest layout=4+2 piece=1M | fixed-80 | 27.55 | 27.58 | 0.345 | 141.3 | 97.70 | 103.5 | 0.749 | 0.585 | 0 | 0.819 |
| role=dest layout=4+2 piece=1M | fixed-40 | 24.05 | 24.07 | 0.602 | 158.2 | 86.31 | 105.1 | 0.756 | 0.583 | 0 | 0.733 |
| role=dest layout=4+2 piece=1M | fixed-20 | 17.65 | 17.67 | 0.883 | 217.2 | 79.58 | 88.43 | 0.878 | 0.673 | 0 | 0.622 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.90 | 9.91 | 0.991 | 400.0 | 56.76 | 74.96 | 0.821 | 0.644 | 0 | 0.494 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 39.77 | 55.98 | 0.878 | 0.665 | 0 | 0.297 |

