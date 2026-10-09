### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 53.70 µs |
| fdatasync of a clean file | p50 10.36 µs, p99 25.74 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7775 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7655 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.16 µs, p99 20.20 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 48.58 | 80.41 | - | - | - |
| fold | 22.60 | 172.9 | - | - | - |
| crc+fold | 28.71 | 136.1 | - | - | - |
| copy | 28.64 | 136.4 | - | - | - |
| decode-21-data | 17.31 | 225.6 | - | - | - |
| decode-21-parity | 17.38 | 224.7 | - | - | - |
| decode-42-data | 9.22 | 423.5 | - | - | - |
| decode-42-parity | 9.22 | 423.7 | - | - | - |
| pipeline-copy | 21.74 | 179.7 | - | - | - |
| pipeline-21 | 8.77 | 445.2 | - | - | - |
| pipeline-42 | 4.59 | 850.2 | - | - | - |
| summary-42 | 5016 | 4.67 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 8.30 | 116.4 | 266.6 | 4199 | 8398 | 0 | 0 |
| layout=2+1 | unit | 29.40 | 33.70 | 51.26 | 68.45 | 128.4 | 0 | 0 |
| layout=4+2 | chunk | 5.80 | 166.7 | 233.3 | 4202 | 16683 | 0 | 0 |
| layout=4+2 | unit | 22.40 | 44.05 | 66.39 | 68.30 | 256.6 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 56.53 | 55.76 | 0.661 | 0 | 0 | 0.340 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 67.80 | 66.12 | 0.624 | 73.19 | 15.63 | 0.410 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 62.88 | 68.76 | 0.643 | 76.25 | 17.09 | 0.423 |
| layout=4+2 piece=1M | fixed-20 | 19.82 | 0.991 | 18.00 | 0/0 | 0 | 70.86 | 75.86 | 0.672 | 72.99 | 18.51 | 0.429 |
| layout=4+2 piece=1M | fixed-40 | 34.28 | 0.857 | 32.00 | 0/0 | 0 | 127.7 | 86.99 | 0.628 | 55.51 | 16.76 | 0.555 |
| layout=4+2 piece=1M | idle | 60.96 | 0 | 58.00 | 0/0 | 0 | 78.54 | 77.53 | 0.614 | 41.20 | 15.78 | 0.845 |
| layout=4+2 piece=1M | idle-ceil-20 | 18.67 | 0 | 17.00 | 2/2 | 0 | 63.34 | 61.29 | 0.680 | 62.49 | 18.77 | 0.486 |
| layout=4+2 piece=1M | unbounded | 80.93 | 0 | 77.00 | 2/2 | 0 | 255.2 | 279.9 | 0.586 | 30.66 | 15.60 | 0.894 |
| layout=4+2 piece=256K | idle | 59.57 | 0 | 56.00 | 2/2 | 0 | 78.14 | 72.59 | 0.594 | 32.92 | 14.12 | 0.821 |
| layout=4+2 piece=4M | idle | 74.27 | 0 | 70.00 | 0/0 | 0 | 84.01 | 81.71 | 0.586 | 37.40 | 15.29 | 0.871 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 48.43 | 54.32 | 0.859 | 0.644 | 0 | 0.346 |
| role=dest layout=4+2 piece=1M | fixed-10 | 10.00 | 10.01 | 1.00 | 400.2 | 70.10 | 79.83 | 0.775 | 0.612 | 0 | 0.517 |
| role=dest layout=4+2 piece=1M | fixed-20 | 17.95 | 17.97 | 0.898 | 216.7 | 83.46 | 97.59 | 0.786 | 0.606 | 0 | 0.635 |
| role=dest layout=4+2 piece=1M | fixed-40 | 25.00 | 25.02 | 0.626 | 150.5 | 90.63 | 110.2 | 0.749 | 0.612 | 0 | 0.741 |
| role=dest layout=4+2 piece=1M | fixed-80 | 28.20 | 28.23 | 0.353 | 133.3 | 107.0 | 92.88 | 0.765 | 0.565 | 0 | 0.821 |
| role=dest layout=4+2 piece=1M | idle | 26.30 | 26.33 | 0 | 141.7 | 62.78 | 74.77 | 0.821 | 0.634 | 0 | 0.827 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 22.20 | 22.22 | 0 | 166.7 | 66.14 | 62.89 | 0.803 | 0.625 | 0 | 0.752 |
| role=dest layout=4+2 piece=1M | unbounded | 62.80 | 62.86 | 0 | 117.1 | 242.7 | 308.7 | 0.784 | 0.547 | 0 | 0.878 |
| role=local layout=copy piece=1M | idle | 16.90 | 33.93 | 0 | 217.0 | 57.96 | 65.51 | 0.807 | 0.639 | 0 | 0.831 |
| role=local layout=copy piece=1M | unbounded | 36.80 | 73.67 | 0 | 200.0 | 273.7 | 252.3 | 0.732 | 0.549 | 0 | 0.893 |
| role=local layout=2+1 piece=1M | idle | 13.00 | 39.29 | 0 | 292.1 | 88.98 | 105.2 | 0.813 | 0.623 | 0 | 0.837 |
| role=local layout=2+1 piece=1M | unbounded | 25.60 | 76.67 | 0 | 291.8 | 301.5 | 374.5 | 0.706 | 0.536 | 0 | 0.896 |
| role=local layout=4+2 piece=1M | idle | 8.80 | 43.84 | 0 | 433.5 | 64.31 | 74.05 | 0.808 | 0.608 | 0 | 0.837 |
| role=local layout=4+2 piece=1M | unbounded | 15.60 | 78.28 | 0 | 483.9 | 474.8 | 525.7 | 0.931 | 0.617 | 0 | 0.893 |

