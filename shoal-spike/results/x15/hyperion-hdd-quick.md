### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.94 µs |
| fdatasync of a clean file | p50 23.88 µs, p99 37.81 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8248 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8263 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.02 µs, p99 32.44 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 12.00 | 62.23 | 0.090 | 21649 | 45.25 | 71.55 | 40532 | 66721 | 83356 | 16.36 | 4181 | 65.32 | 0 | 0 | 317.2 | 1.00 |
| 64K | 1 | class-pool | 12.00 | 60.18 | 0.090 | 39686 | 45.48 | 77.46 | 20188 | 58383 | 74981 | 17.13 | 148.0 | 2.31 | 0 | 0 | 67.08 | 1.05 |
| 64K | 1 | class-fresh | 12.00 | 108.3 | 69.34 | 16582 | 17326 | 65.82 | 8321 | 41680 | 50025 | 23.59 | 76.00 | 1.19 | 0 | 0 | 0.000 | 1.44 |
| 64K | 8 | today | 12.00 | 184.9 | 0.050 | 42734 | 51.65 | 64.14 | 64020 | 108008 | 291861 | 30.64 | 4177 | 65.27 | 0 | 4.00 | 583.5 | 1.00 |
| 64K | 8 | class-pool | 12.00 | 148.9 | 0.050 | 21849 | 72.85 | 64.52 | 35100 | 58235 | 142186 | 62.36 | 145.0 | 2.27 | 0 | 4.00 | 292.4 | 2.04 |
| 64K | 8 | class-fresh | 12.00 | 731.1 | 321.3 | 15967 | 15404 | 111.0 | 24881 | 50186 | 58375 | 119.8 | 71.67 | 1.12 | 0 | 0 | 0.000 | 3.91 |
| 1M | 1 | today | 12.00 | 70.93 | 0.100 | 28761 | 45.69 | 71.89 | 32354 | 66658 | 107550 | 14.26 | 5152 | 5.03 | 0 | 0 | 692.0 | 1.00 |
| 1M | 1 | class-pool | 12.00 | 70.64 | 0.081 | 28086 | 45.41 | 68.37 | 29343 | 58323 | 66688 | 18.47 | 2070 | 2.02 | 0 | 0 | 350.9 | 1.29 |
| 1M | 1 | class-fresh | 12.00 | 109.1 | 69.78 | 25787 | 15592 | 66.33 | 8318 | 50002 | 50036 | 21.83 | 1036 | 1.01 | 0 | 0 | 0.000 | 1.53 |
| 1M | 8 | today | 12.00 | 175.9 | 0.050 | 29052 | 52.49 | 64.69 | 38592 | 74431 | 225032 | 40.07 | 5137 | 5.02 | 0 | 4.00 | 500.4 | 1.00 |
| 1M | 8 | class-pool | 12.00 | 159.0 | 0.051 | 65837 | 70.51 | 63.69 | 55691 | 116027 | 208541 | 37.93 | 2065 | 2.02 | 0 | 4.00 | 350.4 | 0.947 |
| 1M | 8 | class-fresh | 12.00 | 1057 | 420.8 | 61859 | 58827 | 104.7 | 16757 | 141043 | 149296 | 45.07 | 1033 | 1.01 | 0 | 0 | 0.000 | 1.12 |

