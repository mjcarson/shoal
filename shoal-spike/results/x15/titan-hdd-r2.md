### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.41 µs |
| fdatasync of a clean file | p50 24.04 µs, p99 44.92 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8235 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8255 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.08 µs, p99 32.97 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 2 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 211.0 | 76.92 | 0.080 | 23559 | 48.34 | 80.26 | 35688 | 75041 | 241845 | 10.50 | 4197 | 65.58 | 0 | 47.00 | 492.9 | 1.00 |
| 64K | 1 | class-pool | 416.0 | 72.03 | 0.080 | 22840 | 46.05 | 71.35 | 23936 | 50009 | 83948 | 20.80 | 154.5 | 2.41 | 0 | 12.00 | 217.9 | 1.98 |
| 64K | 1 | class-fresh | 475.0 | 115.2 | 81.67 | 15859 | 15852 | 72.73 | 8309 | 41694 | 66700 | 23.75 | 82.80 | 1.29 | 0 | 0 | 0.000 | 2.26 |
| 64K | 8 | today | 443.0 | 158714 | 0.080 | 13675 | 52.06 | 85.32 | 31886 | 225490 | 1937086 | 21.91 | 4190 | 65.47 | 0 | 362.0 | 467.3 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 75149 | 0.080 | 23987 | 34.23 | 69.61 | 41778 | 141730 | 325263 | 58.17 | 150.8 | 2.36 | 0 | 468.0 | 276.1 | 2.65 |
| 64K | 8 | class-fresh | 600.0 | 299.0 | 196.0 | 16838 | 17202 | 123.4 | 24899 | 58449 | 75900 | 140.0 | 75.41 | 1.18 | 0 | 0 | 0.000 | 6.39 |
| 256K | 1 | today | 248.0 | 73.05 | 0.091 | 17973 | 48.06 | 75.71 | 26034 | 58406 | 208532 | 12.39 | 4381 | 17.11 | 0 | 62.00 | 500.7 | 1.00 |
| 256K | 1 | class-pool | 319.0 | 80.61 | 0.080 | 23494 | 40.17 | 74.53 | 31763 | 58353 | 141970 | 15.86 | 542.7 | 2.12 | 0 | 31.00 | 267.4 | 1.28 |
| 256K | 1 | class-fresh | 465.0 | 114.7 | 80.79 | 16431 | 15771 | 73.08 | 8310 | 41701 | 58383 | 23.22 | 276.4 | 1.08 | 0 | 0 | 0.000 | 1.87 |
| 256K | 8 | today | 433.0 | 216397 | 0.080 | 40322 | 53.38 | 78.95 | 98927 | 391741 | 750620 | 21.52 | 4381 | 17.11 | 0 | 343.0 | 400.5 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 91827 | 0.080 | 31844 | 34.91 | 69.39 | 51345 | 175256 | 275420 | 49.18 | 532.1 | 2.08 | 0 | 399.0 | 392.4 | 2.29 |
| 256K | 8 | class-fresh | 512.0 | 307.5 | 196.0 | 24534 | 24563 | 127.9 | 24866 | 74988 | 416682 | 99.26 | 270.1 | 1.05 | 0 | 0 | 0.000 | 4.61 |
| 1M | 1 | today | 128.0 | 67.47 | 0.080 | 34790 | 48.85 | 73.88 | 51222 | 91710 | 316912 | 9.31 | 5155 | 5.03 | 0 | 18.00 | 592.4 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 72.04 | 0.080 | 44230 | 48.18 | 77.28 | 29167 | 75036 | 208523 | 11.99 | 2073 | 2.02 | 0 | 12.00 | 409.6 | 1.29 |
| 1M | 1 | class-fresh | 128.0 | 113.7 | 72.65 | 20973 | 15211 | 71.12 | 8312 | 41705 | 66677 | 21.89 | 1039 | 1.01 | 0 | 0 | 0.000 | 2.35 |
| 1M | 8 | today | 128.0 | 249739 | 0.090 | 57490 | 54.29 | 74.23 | 87959 | 400074 | 734201 | 20.56 | 5143 | 5.02 | 0 | 98.00 | 417.3 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 108766 | 0.080 | 46104 | 47.87 | 97.29 | 52714 | 216721 | 333873 | 41.01 | 2066 | 2.02 | 0 | 97.00 | 250.3 | 1.99 |
| 1M | 8 | class-fresh | 128.0 | 319.1 | 167.5 | 40573 | 43608 | 138.2 | 24795 | 100200 | 141594 | 76.68 | 1034 | 1.01 | 0 | 0 | 0.000 | 3.73 |
| 4M | 1 | today | 32.00 | 78.25 | 0.080 | 57536 | 52.81 | 94.39 | 35371 | 108402 | 300186 | 8.15 | 8216 | 2.01 | 0 | 4.00 | 776.0 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 74.56 | 0.080 | 55944 | 49.80 | 79.92 | 47804 | 108339 | 241875 | 9.97 | 8220 | 2.01 | 0 | 1.00 | 542.6 | 1.22 |
| 4M | 1 | class-fresh | 32.00 | 116.4 | 73.84 | 29015 | 12848 | 74.53 | 8306 | 50016 | 66697 | 19.79 | 4108 | 1.00 | 0 | 0 | 0.000 | 2.43 |
| 4M | 8 | today | 32.00 | 250253 | 0.060 | 174594 | 54.37 | 77.94 | 70986 | 508742 | 566558 | 18.63 | 8210 | 2.00 | 0 | 20.00 | 550.6 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 234189 | 0.080 | 135698 | 54.33 | 77.41 | 90324 | 441929 | 884097 | 18.11 | 8212 | 2.00 | 0 | 20.00 | 467.6 | 0.972 |
| 4M | 8 | class-fresh | 32.00 | 367.4 | 168.1 | 143517 | 128402 | 121.3 | 16752 | 275079 | 291088 | 29.09 | 4104 | 1.00 | 0 | 0 | 0.000 | 1.56 |

