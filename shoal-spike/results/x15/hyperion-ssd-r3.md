### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 865.9 µs |
| fdatasync of a clean file | p50 113.2 µs, p99 127.4 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2139 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2124 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.23 µs, p99 33.56 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 3 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 192.5 | 0.081 | 509.1 | 2329 | 72.53 | 6036 | 9071 | 31820 | 80.43 | 4186 | 65.40 | 4.26 | 110.0 | 62.65 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 85.85 | 0.080 | 157.3 | 2900 | 74.51 | 7089 | 8628 | 13698 | 115.5 | 149.1 | 2.33 | 4.51 | 0 | 4.15 | 1.44 |
| 64K | 1 | class-fresh | 600.0 | 107.8 | 78.45 | 192.5 | 3017 | 64.02 | 2072 | 5553 | 8763 | 175.7 | 78.10 | 1.22 | 2.00 | 0 | 0.000 | 2.18 |
| 64K | 8 | today | 600.0 | 39980 | 0.080 | 2111 | 6753 | 83.69 | 19253 | 70782 | 89673 | 125.1 | 4187 | 65.42 | 2.24 | 474.0 | 71.35 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 13034 | 0.090 | 388.3 | 3268 | 99.40 | 9315 | 26736 | 46204 | 314.0 | 145.9 | 2.28 | 1.94 | 458.0 | 40.57 | 2.51 |
| 64K | 8 | class-fresh | 600.0 | 243.5 | 183.0 | 421.2 | 5637 | 90.80 | 7371 | 14539 | 21076 | 542.6 | 74.45 | 1.16 | 1.00 | 0 | 0.000 | 4.34 |
| 256K | 1 | today | 512.0 | 156.3 | 0.081 | 1462 | 3008 | 73.51 | 6187 | 11040 | 31373 | 78.23 | 4382 | 17.12 | 4.41 | 71.00 | 64.96 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 80.13 | 0.080 | 394.3 | 3061 | 84.82 | 6344 | 10795 | 15316 | 105.1 | 533.2 | 2.08 | 4.45 | 0 | 3.13 | 1.34 |
| 256K | 1 | class-fresh | 512.0 | 108.4 | 79.81 | 426.1 | 3163 | 64.41 | 2065 | 5920 | 9254 | 165.0 | 270.5 | 1.06 | 2.00 | 0 | 0.000 | 2.11 |
| 256K | 8 | today | 512.0 | 41110 | 0.081 | 2400 | 8734 | 83.00 | 18684 | 72571 | 93853 | 121.9 | 4378 | 17.10 | 2.30 | 403.0 | 72.66 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12792 | 0.090 | 772.6 | 3450 | 85.76 | 9057 | 26328 | 38704 | 318.5 | 531.8 | 2.08 | 2.02 | 390.0 | 39.54 | 2.61 |
| 256K | 8 | class-fresh | 512.0 | 255.0 | 176.2 | 1084 | 5714 | 85.85 | 6560 | 14414 | 23925 | 532.3 | 268.5 | 1.05 | 1.03 | 0 | 0.000 | 4.37 |
| 1M | 1 | today | 128.0 | 189.8 | 0.080 | 2424 | 2471 | 73.28 | 6284 | 12031 | 30022 | 76.16 | 5143 | 5.02 | 4.42 | 15.00 | 60.57 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 60.90 | 0.070 | 1359 | 2860 | 129.8 | 6184 | 8922 | 14517 | 103.2 | 2073 | 2.02 | 4.46 | 0 | 14.53 | 1.35 |
| 1M | 1 | class-fresh | 128.0 | 107.9 | 79.83 | 1378 | 2904 | 65.12 | 2075 | 6623 | 12424 | 144.2 | 1037 | 1.01 | 2.00 | 0 | 0.000 | 1.89 |
| 1M | 8 | today | 128.0 | 43521 | 0.080 | 4614 | 11783 | 91.29 | 15308 | 79541 | 98671 | 113.5 | 5143 | 5.02 | 2.20 | 96.00 | 55.75 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 21497 | 0.081 | 3129 | 7502 | 108.0 | 11086 | 42611 | 56844 | 201.3 | 2066 | 2.02 | 1.94 | 94.00 | 40.99 | 1.77 |
| 1M | 8 | class-fresh | 128.0 | 280.4 | 168.9 | 4864 | 10228 | 99.31 | 5965 | 22330 | 31617 | 356.8 | 1035 | 1.01 | 0.922 | 0 | 0.000 | 3.14 |
| 4M | 1 | today | 32.00 | 84.73 | 0.081 | 5513 | 2867 | 73.34 | 6969 | 15811 | 32931 | 62.10 | 8213 | 2.01 | 4.50 | 1.00 | 59.45 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 82.44 | 0.090 | 5509 | 3014 | 73.12 | 7175 | 16782 | 22597 | 59.74 | 8217 | 2.01 | 4.78 | 0 | 50.29 | 0.962 |
| 4M | 1 | class-fresh | 32.00 | 106.7 | 75.74 | 5280 | 2932 | 65.67 | 2055 | 10517 | 12134 | 93.84 | 4108 | 1.00 | 2.00 | 0 | 0.000 | 1.51 |
| 4M | 8 | today | 32.00 | 58846 | 0.080 | 12396 | 16762 | 106.8 | 18376 | 91578 | 150352 | 82.24 | 8214 | 2.01 | 2.19 | 21.00 | 59.92 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 58095 | 0.070 | 13544 | 16289 | 98.09 | 8201 | 102194 | 146136 | 85.36 | 8292 | 2.02 | 2.09 | 21.00 | 57.74 | 1.04 |
| 4M | 8 | class-fresh | 32.00 | 540.2 | 172.9 | 23797 | 25457 | 102.8 | 5561 | 50483 | 53326 | 157.6 | 4103 | 1.00 | 0.812 | 0 | 0.000 | 1.92 |

