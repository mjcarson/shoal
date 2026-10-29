### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1177 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.34 µs |
| fdatasync of a clean file | p50 10.86 µs, p99 16.60 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 73.98 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 75.44 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.51 µs, p99 16.47 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 49.51 | 78.89 | - | - | - |
| fold | 23.54 | 165.9 | - | - | - |
| crc+fold | 28.98 | 134.8 | - | - | - |
| copy | 28.44 | 137.3 | - | - | - |
| decode-21-data | 17.36 | 225.1 | - | - | - |
| decode-21-parity | 17.38 | 224.7 | - | - | - |
| decode-42-data | 9.28 | 421.2 | - | - | - |
| decode-42-parity | 9.31 | 419.5 | - | - | - |
| pipeline-copy | 21.28 | 183.6 | - | - | - |
| pipeline-21 | 8.77 | 445.2 | - | - | - |
| pipeline-42 | 4.58 | 853.2 | - | - | - |
| summary-42 | 5325 | 4.40 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 4 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 4184 | 0.236 | 0.276 | 68.00 | 256.0 | 0 | 0 |
| layout=4+2 | chunk | 111.6 | 8.91 | 9.26 | 4107 | 16421 | 0 | 0 |
| layout=2+1 | unit | 5587 | 0.176 | 0.214 | 68.00 | 128.0 | 0 | 0 |
| layout=2+1 | chunk | 184.2 | 5.38 | 5.75 | 4103 | 8201 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | cpu | 22379 | 0 | 18649 | 5362/5362 | 0 | 0.558 | 1.98 | 0.230 | 26.51 | 13.76 | 0.037 |
| layout=4+2 piece=4M | idle | 2208 | 0 | 2114 | 22/22 | 0 | 1.68 | 2.38 | 0.206 | 28.79 | 13.40 | 0.835 |
| layout=4+2 piece=256K | idle | 1872 | 0 | 1792 | 18/18 | 0 | 0.520 | 1.07 | 0.203 | 28.42 | 12.99 | 0.804 |
| layout=4+2 piece=1M | unbounded | 2243 | 0 | 2147 | 22/22 | 0 | 1.68 | 3.00 | 0.209 | 28.62 | 13.02 | 0.841 |
| layout=4+2 piece=1M | idle-ceil-200 | 200.0 | 0 | 191.0 | 2/2 | 0 | 0.422 | 0.484 | 0.033 | 28.54 | 13.59 | 0.115 |
| layout=4+2 piece=1M | idle | 2123 | 0 | 2031 | 21/21 | 0 | 0.548 | 1.20 | 0.206 | 28.41 | 12.96 | 0.832 |
| layout=4+2 piece=1M | fixed-400 | 398.8 | 0.997 | 381.0 | 4/4 | 0 | 1.53 | 1.99 | 0.132 | 28.67 | 12.95 | 0.179 |
| layout=4+2 piece=1M | fixed-200 | 200.0 | 1.000 | 191.0 | 2/2 | 0 | 1.44 | 1.60 | 0.060 | 28.63 | 13.01 | 0.112 |
| layout=4+2 piece=1M | fixed-100 | 99.90 | 0.999 | 95.00 | 2/2 | 0 | 1.25 | 1.49 | 0.028 | 28.85 | 13.03 | 0.076 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 0.752 | 1.24 | 0.027 | 28.73 | 12.99 | 0.061 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 0.085 | 0.163 | 0.025 | 0 | 0 | 0.044 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 488.2 | 2441 | 0 | 32.72 | 5.50 | 11.14 | 6.57 | 1.14 | 0 | 0.889 |
| role=local layout=4+2 piece=1M | idle | 407.1 | 2037 | 0 | 9.81 | 0.560 | 1.21 | 0.739 | 0.203 | 0 | 0.806 |
| role=local layout=2+1 piece=1M | unbounded | 786.8 | 2363 | 0 | 20.31 | 5.02 | 8.51 | 5.02 | 0.551 | 0 | 0.880 |
| role=local layout=2+1 piece=1M | idle | 668.6 | 2008 | 0 | 5.97 | 0.585 | 0.859 | 0.711 | 0.193 | 0 | 0.808 |
| role=local layout=copy piece=1M | unbounded | 1170 | 2342 | 0 | 13.66 | 6.49 | 11.05 | 6.51 | 0.478 | 0 | 0.890 |
| role=local layout=copy piece=1M | idle | 986.2 | 1974 | 0 | 4.03 | 0.605 | 0.714 | 0.656 | 0.185 | 0 | 0.815 |
| role=dest layout=4+2 piece=1M | unbounded | 1867 | 1869 | 0 | 6.99 | 5.97 | 8.42 | 6.13 | 0.323 | 0 | 0.725 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 399.6 | 400.0 | 0 | 10.01 | 0.533 | 1.10 | 1.04 | 0.209 | 0 | 0.195 |
| role=dest layout=4+2 piece=1M | idle | 1300 | 1301 | 0 | 3.05 | 0.554 | 1.11 | 1.05 | 0.220 | 0 | 0.578 |
| role=dest layout=4+2 piece=1M | fixed-800 | 724.2 | 724.9 | 0.906 | 5.51 | 1.34 | 1.87 | 1.35 | 0.217 | 0 | 0.324 |
| role=dest layout=4+2 piece=1M | fixed-400 | 399.4 | 399.8 | 1.000 | 10.01 | 1.30 | 1.37 | 1.30 | 0.210 | 0 | 0.201 |
| role=dest layout=4+2 piece=1M | fixed-200 | 199.7 | 199.8 | 0.999 | 20.02 | 1.21 | 1.29 | 1.21 | 0.195 | 0 | 0.118 |
| role=dest layout=4+2 piece=1M | fixed-100 | 99.80 | 99.90 | 0.999 | 40.04 | 0.983 | 1.14 | 1.06 | 0.168 | 0 | 0.078 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 0.087 | 0.163 | 0.096 | 0.024 | 0 | 0.046 |

