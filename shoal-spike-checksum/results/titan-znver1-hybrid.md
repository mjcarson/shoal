# titan, znver1 build with --features gxhash3-hybrid, the checks only

```
$ ./shoal-spike-checksum-znver1-hybrid check --quick --core 2
titan | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | znver1 +gxhash3-hybrid | compiled sse4.2,pclmulqdq,aes,avx2 | detected sse4.2,pclmulqdq,aes,avx2 | governor schedutil | core 2 | rustc 1.100.0-nightly (0ed41eb41 2026-09-04)
check crc32c
check crc-fast crc32c
check crc-fast crc64nvme
check crc64fast-nvme
check xxh3-64
check xxh3-128
check blake3
check gxhash 2
check gxhash 3
Illegal instruction (core dumped): exit 132
```

The same binary ran every check on europa, whose Zen4 has VAES, and gave the same digests as every other build.
