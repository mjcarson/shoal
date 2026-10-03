# X2 lookups

host hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · build znver1 · cpu 2 · 4 placement groups a tablet · median of 5 runs of at least 200 ms

| shape | pool | slices | domains | build the view µs | window | rendezvous | rendezvous, libm ln | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | a table's read |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab-2 | p r3/host | 6 | 3 | 0.9 | 29 | 132 | 196 | 344 | 164 | 307 | 381 | 413 | 4.4 |
| lab-2 | p 2+1/host | 6 | 3 | 0.9 | 29 | 132 | 195 | 344 | 163 | 307 | 380 | 413 | 4.4 |
| lab-2 | p 4+2/device | 6 | 6 | 1.2 | 45 | 156 | 217 | 915 | 175 | 1019 | 1036 | 1044 | 4.4 |
| lab-fitted | p r3/host | 4 | 3 | 0.5 | 32 | 101 | 142 | 330 | 119 | 323 | 347 | 366 | 4.4 |
| lab-fitted | p 2+1/host | 4 | 3 | 0.5 | 32 | 101 | 142 | 331 | 119 | 323 | 347 | 366 | 4.4 |
| lab-fitted | p 3+1/device | 4 | 4 | 0.5 | 35 | 111 | 149 | 503 | 123 | 564 | 496 | 508 | 4.4 |
| 6x12 | p r3/host | 72 | 6 | 6.4 | 26 | 850 | 1610 | 2094 | 428 | 579 | 1116 | 689 | 4.4 |
| 6x12 | p 4+2/host | 72 | 6 | 6.5 | 42 | 859 | 1618 | 8944 | 723 | 1715 | 1756 | 1618 | 4.4 |
| 6x12 | p 8+3/device | 72 | 72 | 11.3 | 80 | 1362 | 2084 | 6527 | 1320 | 6829 | 4304 | 4269 | 4.4 |
| 50x24 | p r3/host | 1200 | 50 | 93.1 | 28 | 11965 | 23750 | 27534 | 1159 | 1898 | 12282 | 1431 | 4.4 |
| 50x24 | p 8+3/host | 1200 | 50 | 112.4 | 73 | 12360 | 24135 | 111725 | 3043 | 7573 | 15298 | 5992 | 4.4 |
| 50x24 | p 10+4/host | 1200 | 50 | 119.8 | 113 | 12449 | 24230 | 148728 | 3689 | 9953 | 17249 | 8500 | 4.4 |
| 50x24-slices | p 8+3/host | 4800 | 50 | 308.7 | 72 | 40396 | 88290 | 455484 | 3210 | 7847 | 43396 | 6198 | 4.4 |
| 50x24-slices | p 10+4/host | 4800 | 50 | 335.8 | 112 | 40256 | 87915 | 605726 | 3904 | 10310 | 44939 | 8749 | 4.4 |

Every lookup cell is nanoseconds for one placement group's whole answer.
