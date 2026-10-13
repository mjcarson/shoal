# X2 lookups

host titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · build znver1 · cpu 2 · 4 placement groups a tablet · median of 5 runs of at least 200 ms

| shape | pool | slices | domains | build the view µs | window | rendezvous | rendezvous, libm ln | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | a table's read |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab-2 | p r3/host | 6 | 3 | 0.9 | 29 | 132 | 195 | 345 | 163 | 308 | 382 | 414 | 4.4 |
| lab-2 | p 2+1/host | 6 | 3 | 0.9 | 29 | 132 | 195 | 345 | 163 | 308 | 381 | 413 | 4.4 |
| lab-2 | p 4+2/device | 6 | 6 | 1.2 | 45 | 155 | 216 | 917 | 174 | 1022 | 1032 | 1044 | 4.4 |
| lab-fitted | p r3/host | 4 | 3 | 0.5 | 32 | 101 | 142 | 331 | 119 | 324 | 350 | 367 | 4.4 |
| lab-fitted | p 2+1/host | 4 | 3 | 0.5 | 32 | 101 | 142 | 331 | 119 | 324 | 349 | 366 | 4.4 |
| lab-fitted | p 3+1/device | 4 | 4 | 0.5 | 35 | 110 | 149 | 503 | 124 | 565 | 498 | 514 | 4.4 |
| 6x12 | p r3/host | 72 | 6 | 5.9 | 26 | 847 | 1609 | 2102 | 427 | 579 | 1119 | 725 | 4.4 |
| 6x12 | p 4+2/host | 72 | 6 | 7.0 | 42 | 857 | 1617 | 8938 | 724 | 1717 | 1756 | 1618 | 4.4 |
| 6x12 | p 8+3/device | 72 | 72 | 11.6 | 80 | 1363 | 2086 | 6517 | 1322 | 6838 | 4292 | 4264 | 4.4 |
| 50x24 | p r3/host | 1200 | 50 | 94.5 | 28 | 11968 | 23732 | 27478 | 1160 | 1900 | 12258 | 1432 | 4.4 |
| 50x24 | p 8+3/host | 1200 | 50 | 111.3 | 73 | 12359 | 24113 | 112134 | 3040 | 7580 | 15292 | 5987 | 4.4 |
| 50x24 | p 10+4/host | 1200 | 50 | 119.4 | 113 | 12453 | 24239 | 149137 | 3687 | 9947 | 17257 | 8487 | 4.4 |
| 50x24-slices | p 8+3/host | 4800 | 50 | 306.6 | 72 | 40052 | 88251 | 453769 | 3211 | 7859 | 43013 | 6200 | 4.4 |
| 50x24-slices | p 10+4/host | 4800 | 50 | 333.4 | 113 | 40213 | 88337 | 605060 | 3912 | 10305 | 45017 | 8756 | 4.4 |

Every lookup cell is nanoseconds for one placement group's whole answer.
