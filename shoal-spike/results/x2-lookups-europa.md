# X2 lookups

host europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · build znver1 · cpu 8 · 4 placement groups a tablet · median of 5 runs of at least 200 ms

| shape | pool | slices | domains | build the view µs | window | rendezvous | rendezvous, libm ln | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | a table's read |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab-2 | p r3/host | 6 | 3 | 0.3 | 10 | 64 | 84 | 142 | 60 | 130 | 174 | 172 | 2.4 |
| lab-2 | p 2+1/host | 6 | 3 | 0.3 | 9 | 64 | 84 | 142 | 60 | 132 | 173 | 171 | 2.4 |
| lab-2 | p 4+2/device | 6 | 6 | 0.4 | 15 | 73 | 87 | 370 | 79 | 391 | 521 | 529 | 2.4 |
| lab-fitted | p r3/host | 4 | 3 | 0.2 | 9 | 42 | 54 | 136 | 46 | 137 | 152 | 157 | 2.4 |
| lab-fitted | p 2+1/host | 4 | 3 | 0.2 | 9 | 43 | 54 | 136 | 46 | 137 | 152 | 158 | 2.4 |
| lab-fitted | p 3+1/device | 4 | 4 | 0.2 | 10 | 45 | 55 | 205 | 48 | 218 | 251 | 253 | 2.4 |
| 6x12 | p r3/host | 72 | 6 | 2.0 | 9 | 450 | 736 | 748 | 160 | 225 | 566 | 272 | 2.4 |
| 6x12 | p 4+2/host | 72 | 6 | 2.1 | 15 | 454 | 743 | 3314 | 260 | 653 | 918 | 721 | 2.4 |
| 6x12 | p 8+3/device | 72 | 72 | 3.4 | 29 | 663 | 866 | 2339 | 632 | 2384 | 2174 | 2149 | 2.4 |
| 50x24 | p r3/host | 1200 | 50 | 33.9 | 9 | 6002 | 9854 | 9976 | 451 | 678 | 6130 | 566 | 2.4 |
| 50x24 | p 8+3/host | 1200 | 50 | 38.3 | 29 | 6052 | 10093 | 41639 | 1203 | 2745 | 7637 | 2735 | 2.4 |
| 50x24 | p 10+4/host | 1200 | 50 | 40.6 | 40 | 6121 | 10160 | 55712 | 1470 | 3639 | 8699 | 4010 | 2.4 |
| 50x24-slices | p 8+3/host | 4800 | 50 | 108.2 | 29 | 16584 | 30529 | 167079 | 1328 | 3019 | 18192 | 2872 | 2.4 |
| 50x24-slices | p 10+4/host | 4800 | 50 | 116.1 | 40 | 16653 | 30567 | 222087 | 1623 | 3957 | 19262 | 4175 | 2.4 |

Every lookup cell is nanoseconds for one placement group's whole answer.
