shoal-spike fanout: topology fanout and report traffic, Q13 at M3
host europa · governor performance · json bodies as the wire carries them

## Topology frame: encoded bytes and encode time per version (median of 200)

| members | tables | frame bytes | encode µs |
| --- | --- | --- | --- |
| 3 | 1 | 1052 | 0.8 |
| 3 | 4 | 1150 | 0.8 |
| 3 | 16 | 1547 | 1.0 |
| 3 | 64 | 3153 | 1.5 |
| 8 | 1 | 2283 | 1.7 |
| 8 | 4 | 2381 | 1.7 |
| 8 | 16 | 2778 | 1.8 |
| 8 | 64 | 4384 | 2.4 |
| 16 | 1 | 4251 | 3.1 |
| 16 | 4 | 4349 | 3.0 |
| 16 | 16 | 4746 | 3.2 |
| 16 | 64 | 6352 | 3.7 |
| 32 | 1 | 8187 | 5.6 |
| 32 | 4 | 8285 | 5.7 |
| 32 | 16 | 8682 | 5.9 |
| 32 | 64 | 10288 | 6.5 |
| 64 | 1 | 16060 | 11.2 |
| 64 | 4 | 16158 | 11.2 |
| 64 | 16 | 16555 | 11.5 |
| 64 | 64 | 18161 | 12.0 |

## Push of one version, 64 members and 16 tables, per subscriber count

| subscribers | bytes written | µs per version (encode once, copy per subscriber) |
| --- | --- | --- |
| 1 | 16555 | 11.5 |
| 100 | 1655500 | 484.6 |
| 1000 | 16555000 | 5046.9 |

## Topology frame with configured sets and moves (F45), 64 members and 16 tables

| configured sets | moves | frame bytes | encode µs |
| --- | --- | --- | --- |
| 0 | 0 | 16555 | 10.9 |
| 16 | 0 | 24896 | 17.1 |
| 64 | 0 | 50004 | 35.4 |
| 64 | 8 | 54075 | 38.3 |

## Pool map frame (X2), JSON as the topology frame is pushed

| shape | devices | slices | frame bytes | over today's tablet frame | encode µs | push to 1 µs | to 100 µs | to 1000 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab-1 | 3 | 3 | 1658 | 0.10× | 1.3 | 1.4 | 4.9 | 442.9 |
| lab-2 | 6 | 6 | 2747 | 0.17× | 2.0 | 2.0 | 10.3 | 759.0 |
| lab-fitted | 4 | 4 | 2150 | 0.13× | 1.6 | 1.6 | 5.2 | 585.9 |
| 6x12 | 72 | 72 | 22367 | 1.35× | 22.6 | 23.2 | 692.3 | 7692.4 |
| 6x12-mixed | 72 | 72 | 22252 | 1.34× | 14.2 | 14.4 | 678.7 | 7371.2 |
| 6x12-uneven | 72 | 72 | 22434 | 1.36× | 14.2 | 14.6 | 628.9 | 7060.9 |
| 6x12-slices | 72 | 180 | 29315 | 1.77× | 18.5 | 19.1 | 879.5 | 9725.9 |
| 50x24 | 1200 | 1200 | 361199 | 21.82× | 236.3 | 244.2 | 12338.7 | 128696.2 |
| two-classes | 72 | 96 | 23867 | 1.44× | 15.1 | 15.3 | 643.6 | 7109.0 |
| 50x24-slices | 1200 | 4800 | 598591 | 36.16× | 389.3 | 398.9 | 19855.1 | 211037.7 |

## What one record adds

| record | bytes |
| --- | --- |
| a device of one slice | 301 |
| a slice beyond a device's first | 64 |
| a change kept | 97 |
| a move listed | 89 |
| an exception | 115 |

## A busy pool map

Frame bytes. `pending listed` is the alternative to keeping a generation: every placement group a device added moves, at 4 groups a tablet, for one consumer and for a hundred.

| shape | devices alone | 64 changes kept | a move a device in flight | 1,000 exceptions | 10,000 exceptions | pending listed, 1 consumer | pending listed, 100 consumers |
| --- | --- | --- | --- | --- | --- | --- | --- |
| lab-2 | 2747 | 8947 | 3283 | 117866 | 1154173 | 1251273 (14007 groups) | 124841943 |
| 6x12 | 22367 | 28631 | 28780 | 137474 | 1173737 | 242496 (2470 groups) | 22036181 |
| 50x24 | 361199 | 367463 | 468169 | 476344 | 1512569 | 380272 (214 groups) | 2268596 |

## Status reports at a 500 ms interval

| members | report bytes | reports/s at the leader | bytes/s in at the leader | stats bytes | stats bytes/s in at the leader |
| --- | --- | --- | --- | --- | --- |
| 3 | 323 | 4 | 1292 | 8208 | 9177 |
| 8 | 548 | 14 | 7672 | 8433 | 35270 |
| 16 | 908 | 30 | 27240 | 8793 | 86378 |
| 32 | 1628 | 62 | 100936 | 9513 | 223154 |
| 64 | 3068 | 126 | 386568 | 10953 | 634946 |
