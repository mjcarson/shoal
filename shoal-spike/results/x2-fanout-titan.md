shoal-spike fanout: topology fanout and report traffic, Q13 at M3
host titan · governor performance · json bodies as the wire carries them

## Topology frame: encoded bytes and encode time per version (median of 200)

| members | tables | frame bytes | encode µs |
| --- | --- | --- | --- |
| 3 | 1 | 1052 | 2.2 |
| 3 | 4 | 1150 | 2.4 |
| 3 | 16 | 1547 | 2.7 |
| 3 | 64 | 3153 | 4.3 |
| 8 | 1 | 2283 | 4.5 |
| 8 | 4 | 2381 | 4.6 |
| 8 | 16 | 2778 | 4.9 |
| 8 | 64 | 4384 | 6.6 |
| 16 | 1 | 4251 | 8.3 |
| 16 | 4 | 4349 | 8.4 |
| 16 | 16 | 4746 | 8.8 |
| 16 | 64 | 6352 | 10.2 |
| 32 | 1 | 8187 | 15.4 |
| 32 | 4 | 8285 | 15.8 |
| 32 | 16 | 8682 | 16.2 |
| 32 | 64 | 10288 | 17.6 |
| 64 | 1 | 16060 | 28.7 |
| 64 | 4 | 16158 | 28.8 |
| 64 | 16 | 16555 | 30.0 |
| 64 | 64 | 18161 | 31.3 |

## Push of one version, 64 members and 16 tables, per subscriber count

| subscribers | bytes written | µs per version (encode once, copy per subscriber) |
| --- | --- | --- |
| 1 | 16555 | 30.4 |
| 100 | 1655500 | 928.7 |
| 1000 | 16555000 | 11186.8 |

## Topology frame with configured sets and moves (F45), 64 members and 16 tables

| configured sets | moves | frame bytes | encode µs |
| --- | --- | --- | --- |
| 0 | 0 | 16555 | 29.5 |
| 16 | 0 | 24896 | 43.8 |
| 64 | 0 | 50004 | 88.2 |
| 64 | 8 | 54075 | 95.7 |

## Pool map frame (X2), JSON as the topology frame is pushed

| shape | devices | slices | frame bytes | over today's tablet frame | encode µs | push to 1 µs | to 100 µs | to 1000 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab-1 | 3 | 3 | 1658 | 0.10× | 3.3 | 3.4 | 15.1 | 867.7 |
| lab-2 | 6 | 6 | 2747 | 0.17× | 5.2 | 5.4 | 27.7 | 1563.9 |
| lab-fitted | 4 | 4 | 2150 | 0.13× | 4.2 | 4.4 | 16.9 | 1171.5 |
| 6x12 | 72 | 72 | 22367 | 1.35× | 38.0 | 38.8 | 1239.1 | 15432.7 |
| 6x12-mixed | 72 | 72 | 22252 | 1.34× | 37.9 | 38.7 | 1230.6 | 15392.4 |
| 6x12-uneven | 72 | 72 | 22434 | 1.36× | 38.1 | 39.1 | 1242.3 | 15491.7 |
| 6x12-slices | 72 | 180 | 29315 | 1.77× | 50.1 | 51.3 | 1708.4 | 20366.2 |
| 50x24 | 1200 | 1200 | 361199 | 21.82× | 602.0 | 614.9 | 24360.5 | 247102.2 |
| two-classes | 72 | 96 | 23867 | 1.44× | 40.6 | 41.5 | 1016.1 | 16147.9 |
| 50x24-slices | 1200 | 4800 | 598591 | 36.16× | 1010.4 | 1032.8 | 40818.6 | 402729.4 |

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
