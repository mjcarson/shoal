# F85 object load report

Rounds 1 to 8, 8 in all, from `shoal-spike/results/object-load/raw`. Every figure is the median of
the rounds with the lowest and highest in brackets, in microseconds unless it says otherwise. A
ratio is the side's median over the cell alone's; *above* and *below* mean the two sides' run
intervals are disjoint, *within* that they overlap. S15's line is 1.25×, judged on the null leg.

## The null leg: the cell

| Side | Rate MiB/s | Read p50 | Read p99 | Read p99 ÷ alone | Write p50 | Write p99 | Write p99 ÷ alone | Ops/s |
| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | ---: |
| alone | - | 183 [173–193] | 435 [423–475] | - | 7,127 [6,892–7,280] | 15,377 [14,143–16,435] | - | 8,296 [8,111–8,464] |
| beside an idle object executor | - | 176 [172–185] | 415 [394–447] | 0.95×, within | 7,073 [6,998–7,195] | 14,079 [13,060–15,360] | 0.92×, within | 8,406 [8,252–8,515] |
| beside the store's load | 100 | 176 [165–182] | 422 [396–598] | 0.97×, within | 7,077 [6,918–7,160] | 15,564 [14,183–17,780] | 1.01×, within | 8,351 [8,152–8,469] |
| beside the store's load | 500 | 184 [181–200] | 458 [451–551] | 1.05×, within | 7,193 [7,047–7,250] | 15,098 [13,508–16,441] | 0.98×, within | 8,199 [8,157–8,280] |

## The null leg: the load in the measured window

| Rate asked | MiB/s reached | Chunks | Stage p50 | Stage p99 | Apply p50 | Apply p99 | Behind max ms | Refused | Served on | Driver on |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 100 | 99.9 [99.8–100.3] | 240 [236–245] | 1,280 [1,200–1,293] | 4,279 [3,992–4,382] | 1,619 [1,517–1,639] | 3,842 [3,750–4,880] | 2.0 [2.0–2.9] | 0 | [1] | 0 |
| 500 | 407.3 [404.1–411.2] | 992 [977–1,003] | 920 [914–924] | 4,974 [4,921–5,107] | 898 [892–914] | 3,514 [3,446–3,612] | 50.0 [50.0–50.0] | 0 | [1] | 0 |

## The evo leg (the device shares the tables' 970 EVO): the cell

| Side | Rate MiB/s | Read p50 | Read p99 | Read p99 ÷ alone | Write p50 | Write p99 | Write p99 ÷ alone | Ops/s |
| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | ---: |
| alone | - | 182 [175–189] | 441 [404–474] | - | 7,123 [7,075–7,277] | 15,188 [13,517–16,117] | - | 8,268 [8,123–8,320] |
| beside an idle object executor | - | 185 [175–191] | 459 [420–635] | 1.04×, within | 7,110 [7,006–7,256] | 15,165 [13,644–16,382] | 1.00×, within | 8,332 [8,117–8,424] |
| beside the store's load | 100 | 198 [194–205] | 494 [468–525] | 1.12×, within | 10,540 [10,378–10,812] | 20,669 [19,533–23,089] | **1.36×**, above | 5,689 [5,506–5,787] |

## The evo leg: the load in the measured window

| Rate asked | MiB/s reached | Chunks | Stage p50 | Stage p99 | Apply p50 | Apply p99 | Behind max ms | Refused | Served on | Driver on |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 100 | 37.3 [37.0–37.8] | 131 [129–137] | 7,144 [7,004–7,413] | 12,326 [11,221–14,675] | 18,797 [18,554–18,914] | 29,436 [26,119–30,720] | 50.0 [50.0–50.0] | 0 | [1] | 0 |

## The verdict on the null leg

| Side | Rate MiB/s | Fails on |
| --- | ---: | --- |
| beside an idle object executor | - | nothing: held |
| beside the store's load | 100 | nothing: held |
| beside the store's load | 500 | nothing: held |

