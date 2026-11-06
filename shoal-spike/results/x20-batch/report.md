| Round | Carried | The scan | Rows a second | MiB a second carried | A tablet, p50 / max | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | keys | 2.39 s | 1,253,808 | 9.6 | 0.70 / 0.90 s | 0 |
| 2 | keys | 2.36 s | 1,270,374 | 9.7 | 0.68 / 0.88 s | 0 |
| 3 | keys | 2.39 s | 1,252,628 | 9.6 | 0.68 / 0.89 s | 0 |
| 4 | keys | 2.40 s | 1,251,477 | 9.5 | 0.68 / 0.88 s | 0 |

| Round | Carried | Kind | p50 before → during | p99 before → during | Rate during over before | p99 during over before |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | keys | read | 0.54 → 0.71 ms | 3.09 → 6.71 ms | 0.59 | 2.17 |
| 1 | keys | insert | 2.51 → 4.52 ms | 5.68 → 12.79 ms | 0.58 | 2.25 |
| 2 | keys | read | 0.54 → 0.70 ms | 3.01 → 6.61 ms | 0.57 | 2.19 |
| 2 | keys | insert | 2.49 → 4.73 ms | 5.61 → 12.20 ms | 0.57 | 2.18 |
| 3 | keys | read | 0.55 → 0.70 ms | 3.13 → 6.36 ms | 0.60 | 2.03 |
| 3 | keys | insert | 2.51 → 4.56 ms | 5.59 → 12.73 ms | 0.58 | 2.28 |
| 4 | keys | read | 0.53 → 0.72 ms | 3.15 → 6.40 ms | 0.57 | 2.03 |
| 4 | keys | insert | 2.49 → 4.63 ms | 5.54 → 14.05 ms | 0.57 | 2.54 |

| Carried | Rounds | The scan | Read p99 during over before | Insert p99 during over before | The cell's rate during over before |
| --- | --- | --- | --- | --- | --- |
| keys | 4 | 2.39 [2.36, 2.40] s | 2.10 [2.03, 2.19] | 2.27 [2.18, 2.54] | 0.58 [0.57, 0.59] |

| Kind | Rounds | A second | p50 | p99 | p999 |
| --- | --- | --- | --- | --- | --- |
| read | 4 | 1128 [1122, 1134] | 0.54 [0.53, 0.55] ms | 3.11 [3.01, 3.15] ms | 4.52 [4.46, 4.60] ms |
| insert | 4 | 1124 [1119, 1130] | 2.50 [2.49, 2.51] ms | 5.60 [5.54, 5.68] ms | 6.92 [6.89, 6.97] ms |
