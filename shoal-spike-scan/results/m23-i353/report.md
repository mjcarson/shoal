# Item 353: long scan steps by shard, beside their cpus

Written by `m23-i353-report.py` from the captures and cpu samples under `target/lab/m23-i353/captures`. A step is counted past 100 µs on its own cpu as its node judges it; *faulted* is such a step that took a page fault inside it, and every other was held by its work or by the interrupts its cpu took, which a kernel without irq time accounting charges to the thread it interrupts. A rate is steps past a second of the steps' own cpu.

## Rounds

| Round | NIC | europa's NIC cpu (shards' cpus) | titan's | hyperion's |
| --- | --- | --- | --- | --- |
| 1 | as it was | 23 (1, 2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) |
| 2 | as it was | 23 (1, 2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) |
| 3 | moved to 7 | 23 (1, 2, 3, 4, 5, 6) | 7 (2, 3, 4, 5, 6) | 7 (2, 3, 4, 5, 6) |
| 4 | as it was | 23 (1, 2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) |
| 5 | as it was | 23 (1, 2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) | 5 (2, 3, 4, 5, 6) |
| 6 | moved to 7 | 23 (1, 2, 3, 4, 5, 6) | 7 (2, 3, 4, 5, 6) | 7 (2, 3, 4, 5, 6) |

## Steps past 100 µs on their own cpu, by shard

Each shard's steps of every round of the NIC state, a population's arms together, beside its cpu's figures averaged over the same arms. *Unfaulted a second* is the steps past 100 µs on their own cpu that took no fault, a second of the shard's step cpu, with an approximate 95% interval.

### Big

| Host | NIC | Shard cpu | The NIC's cpu | Steps | Step cpu (s) | Captures past (faulted) | Encodes past (faulted) | Unfaulted a second | Softirq % | Irq % | NET_RX/s | NIC irq/s | NVMe irq/s | TLB/s | CAL/s | Faults/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa | as it was | 1 |  | 222 | 0.00 | 0 (0) | 0 (0) | 0.00 [0.00–3849.06] | 0.11 | 0.00 | 264 | 0 | 1 | 3 | 3,659 | 78 |
| europa | as it was | 2 |  | 22,565 | 0.04 | 0 (0) | 6 (0) | 134.94 [26.97–242.92] | 0.11 | 0.00 | 248 | 0 | 971 | 3 | 3,190 | 117 |
| europa | as it was | 3 |  | 16,165 | 0.03 | 0 (0) | 1 (0) | 30.13 [0.00–89.19] | 0.11 | 0.00 | 231 | 0 | 956 | 3 | 3,228 | 120 |
| europa | as it was | 4 |  | 10,172 | 0.02 | 0 (0) | 1 (0) | 50.29 [0.00–148.85] | 0.07 | 0.00 | 152 | 0 | 906 | 3 | 3,082 | 102 |
| europa | as it was | 5 |  | 13,258 | 0.02 | 1 (0) | 2 (0) | 121.72 [0.00–259.46] | 0.10 | 0.00 | 213 | 0 | 909 | 3 | 3,103 | 101 |
| europa | as it was | 6 |  | 19,029 | 0.04 | 0 (0) | 2 (0) | 55.79 [0.00–133.11] | 0.12 | 0.00 | 255 | 0 | 947 | 3 | 3,244 | 89 |
| europa | moved | 1 |  | 3,933 | 0.01 | 0 (0) | 0 (0) | 0.00 [0.00–242.56] | 0.11 | 0.00 | 260 | 0 | 1 | 3 | 3,637 | 96 |
| europa | moved | 2 |  | 4,807 | 0.01 | 0 (0) | 0 (0) | 0.00 [0.00–209.99] | 0.15 | 0.00 | 354 | 0 | 918 | 3 | 3,139 | 98 |
| europa | moved | 3 |  | 17,157 | 0.03 | 1 (0) | 0 (0) | 31.78 [0.00–94.07] | 0.07 | 0.00 | 136 | 0 | 1,032 | 3 | 3,010 | 102 |
| europa | moved | 4 |  | 5,317 | 0.01 | 0 (0) | 1 (0) | 94.61 [0.00–280.05] | 0.09 | 0.00 | 169 | 0 | 927 | 3 | 3,103 | 95 |
| europa | moved | 5 |  | 6,978 | 0.01 | 0 (0) | 0 (0) | 0.00 [0.00–134.40] | 0.09 | 0.00 | 171 | 0 | 975 | 3 | 3,051 | 95 |
| europa | moved | 6 |  | 1,934 | 0.00 | 0 (0) | 0 (0) | 0.00 [0.00–565.06] | 0.08 | 0.00 | 181 | 0 | 882 | 3 | 3,061 | 79 |
| titan | as it was | 2 |  | 33,693 | 0.19 | 11 (0) | 2 (0) | 67.89 [30.99–104.80] | 0.05 | 0.00 | 17 | 0 | 530 | 24 | 1,499 | 296 |
| titan | as it was | 3 |  | 32,232 | 0.19 | 16 (0) | 4 (0) | 103.13 [57.93–148.33] | 0.06 | 0.00 | 18 | 0 | 515 | 23 | 1,466 | 314 |
| titan | as it was | 4 |  | 37,632 | 0.23 | 14 (0) | 7 (3) | 79.32 [42.67–115.96] | 0.05 | 0.00 | 16 | 0 | 503 | 24 | 1,467 | 329 |
| titan | as it was | 5 | yes | 28,788 | 0.19 | 8 (0) | 3 (0) | 58.91 [24.10–93.73] | 8.90 | 0.00 | 14,512 | 9,226 | 449 | 26 | 1,692 | 257 |
| titan | as it was | 6 |  | 33,011 | 0.19 | 12 (0) | 9 (3) | 92.93 [50.00–135.86] | 0.04 | 0.00 | 15 | 0 | 539 | 22 | 1,348 | 323 |
| titan | moved | 2 |  | 23,558 | 0.13 | 11 (0) | 3 (0) | 104.84 [49.92–159.76] | 0.05 | 0.00 | 19 | 0 | 571 | 25 | 1,618 | 273 |
| titan | moved | 3 |  | 14,546 | 0.08 | 5 (0) | 1 (0) | 70.95 [14.18–127.73] | 0.05 | 0.00 | 15 | 0 | 464 | 24 | 1,522 | 283 |
| titan | moved | 4 |  | 21,279 | 0.12 | 9 (0) | 4 (0) | 105.45 [48.13–162.78] | 0.05 | 0.00 | 17 | 0 | 518 | 24 | 1,531 | 318 |
| titan | moved | 5 |  | 22,081 | 0.13 | 6 (0) | 2 (0) | 63.37 [19.46–107.29] | 0.06 | 0.00 | 18 | 0 | 526 | 23 | 1,519 | 323 |
| titan | moved | 6 |  | 16,027 | 0.09 | 4 (0) | 3 (0) | 77.64 [20.12–135.16] | 0.05 | 0.00 | 17 | 0 | 480 | 24 | 1,523 | 282 |
| hyperion | as it was | 2 |  | 33,195 | 0.19 | 10 (0) | 2 (0) | 64.41 [27.97–100.86] | 0.05 | 0.00 | 17 | 0 | 552 | 14 | 1,563 | 295 |
| hyperion | as it was | 3 |  | 21,206 | 0.13 | 4 (0) | 3 (0) | 54.06 [14.01–94.11] | 0.05 | 0.00 | 18 | 0 | 470 | 13 | 1,546 | 277 |
| hyperion | as it was | 4 |  | 42,030 | 0.26 | 19 (0) | 10 (1) | 109.35 [68.84–149.85] | 0.05 | 0.00 | 16 | 0 | 515 | 13 | 1,512 | 312 |
| hyperion | as it was | 5 | yes | 26,234 | 0.18 | 10 (0) | 8 (0) | 99.31 [53.43–145.19] | 9.35 | 0.00 | 14,641 | 9,302 | 485 | 14 | 1,804 | 269 |
| hyperion | as it was | 6 |  | 34,997 | 0.19 | 10 (0) | 5 (0) | 77.83 [38.44–117.21] | 0.05 | 0.00 | 15 | 0 | 496 | 12 | 1,417 | 277 |
| hyperion | moved | 2 |  | 20,082 | 0.11 | 5 (0) | 2 (0) | 61.78 [16.01–107.54] | 0.05 | 0.00 | 18 | 0 | 538 | 16 | 1,677 | 305 |
| hyperion | moved | 3 |  | 16,182 | 0.09 | 9 (0) | 1 (0) | 109.37 [41.58–177.16] | 0.05 | 0.00 | 17 | 0 | 470 | 15 | 1,597 | 303 |
| hyperion | moved | 4 |  | 18,636 | 0.11 | 4 (0) | 3 (0) | 62.30 [16.15–108.45] | 0.05 | 0.00 | 18 | 0 | 524 | 15 | 1,590 | 290 |
| hyperion | moved | 5 |  | 21,723 | 0.13 | 6 (0) | 4 (0) | 79.58 [30.26–128.91] | 0.05 | 0.00 | 16 | 0 | 506 | 14 | 1,556 | 293 |
| hyperion | moved | 6 |  | 21,399 | 0.12 | 12 (0) | 2 (0) | 115.17 [54.84–175.50] | 0.06 | 0.00 | 19 | 0 | 519 | 16 | 1,641 | 279 |

### Keyword

| Host | NIC | Shard cpu | The NIC's cpu | Steps | Step cpu (s) | Captures past (faulted) | Encodes past (faulted) | Unfaulted a second | Softirq % | Irq % | NET_RX/s | NIC irq/s | NVMe irq/s | TLB/s | CAL/s | Faults/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa | as it was | 1 |  | 4,120 | 0.06 | 0 (0) | 6 (1) | 77.49 [9.57–145.41] | 0.11 | 0.00 | 244 | 0 | 1 | 3 | 3,448 | 101 |
| europa | as it was | 2 |  | 21,467 | 0.29 | 0 (0) | 29 (8) | 72.74 [41.63–103.86] | 0.10 | 0.00 | 221 | 0 | 1,043 | 3 | 3,030 | 107 |
| europa | as it was | 3 |  | 25,507 | 0.35 | 0 (0) | 50 (16) | 95.96 [63.71–128.22] | 0.08 | 0.00 | 161 | 0 | 1,057 | 3 | 2,931 | 109 |
| europa | as it was | 4 |  | 12,959 | 0.19 | 0 (0) | 18 (0) | 95.28 [51.26–139.29] | 0.11 | 0.00 | 216 | 0 | 1,016 | 3 | 3,007 | 105 |
| europa | as it was | 5 |  | 13,052 | 0.16 | 0 (0) | 20 (4) | 99.26 [50.62–147.89] | 0.13 | 0.00 | 267 | 0 | 1,024 | 3 | 3,060 | 100 |
| europa | as it was | 6 |  | 22,603 | 0.31 | 2 (0) | 47 (22) | 87.73 [54.64–120.82] | 0.09 | 0.00 | 191 | 0 | 1,042 | 3 | 3,072 | 107 |
| europa | moved | 1 |  | 7,359 | 0.09 | 0 (0) | 8 (2) | 63.31 [12.65–113.96] | 0.10 | 0.00 | 223 | 0 | 1 | 3 | 3,448 | 108 |
| europa | moved | 2 |  | 15,525 | 0.20 | 1 (0) | 34 (10) | 124.11 [75.46–172.77] | 0.08 | 0.00 | 195 | 0 | 1,060 | 3 | 2,961 | 127 |
| europa | moved | 3 |  | 12,660 | 0.16 | 0 (0) | 19 (2) | 103.09 [54.09–152.10] | 0.11 | 0.00 | 215 | 0 | 1,074 | 3 | 2,957 | 105 |
| europa | moved | 4 |  | 9,256 | 0.13 | 0 (0) | 15 (6) | 68.93 [23.89–113.96] | 0.11 | 0.00 | 217 | 0 | 1,053 | 3 | 2,980 | 105 |
| europa | moved | 5 |  | 7,032 | 0.10 | 1 (0) | 11 (1) | 113.71 [46.51–180.91] | 0.11 | 0.00 | 227 | 0 | 1,045 | 3 | 3,029 | 119 |
| europa | moved | 6 |  | 4,233 | 0.04 | 0 (0) | 5 (0) | 111.30 [13.74–208.86] | 0.08 | 0.00 | 169 | 0 | 1,047 | 3 | 2,982 | 98 |
| titan | as it was | 2 |  | 27,765 | 0.38 | 0 (0) | 13 (3) | 26.00 [9.88–42.11] | 0.05 | 0.00 | 17 | 0 | 688 | 19 | 1,446 | 195 |
| titan | as it was | 3 |  | 24,800 | 0.33 | 1 (0) | 11 (3) | 27.22 [9.44–45.00] | 0.04 | 0.00 | 16 | 0 | 667 | 19 | 1,377 | 218 |
| titan | as it was | 4 |  | 22,692 | 0.29 | 1 (0) | 6 (2) | 16.96 [2.09–31.82] | 0.04 | 0.00 | 14 | 0 | 667 | 20 | 1,361 | 211 |
| titan | as it was | 5 | yes | 30,540 | 0.46 | 1 (0) | 11 (2) | 21.55 [8.19–34.91] | 9.04 | 0.00 | 13,528 | 8,596 | 649 | 19 | 1,592 | 250 |
| titan | as it was | 6 |  | 26,678 | 0.32 | 0 (0) | 8 (0) | 24.85 [7.63–42.07] | 0.04 | 0.00 | 15 | 0 | 693 | 19 | 1,320 | 220 |
| titan | moved | 2 |  | 7,445 | 0.11 | 0 (0) | 3 (0) | 27.70 [0.00–59.04] | 0.05 | 0.00 | 17 | 0 | 680 | 20 | 1,466 | 214 |
| titan | moved | 3 |  | 12,001 | 0.17 | 0 (0) | 7 (1) | 36.33 [7.26–65.40] | 0.04 | 0.00 | 14 | 0 | 669 | 19 | 1,396 | 231 |
| titan | moved | 4 |  | 16,599 | 0.22 | 0 (0) | 11 (4) | 31.65 [8.20–55.09] | 0.05 | 0.00 | 17 | 0 | 690 | 20 | 1,432 | 222 |
| titan | moved | 5 |  | 12,599 | 0.16 | 0 (0) | 5 (1) | 24.77 [0.50–49.05] | 0.04 | 0.00 | 15 | 0 | 692 | 19 | 1,428 | 232 |
| titan | moved | 6 |  | 13,920 | 0.17 | 0 (0) | 3 (1) | 11.74 [0.00–28.01] | 0.05 | 0.00 | 16 | 0 | 713 | 19 | 1,429 | 228 |
| hyperion | as it was | 2 |  | 38,231 | 0.84 | 1 (0) | 44 (17) | 33.27 [20.95–45.60] | 0.05 | 0.00 | 17 | 0 | 699 | 10 | 1,518 | 242 |
| hyperion | as it was | 3 |  | 28,131 | 0.38 | 4 (0) | 10 (5) | 23.40 [8.11–38.69] | 0.05 | 0.00 | 16 | 0 | 694 | 10 | 1,456 | 235 |
| hyperion | as it was | 4 |  | 30,707 | 0.40 | 1 (0) | 8 (1) | 19.94 [6.12–33.76] | 0.05 | 0.00 | 15 | 0 | 682 | 9 | 1,487 | 231 |
| hyperion | as it was | 5 | yes | 26,893 | 0.42 | 0 (0) | 16 (4) | 28.47 [12.36–44.57] | 8.65 | 0.00 | 13,750 | 8,739 | 659 | 10 | 1,711 | 237 |
| hyperion | as it was | 6 |  | 26,633 | 0.36 | 0 (0) | 14 (1) | 36.18 [16.51–55.84] | 0.04 | 0.00 | 15 | 0 | 696 | 10 | 1,336 | 205 |
| hyperion | moved | 2 |  | 25,178 | 0.53 | 0 (0) | 27 (12) | 28.44 [14.05–42.83] | 0.05 | 0.00 | 19 | 0 | 727 | 11 | 1,691 | 265 |
| hyperion | moved | 3 |  | 12,487 | 0.15 | 0 (0) | 2 (0) | 12.99 [0.00–30.99] | 0.05 | 0.00 | 16 | 0 | 695 | 10 | 1,529 | 216 |
| hyperion | moved | 4 |  | 10,332 | 0.12 | 0 (0) | 3 (1) | 16.45 [0.00–39.26] | 0.05 | 0.00 | 17 | 0 | 692 | 10 | 1,543 | 222 |
| hyperion | moved | 5 |  | 13,156 | 0.16 | 0 (0) | 3 (0) | 19.13 [0.00–40.79] | 0.05 | 0.00 | 15 | 0 | 704 | 10 | 1,512 | 249 |
| hyperion | moved | 6 |  | 15,002 | 0.20 | 0 (0) | 5 (0) | 25.23 [3.11–47.35] | 0.04 | 0.00 | 15 | 0 | 711 | 10 | 1,537 | 207 |

## The NIC's shard against the rest

Unfaulted steps past 100 µs on their own cpu a second of step cpu, on the shard whose cpu took the host's NIC interrupts against every other shard of the host, with their counts. With the NIC moved there is no NIC shard, and the shard that had it is read as the rest are.

| Population | Host | NIC | The NIC's shard | Count | The rest | Count | Ratio |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Big | europa | as it was | - | - | 82.00 [37.42–126.57] | 13 | - |
| Big | europa | moved | - | - | 25.81 [0.00–61.57] | 2 | - |
| Big | titan | as it was | 58.91 [24.10–93.73] | 11 | 85.60 [65.41–105.80] | 69 | 0.69 |
| Big | titan | moved | - | - | 86.06 [61.71–110.40] | 48 | - |
| Big | hyperion | as it was | 99.31 [53.43–145.19] | 18 | 81.09 [60.90–101.27] | 62 | 1.22 |
| Big | hyperion | moved | - | - | 85.06 [60.99–109.12] | 48 | - |
| Keyword | europa | as it was | - | - | 88.62 [72.83–104.41] | 121 | - |
| Keyword | europa | moved | - | - | 99.54 [76.71–122.38] | 73 | - |
| Keyword | titan | as it was | 21.55 [8.19–34.91] | 10 | 24.02 [15.70–32.35] | 32 | 0.90 |
| Keyword | titan | moved | - | - | 26.62 [15.50–37.74] | 22 | - |
| Keyword | hyperion | as it was | 28.47 [12.36–44.57] | 12 | 29.20 [21.68–36.71] | 58 | 0.98 |
| Keyword | hyperion | moved | - | - | 23.32 [14.52–32.11] | 27 | - |

## What the kernel did to the long steps

Every round together. *Past* is past 100 µs by the wall, *own cpu* on the step's own cpu; *switched* is a step past by the wall whose thread left its cpu inside it, and *faults a faulted step* the page faults the long steps took over those that faulted.

| Population | Host | Kind | Steps | Past | Own cpu | Faulted | Unfaulted | By the wall alone | Switched | Faults a faulted step |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Big | europa | captures | 16,609 | 2 | 2 | 0 | 2 | 0 | 0 | - |
| Big | europa | encodes | 104,928 | 13 | 13 | 0 | 13 | 0 | 0 | - |
| Big | titan | captures | 28,170 | 103 | 96 | 0 | 96 | 7 | 7 | - |
| Big | titan | encodes | 234,677 | 76 | 38 | 6 | 32 | 38 | 38 | 2 |
| Big | hyperion | captures | 28,119 | 94 | 89 | 0 | 89 | 5 | 7 | - |
| Big | hyperion | encodes | 227,565 | 68 | 40 | 1 | 39 | 28 | 29 | 1 |
| Keyword | europa | captures | 36,969 | 4 | 4 | 0 | 4 | 0 | 0 | - |
| Keyword | europa | encodes | 118,804 | 264 | 262 | 72 | 190 | 2 | 1 | 4 |
| Keyword | titan | captures | 53,419 | 7 | 3 | 0 | 3 | 4 | 4 | - |
| Keyword | titan | encodes | 141,620 | 95 | 78 | 17 | 61 | 17 | 18 | 5 |
| Keyword | hyperion | captures | 57,377 | 13 | 6 | 0 | 6 | 7 | 7 | - |
| Keyword | hyperion | encodes | 169,373 | 153 | 132 | 41 | 91 | 21 | 21 | 4 |

## By window

Every shard's 2 s windows of a host, as the bench sampled them, put in three equal groups by the softirqs a second its cpu ran over the window, every kind counted (a count, where the tick-sampled share is zero in most windows), sorted on that alone; and the unfaulted steps past 100 µs on their own cpu in each group a second of step cpu. Steps that follow softirqs climb with them; steps of their own do not.

| Population | Host | Softirqs/s (low) | Rate | Count | Softirqs/s (middle) | Rate | Count | Softirqs/s (high) | Rate | Count |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Big | europa | 314 | 140.98 [17.41–264.56] | 5 | 517 | 46.46 [9.28–83.63] | 6 | 989 | 56.01 [1.12–110.89] | 4 |
| Big | titan | 302 | 101.14 [77.28–125.01] | 69 | 469 | 65.18 [41.45–88.90] | 29 | 4,980 | 70.86 [45.50–96.22] | 30 |
| Big | hyperion | 262 | 92.28 [69.50–115.07] | 63 | 435 | 68.05 [43.28–92.82] | 29 | 6,245 | 89.70 [60.40–119.01] | 36 |
| Keyword | europa | 307 | 117.49 [82.78–152.21] | 44 | 419 | 89.41 [70.29–108.54] | 84 | 786 | 84.10 [63.81–104.38] | 66 |
| Keyword | titan | 289 | 23.05 [13.83–32.27] | 24 | 424 | 21.25 [11.43–31.07] | 18 | 4,552 | 29.95 [17.44–42.47] | 22 |
| Keyword | hyperion | 262 | 27.23 [19.00–35.47] | 42 | 406 | 27.95 [18.82–37.08] | 36 | 4,533 | 25.83 [14.21–37.44] | 19 |

## What a long step's window took

The mean of each interrupt and softirq a second on the shard's own cpu, over the 2 s windows that held an unfaulted step past 100 µs on its own cpu against those that held none, each window weighted alike. Interrupts that hold a step would be more frequent in its windows.

| Population | Host | Windows | NET_RX | TIMER | SCHED | RCU | BLOCK | NVME | LOC | RES | CAL | TLB | minflt |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Big | europa | 15 with a long step | 151 | 265 | 184 | 25 | 0 | 1,822 | 1,537 | 133 | 2,228 | 9 | 458 |
| Big | europa | 770 without | 137 | 263 | 184 | 23 | 0 | 1,101 | 1,431 | 142 | 2,040 | 5 | 304 |
| Big | titan | 124 with a long step | 1,012 | 312 | 37 | 72 | 0 | 731 | 1,996 | 171 | 1,054 | 28 | 398 |
| Big | titan | 3,125 without | 1,444 | 381 | 33 | 75 | 0 | 574 | 2,128 | 279 | 1,286 | 21 | 351 |
| Big | hyperion | 124 with a long step | 1,395 | 346 | 28 | 42 | 0 | 767 | 2,125 | 203 | 1,150 | 8 | 376 |
| Big | hyperion | 3,075 without | 1,871 | 388 | 30 | 43 | 0 | 572 | 2,272 | 277 | 1,346 | 11 | 334 |
| Keyword | europa | 144 with a long step | 101 | 211 | 144 | 20 | 0 | 606 | 1,091 | 70 | 1,080 | 2 | 206 |
| Keyword | europa | 390 without | 114 | 224 | 156 | 20 | 0 | 509 | 1,162 | 83 | 1,462 | 2 | 102 |
| Keyword | titan | 62 with a long step | 1,356 | 358 | 37 | 72 | 0 | 375 | 1,993 | 175 | 976 | 20 | 177 |
| Keyword | titan | 2,603 without | 1,290 | 358 | 33 | 72 | 0 | 451 | 1,963 | 220 | 1,095 | 19 | 152 |
| Keyword | hyperion | 94 with a long step | 821 | 316 | 27 | 43 | 0 | 453 | 1,806 | 106 | 873 | 5 | 244 |
| Keyword | hyperion | 2,464 without | 1,315 | 368 | 30 | 42 | 0 | 458 | 1,970 | 233 | 1,173 | 7 | 170 |

## Each host's faults

Every process's on the host over the round's samples: page faults a second, and the major faults, compaction stalls and huge page faults in all.

| Round | Host | Seconds | Faults/s | Major faults | Compaction stalls | Huge page faults |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | europa | 1,436 | 5,988 | 580 | 1,231 | 11,485 |
| 1 | titan | 1,436 | 8,234 | 507 | 9,051 | 15,786 |
| 1 | hyperion | 1,435 | 7,474 | 404 | 6,955 | 15,165 |
| 2 | europa | 1,463 | 5,849 | 556 | 0 | 11,337 |
| 2 | titan | 1,463 | 8,369 | 474 | 10,477 | 15,429 |
| 2 | hyperion | 1,462 | 7,357 | 421 | 6,568 | 14,801 |
| 3 | europa | 1,383 | 5,763 | 230 | 0 | 10,845 |
| 3 | titan | 1,383 | 8,263 | 500 | 9,481 | 14,772 |
| 3 | hyperion | 1,382 | 7,924 | 408 | 8,339 | 15,450 |
| 4 | europa | 1,439 | 5,734 | 174 | 0 | 10,905 |
| 4 | titan | 1,438 | 8,105 | 572 | 9,074 | 14,776 |
| 4 | hyperion | 1,438 | 7,688 | 467 | 7,648 | 15,733 |
| 5 | europa | 1,427 | 5,774 | 911 | 0 | 11,115 |
| 5 | titan | 1,426 | 7,862 | 526 | 7,986 | 14,990 |
| 5 | hyperion | 1,426 | 7,486 | 444 | 7,282 | 15,229 |
| 6 | europa | 1,412 | 5,936 | 75 | 0 | 11,313 |
| 6 | titan | 1,411 | 8,239 | 616 | 9,837 | 15,281 |
| 6 | hyperion | 1,411 | 7,424 | 356 | 6,886 | 14,996 |
