# X2 placement simulation

One consumer. Placement groups a tablet [1, 4, 16, 64]; 4096 tablets. Seeded: these tables are the same on every host and every run.

## Shapes

| shape | hosts | devices | slices | pools (domains) | what it stands for |
| --- | --- | --- | --- | --- | --- |
| lab-1 | 3 | 3 | 3 | p r3/host (3), p 2+1/host (3) | the lab, one 500 GB device a host |
| lab-2 | 3 | 6 | 6 | p r3/host (3), p 2+1/host (3), p 4+2/device (6) | the lab, two 500 GB devices a host |
| lab-fitted | 3 | 4 | 4 | p r3/host (3), p 2+1/host (3), p 3+1/device (4) | the lab as fitted: 261 + 931 GiB on europa, 466 on titan and hyperion |
| 6x12 | 6 | 72 | 72 | p r3/host (6), p 4+2/host (6), p 8+3/device (72) | six hosts of twelve 8 TB devices |
| 6x12-mixed | 6 | 72 | 72 | p 4+2/host (6), p 8+3/device (72) | six hosts, each of six 4 TB and six 16 TB devices |
| 6x12-uneven | 6 | 72 | 72 | p r3/host (6), p 4+2/host (6), p 8+3/device (72) | three hosts of twelve 4 TB devices, three of twelve 16 TB |
| 6x12-slices | 6 | 72 | 180 | p 4+2/host (6), p 8+3/device (72) | six hosts of twelve 8 TB devices, every other one in four slices |
| 50x24 | 50 | 1200 | 1200 | p r3/host (50), p 8+3/host (50), p 10+4/host (50) | fifty hosts of twenty-four 16 TB devices |
| two-classes | 6 | 72 | 96 | bulk 4+2/host (6), fast r3/host (6) | six hosts, each of eight 16 TB hdd and four 3.84 TB ssd in two slices |

## Fill

Each cell is the fullest device over the mean, then the coefficient of variation across the pool's devices, of chunks over weight. `table` is the planner's assignment, the best the shape allows. A window cell that breaks the domain rule says how many groups it broke it for.

### lab-1: the lab, one 500 GB device a host

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | 1 | 4096 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% |
| p r3/host | 4 | 16384 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% |
| p r3/host | 16 | 65536 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% |
| p r3/host | 64 | 262144 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0%, 1 fell back | +0.0% / 0.0% | +0.0% / 0.0%, 2 fell back | +0.0% / 0.0% |
| p 2+1/host | 1 | 4096 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% |
| p 2+1/host | 4 | 16384 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% |
| p 2+1/host | 16 | 65536 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0% |
| p 2+1/host | 64 | 262144 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0%, 1 fell back | +0.0% / 0.0% | +0.0% / 0.0%, 2 fell back | +0.0% / 0.0% |

### lab-2: the lab, two 500 GB devices a host

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | 1 | 2048 | +0.0% / 0.0% | +3.7% / 2.2% | +2.0% / 1.2% | +0.3% / 0.2% | +3.1% / 2.6% | +0.0% / 0.0% |
| p r3/host | 4 | 8192 | +0.0% / 0.0% | +0.9% / 0.7% | +1.8% / 1.2% | +0.9% / 0.7% | +1.8% / 1.1% | +0.0% / 0.0% |
| p r3/host | 16 | 32768 | +0.0% / 0.0% | +0.5% / 0.3% | +0.4% / 0.3% | +0.3% / 0.2% | +0.5% / 0.3% | +0.0% / 0.0% |
| p r3/host | 64 | 131072 | +0.0% / 0.0% | +0.3% / 0.2% | +0.1% / 0.1% | +0.2% / 0.1% | +0.4% / 0.2% | +0.0% / 0.0% |
| p 2+1/host | 1 | 2048 | +0.0% / 0.0% | +3.7% / 2.2% | +2.0% / 1.2% | +0.3% / 0.2% | +3.1% / 2.6% | +0.0% / 0.0% |
| p 2+1/host | 4 | 8192 | +0.0% / 0.0% | +0.9% / 0.7% | +1.8% / 1.2% | +0.9% / 0.7% | +1.8% / 1.1% | +0.0% / 0.0% |
| p 2+1/host | 16 | 32768 | +0.0% / 0.0% | +0.5% / 0.3% | +0.4% / 0.3% | +0.3% / 0.2% | +0.5% / 0.3% | +0.0% / 0.0% |
| p 2+1/host | 64 | 131072 | +0.0% / 0.0% | +0.3% / 0.2% | +0.1% / 0.1% | +0.2% / 0.1% | +0.4% / 0.2% | +0.0% / 0.0% |
| p 4+2/device | 1 | 4096 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0%, 20 fell back | +0.0% / 0.0% | +0.0% / 0.0%, 17 fell back | +0.0% / 0.0% |
| p 4+2/device | 4 | 16384 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0%, 74 fell back | +0.0% / 0.0% | +0.0% / 0.0%, 68 fell back | +0.0% / 0.0% |
| p 4+2/device | 16 | 65536 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0%, 302 fell back | +0.0% / 0.0% | +0.0% / 0.0%, 283 fell back | +0.0% / 0.0% |
| p 4+2/device | 64 | 262144 | +0.0% / 0.0% | +0.0% / 0.0% | +0.0% / 0.0%, 1099 fell back | +0.0% / 0.0% | +0.0% / 0.0%, 1168 fell back | +0.0% / 0.0% |

### lab-fitted: the lab as fitted: 261 + 931 GiB on europa, 466 on titan and hyperion

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | 1 | 3072 | +103.4% / 52.4%, breaks the rule for 2048 | +51.9% / 45.7% | +51.9% / 46.6%, 2 fell back | +51.9% / 46.1% | +51.9% / 45.8% | +51.9% / 46.3% |
| p r3/host | 4 | 12288 | +103.4% / 52.4%, breaks the rule for 8192 | +51.9% / 46.2% | +51.9% / 46.5%, 8 fell back | +51.9% / 46.2% | +51.9% / 46.1%, 6 fell back | +51.9% / 46.3% |
| p r3/host | 16 | 49152 | +103.4% / 52.4%, breaks the rule for 32768 | +51.9% / 46.2% | +51.9% / 46.3%, 31 fell back | +51.9% / 46.2% | +51.9% / 46.3%, 25 fell back | +51.9% / 46.3% |
| p r3/host | 64 | 196608 | +103.4% / 52.4%, breaks the rule for 131072 | +51.9% / 46.3% | +51.9% / 46.3%, 102 fell back | +51.9% / 46.3% | +51.9% / 46.3%, 101 fell back | +51.9% / 46.3% |
| p 2+1/host | 1 | 3072 | +103.4% / 52.4%, breaks the rule for 2048 | +51.9% / 45.7% | +51.9% / 46.6%, 2 fell back | +51.9% / 46.1% | +51.9% / 45.8% | +51.9% / 46.3% |
| p 2+1/host | 4 | 12288 | +103.4% / 52.4%, breaks the rule for 8192 | +51.9% / 46.2% | +51.9% / 46.5%, 8 fell back | +51.9% / 46.2% | +51.9% / 46.1%, 6 fell back | +51.9% / 46.3% |
| p 2+1/host | 16 | 49152 | +103.4% / 52.4%, breaks the rule for 32768 | +51.9% / 46.2% | +51.9% / 46.3%, 31 fell back | +51.9% / 46.2% | +51.9% / 46.3%, 25 fell back | +51.9% / 46.3% |
| p 2+1/host | 64 | 196608 | +103.4% / 52.4%, breaks the rule for 131072 | +51.9% / 46.3% | +51.9% / 46.3%, 102 fell back | +51.9% / 46.3% | +51.9% / 46.3%, 101 fell back | +51.9% / 46.3% |
| p 3+1/device | 1 | 4096 | +103.4% / 52.4% | +103.4% / 52.4% | +103.4% / 52.4%, 36 fell back | +103.4% / 52.4% | +103.4% / 52.4%, 39 fell back | +103.4% / 52.4% |
| p 3+1/device | 4 | 16384 | +103.4% / 52.4% | +103.4% / 52.4% | +103.4% / 52.4%, 130 fell back | +103.4% / 52.4% | +103.4% / 52.4%, 167 fell back | +103.4% / 52.4% |
| p 3+1/device | 16 | 65536 | +103.4% / 52.4% | +103.4% / 52.4% | +103.4% / 52.4%, 569 fell back | +103.4% / 52.4% | +103.4% / 52.4%, 606 fell back | +103.4% / 52.4% |
| p 3+1/device | 64 | 262144 | +103.4% / 52.4% | +103.4% / 52.4% | +103.4% / 52.4%, 2285 fell back | +103.4% / 52.4% | +103.4% / 52.4%, 2293 fell back | +103.4% / 52.4% |

### 6x12: six hosts of twelve 8 TB devices

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | 1 | 171 | +0.2% / 0.5% | +18.9% / 8.0% | +14.3% / 8.0% | +21.3% / 7.1% | +17.8% / 7.9% | +0.2% / 0.3% |
| p r3/host | 4 | 683 | +0.2% / 0.2% | +8.7% / 4.0% | +8.1% / 3.5% | +8.3% / 3.6% | +7.5% / 3.5% | +0.0% / 0.1% |
| p r3/host | 16 | 2731 | +0.1% / 0.0% | +4.5% / 1.9% | +4.5% / 1.8% | +4.3% / 1.9% | +4.3% / 1.9% | +0.0% / 0.0% |
| p r3/host | 64 | 10923 | +0.0% / 0.0% | +2.8% / 0.8% | +2.2% / 0.7% | +2.6% / 1.0% | +2.0% / 0.9% | +0.0% / 0.0% |
| p 4+2/host | 1 | 341 | +0.2% / 0.5% | +10.4% / 5.2% | +9.9% / 5.0%, 14 fell back | +12.5% / 5.4% | +14.6% / 5.6%, 15 fell back | +0.2% / 0.1% |
| p 4+2/host | 4 | 1365 | +0.2% / 0.2% | +6.5% / 2.4% | +5.2% / 2.2%, 65 fell back | +5.5% / 2.5% | +5.0% / 2.3%, 58 fell back | +0.0% / 0.0% |
| p 4+2/host | 16 | 5461 | +0.1% / 0.0% | +4.0% / 1.3% | +3.6% / 1.5%, 290 fell back | +2.8% / 1.2% | +3.0% / 1.4%, 275 fell back | +0.0% / 0.0% |
| p 4+2/host | 64 | 21845 | +0.0% / 0.0% | +1.3% / 0.6% | +1.5% / 0.6%, 1130 fell back | +2.1% / 0.7% | +1.6% / 0.6%, 1102 fell back | +0.0% / 0.0% |
| p 8+3/device | 1 | 626 | +0.2% / 0.4% | +9.9% / 3.6% | +7.7% / 3.6% | +6.7% / 3.3% | +9.8% / 3.9% | +0.0% / 0.1% |
| p 8+3/device | 4 | 2503 | +0.2% / 0.2% | +5.6% / 1.7% | +3.2% / 1.6% | +4.4% / 1.8% | +4.3% / 1.9% | +0.0% / 0.0% |
| p 8+3/device | 16 | 10012 | +0.1% / 0.0% | +1.8% / 0.8% | +1.8% / 0.8% | +1.9% / 0.9% | +2.6% / 1.0% | +0.0% / 0.0% |
| p 8+3/device | 64 | 40050 | +0.0% / 0.0% | +1.0% / 0.4% | +0.8% / 0.4% | +1.4% / 0.5% | +0.9% / 0.4% | +0.0% / 0.0% |

### 6x12-mixed: six hosts, each of six 4 TB and six 16 TB devices

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p 4+2/host | 1 | 341 | +150.4% / 93.8% | +22.3% / 6.9% | +14.2% / 5.5%, 20 fell back | +16.4% / 6.7% | +15.0% / 6.3%, 18 fell back | +0.2% / 0.3% |
| p 4+2/host | 4 | 1365 | +150.4% / 93.7% | +10.8% / 3.0% | +6.7% / 3.2%, 83 fell back | +10.2% / 3.2% | +7.8% / 2.8%, 64 fell back | +0.0% / 0.0% |
| p 4+2/host | 16 | 5461 | +150.2% / 93.7% | +3.9% / 1.6% | +4.9% / 1.7%, 308 fell back | +4.9% / 1.6% | +4.9% / 1.6%, 291 fell back | +0.0% / 0.0% |
| p 4+2/host | 64 | 21845 | +150.0% / 93.7% | +2.3% / 0.8% | +1.9% / 0.8%, 1180 fell back | +2.9% / 0.8% | +2.5% / 0.9%, 1131 fell back | +0.0% / 0.0% |
| p 8+3/device | 1 | 626 | +150.4% / 93.7% | +23.8% / 7.0% | +20.2% / 6.3% | +21.8% / 7.4% | +21.4% / 7.0% | +0.1% / 0.1% |
| p 8+3/device | 4 | 2503 | +150.4% / 93.7% | +12.0% / 5.5% | +14.0% / 5.6% | +15.7% / 5.3% | +13.4% / 5.1% | +0.0% / 0.0% |
| p 8+3/device | 16 | 10012 | +150.2% / 93.7% | +11.4% / 5.0% | +10.3% / 4.8% | +11.4% / 5.1% | +10.3% / 4.9% | +0.0% / 0.0% |
| p 8+3/device | 64 | 40050 | +150.0% / 93.7% | +9.8% / 4.7% | +10.0% / 4.7% | +9.7% / 4.7% | +8.7% / 4.7% | +0.0% / 0.0% |

### 6x12-uneven: three hosts of twelve 4 TB devices, three of twelve 16 TB

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | 1 | 171 | +150.4% / 93.7% | +50.9% / 20.2% | +67.0% / 22.0% | +61.1% / 23.4% | +74.3% / 22.4% | +0.3% / 0.3% |
| p r3/host | 4 | 683 | +150.4% / 93.7% | +47.2% / 18.6% | +40.2% / 19.3% | +47.6% / 20.2% | +39.9% / 19.4% | +0.1% / 0.0% |
| p r3/host | 16 | 2731 | +150.2% / 93.7% | +36.5% / 18.6% | +34.5% / 18.6% | +37.5% / 18.9% | +35.9% / 18.5% | +0.0% / 0.0% |
| p r3/host | 64 | 10923 | +150.0% / 93.7% | +32.2% / 18.5% | +32.7% / 18.4% | +32.7% / 18.6% | +32.5% / 18.4% | +0.0% / 0.0% |
| p 4+2/host | 1 | 341 | +150.4% / 93.7% | +190.0% / 94.4% | +170.2% / 94.1%, 611 fell back | +176.8% / 94.2% | +191.5% / 94.3%, 561 fell back | +150.4% / 93.7% |
| p 4+2/host | 4 | 1365 | +150.4% / 93.7% | +168.6% / 93.9% | +164.7% / 93.8%, 2395 fell back | +166.2% / 93.8% | +163.6% / 93.9%, 2272 fell back | +150.1% / 93.7% |
| p 4+2/host | 16 | 5461 | +150.2% / 93.7% | +159.2% / 93.8% | +157.3% / 93.8%, 9468 fell back | +156.9% / 93.8% | +157.0% / 93.8%, 9297 fell back | +150.0% / 93.7% |
| p 4+2/host | 64 | 21845 | +150.0% / 93.7% | +153.8% / 93.7% | +152.5% / 93.7%, 37571 fell back | +152.6% / 93.7% | +154.0% / 93.7%, 37465 fell back | +150.0% / 93.7% |
| p 8+3/device | 1 | 626 | +150.4% / 93.7% | +23.0% / 7.0% | +18.6% / 6.2% | +22.2% / 7.2% | +17.0% / 5.7% | +0.1% / 0.1% |
| p 8+3/device | 4 | 2503 | +150.4% / 93.7% | +14.4% / 4.9% | +13.8% / 4.8% | +11.9% / 5.0% | +15.5% / 5.4% | +0.0% / 0.0% |
| p 8+3/device | 16 | 10012 | +150.2% / 93.7% | +9.9% / 4.7% | +10.0% / 4.8% | +10.4% / 4.6% | +10.7% / 4.7% | +0.0% / 0.0% |
| p 8+3/device | 64 | 40050 | +150.0% / 93.7% | +8.7% / 4.7% | +8.6% / 4.6% | +9.2% / 4.7% | +9.7% / 4.8% | +0.0% / 0.0% |

### 6x12-slices: six hosts of twelve 8 TB devices, every other one in four slices

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p 4+2/host | 1 | 341 | +61.7% / 60.0% | +11.0% / 5.0% | +15.1% / 4.7%, 21 fell back | +11.0% / 4.8% | +16.0% / 4.8%, 18 fell back | +0.2% / 0.1% |
| p 4+2/host | 4 | 1365 | +60.2% / 60.0% | +5.8% / 2.2% | +5.8% / 2.7%, 60 fell back | +6.2% / 2.8% | +7.2% / 2.9%, 64 fell back | +0.0% / 0.0% |
| p 4+2/host | 16 | 5461 | +60.2% / 60.0% | +2.7% / 1.2% | +3.1% / 1.2%, 291 fell back | +3.8% / 1.3% | +3.7% / 1.4%, 285 fell back | +0.0% / 0.0% |
| p 4+2/host | 64 | 21845 | +60.1% / 60.0% | +1.3% / 0.6% | +2.0% / 0.7%, 1149 fell back | +1.7% / 0.7% | +1.4% / 0.6%, 1127 fell back | +0.0% / 0.0% |
| p 8+3/device | 1 | 626 | +61.7% / 60.1%, breaks the rule for 3006 | +9.1% / 3.8% | +12.0% / 3.8% | +9.5% / 3.6% | +9.0% / 3.6% | +0.0% / 0.1% |
| p 8+3/device | 4 | 2503 | +60.2% / 60.0%, breaks the rule for 12014 | +4.0% / 1.7% | +4.4% / 1.7% | +3.8% / 1.8% | +4.6% / 1.8% | +0.0% / 0.0% |
| p 8+3/device | 16 | 10012 | +60.2% / 60.0%, breaks the rule for 48062 | +2.6% / 1.0% | +2.1% / 0.9% | +2.3% / 1.0% | +1.9% / 1.0% | +0.0% / 0.0% |
| p 8+3/device | 64 | 40050 | +60.1% / 60.0%, breaks the rule for 192238 | +1.6% / 0.5% | +1.0% / 0.5% | +1.3% / 0.5% | +1.0% / 0.4% | +0.0% / 0.0% |

### 50x24: fifty hosts of twenty-four 16 TB devices

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | 1 | 10 | +17.2% / 14.4% | +105.1% / 31.2% | +114.8% / 31.2% | +124.6% / 31.7% | +95.3% / 30.4% | +7.4% / 4.2% |
| p r3/host | 4 | 41 | +2.5% / 3.5% | +48.9% / 15.9% | +66.0% / 15.7% | +53.8% / 15.7% | +51.4% / 15.6% | +0.1% / 0.5% |
| p r3/host | 16 | 164 | +0.7% / 0.9% | +26.3% / 7.7% | +26.3% / 7.7% | +31.8% / 7.8% | +28.8% / 8.0% | +0.1% / 0.2% |
| p r3/host | 64 | 655 | +0.3% / 0.2% | +12.9% / 4.0% | +12.5% / 3.9% | +13.5% / 3.9% | +10.9% / 3.9% | +0.1% / 0.1% |
| p 8+3/host | 1 | 38 | +17.2% / 14.3% | +57.1% / 15.9% | +67.8% / 15.9% | +57.1% / 16.2% | +57.1% / 15.9% | +1.2% / 1.3% |
| p 8+3/host | 4 | 150 | +2.5% / 3.5% | +26.5% / 8.0% | +27.2% / 8.4% | +25.2% / 8.4% | +32.5% / 7.9% | +0.5% / 0.3% |
| p 8+3/host | 16 | 601 | +0.7% / 0.9% | +12.5% / 4.2% | +13.9% / 4.0% | +13.4% / 4.1% | +15.2% / 4.1% | +0.0% / 0.1% |
| p 8+3/host | 64 | 2403 | +0.3% / 0.2% | +7.9% / 2.1% | +6.0% / 2.0% | +6.9% / 2.0% | +8.1% / 2.1% | +0.0% / 0.0% |
| p 10+4/host | 1 | 48 | +17.2% / 14.3% | +46.5% / 14.3% | +50.7% / 14.3% | +56.9% / 14.1% | +50.7% / 14.0% | +0.4% / 0.9% |
| p 10+4/host | 4 | 191 | +2.5% / 3.5% | +28.2% / 7.1% | +22.4% / 7.5% | +21.9% / 7.2% | +21.9% / 7.1% | +0.4% / 0.2% |
| p 10+4/host | 16 | 765 | +0.7% / 0.9% | +11.0% / 3.6% | +11.8% / 3.6% | +12.2% / 3.6% | +12.2% / 3.6% | +0.1% / 0.1% |
| p 10+4/host | 64 | 3058 | +0.3% / 0.2% | +6.4% / 1.8% | +6.2% / 1.8% | +6.1% / 1.8% | +4.8% / 1.8% | +0.0% / 0.0% |

### two-classes: six hosts, each of eight 16 TB hdd and four 3.84 TB ssd in two slices

| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bulk 4+2/host | 1 | 512 | +0.8% / 0.5% | +9.4% / 4.5% | +12.1% / 4.4%, 23 fell back | +11.1% / 4.1% | +7.4% / 4.3%, 18 fell back | +0.0% / 0.0% |
| bulk 4+2/host | 4 | 2048 | +0.2% / 0.1% | +5.3% / 2.2% | +4.3% / 2.0%, 72 fell back | +4.3% / 1.6% | +5.0% / 2.2%, 74 fell back | +0.0% / 0.0% |
| bulk 4+2/host | 16 | 8192 | +0.0% / 0.0% | +2.8% / 1.0% | +1.8% / 1.0%, 252 fell back | +2.4% / 1.0% | +2.4% / 1.2%, 278 fell back | +0.0% / 0.0% |
| bulk 4+2/host | 64 | 32768 | +0.0% / 0.0% | +1.6% / 0.5% | +1.1% / 0.6%, 1047 fell back | +1.2% / 0.5% | +0.9% / 0.5%, 1082 fell back | +0.0% / 0.0% |
| fast r3/host | 1 | 512 | +0.8% / 0.5% | +8.8% / 4.4% | +9.6% / 4.0% | +10.7% / 4.3% | +11.7% / 5.0% | +0.0% / 0.0% |
| fast r3/host | 4 | 2048 | +0.2% / 0.1% | +4.3% / 2.5% | +2.9% / 1.6% | +4.3% / 2.5% | +4.0% / 2.0% | +0.0% / 0.0% |
| fast r3/host | 16 | 8192 | +0.0% / 0.0% | +2.5% / 0.9% | +1.5% / 0.9% | +2.7% / 1.3% | +2.3% / 1.0% | +0.0% / 0.0% |
| fast r3/host | 64 | 32768 | +0.0% / 0.0% | +1.0% / 0.4% | +1.1% / 0.5% | +1.7% / 0.6% | +1.1% / 0.5% | +0.0% / 0.0% |

## Exceptions to rendezvous

Starting from the rendezvous answer, one chunk at a time is moved off the fullest device onto the emptiest the domain rule allows. A cell is the exceptions on the map once the fullest device is within the margin, and the placement groups they touch, as a share of the pool's; `stuck` is a balancer that found no chunk on the fullest device it could move anywhere emptier.

| shape | pool | groups a tablet | fullest before | within 5% | within 2% | within 1% |
| --- | --- | --- | --- | --- | --- | --- |
| lab-1 | p r3/host | 1 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-1 | p r3/host | 4 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-1 | p r3/host | 16 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-1 | p r3/host | 64 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-1 | p 2+1/host | 1 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-1 | p 2+1/host | 4 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-1 | p 2+1/host | 16 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-1 | p 2+1/host | 64 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p r3/host | 1 | +3.7% | 0 (0.00% of groups) | 36 (0.88% of groups) | 56 (1.37% of groups) |
| lab-2 | p r3/host | 4 | +0.9% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p r3/host | 16 | +0.5% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p r3/host | 64 | +0.3% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p 2+1/host | 1 | +3.7% | 0 (0.00% of groups) | 36 (0.88% of groups) | 56 (1.37% of groups) |
| lab-2 | p 2+1/host | 4 | +0.9% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p 2+1/host | 16 | +0.5% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p 2+1/host | 64 | +0.3% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p 4+2/device | 1 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p 4+2/device | 4 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p 4+2/device | 16 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-2 | p 4+2/device | 64 | +0.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| lab-fitted | p r3/host | 1 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p r3/host | 4 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p r3/host | 16 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p r3/host | 64 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p 2+1/host | 1 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p 2+1/host | 4 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p 2+1/host | 16 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p 2+1/host | 64 | +51.9% | stuck | stuck | stuck |
| lab-fitted | p 3+1/device | 1 | +103.4% | stuck | stuck | stuck |
| lab-fitted | p 3+1/device | 4 | +103.4% | stuck | stuck | stuck |
| lab-fitted | p 3+1/device | 16 | +103.4% | stuck | stuck | stuck |
| lab-fitted | p 3+1/device | 64 | +103.4% | stuck | stuck | stuck |
| 6x12 | p r3/host | 1 | +18.9% | 159 (3.44% of groups) | 289 (5.71% of groups) | 348 (6.67% of groups) |
| 6x12 | p r3/host | 4 | +8.7% | 86 (0.52% of groups) | 408 (2.04% of groups) | 614 (2.89% of groups) |
| 6x12 | p r3/host | 16 | +4.5% | 0 (0.00% of groups) | 265 (0.39% of groups) | 655 (0.88% of groups) |
| 6x12 | p r3/host | 64 | +2.8% | 0 (0.00% of groups) | 84 (0.03% of groups) | 490 (0.17% of groups) |
| 6x12 | p 4+2/host | 1 | +10.4% | 131 (2.29% of groups) | 342 (4.71% of groups) | 459 (5.88% of groups) |
| 6x12 | p 4+2/host | 4 | +6.5% | 22 (0.13% of groups) | 258 (1.25% of groups) | 539 (2.19% of groups) |
| 6x12 | p 4+2/host | 16 | +4.0% | 0 (0.00% of groups) | 166 (0.25% of groups) | 586 (0.72% of groups) |
| 6x12 | p 4+2/host | 64 | +1.3% | 0 (0.00% of groups) | 0 (0.00% of groups) | 220 (0.08% of groups) |
| 6x12 | p 8+3/device | 1 | +9.9% | 69 (1.44% of groups) | 293 (4.17% of groups) | 455 (5.40% of groups) |
| 6x12 | p 8+3/device | 4 | +5.6% | 16 (0.10% of groups) | 213 (1.07% of groups) | 497 (1.92% of groups) |
| 6x12 | p 8+3/device | 16 | +1.8% | 0 (0.00% of groups) | 0 (0.00% of groups) | 292 (0.35% of groups) |
| 6x12 | p 8+3/device | 64 | +1.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 0 (0.00% of groups) |
| 6x12-mixed | p 4+2/host | 1 | +22.3% | 148 (3.00% of groups) | 285 (5.10% of groups) | 385 (6.42% of groups) |
| 6x12-mixed | p 4+2/host | 4 | +10.8% | 38 (0.23% of groups) | 240 (1.19% of groups) | 486 (1.97% of groups) |
| 6x12-mixed | p 4+2/host | 16 | +3.9% | 0 (0.00% of groups) | 168 (0.24% of groups) | 575 (0.71% of groups) |
| 6x12-mixed | p 4+2/host | 64 | +2.3% | 0 (0.00% of groups) | 27 (0.01% of groups) | 291 (0.11% of groups) |
| 6x12-mixed | p 8+3/device | 1 | +23.8% | 430 (6.79% of groups) | 670 (9.57% of groups) | 798 (10.67% of groups) |
| 6x12-mixed | p 8+3/device | 4 | +12.0% | 1176 (4.38% of groups) | 2148 (6.87% of groups) | 2498 (7.81% of groups) |
| 6x12-mixed | p 8+3/device | 16 | +11.4% | 3943 (3.26% of groups) | 8231 (5.93% of groups) | 9671 (6.88% of groups) |
| 6x12-mixed | p 8+3/device | 64 | +9.8% | 14455 (2.71% of groups) | 31771 (5.69% of groups) | 37531 (6.71% of groups) |
| 6x12-uneven | p r3/host | 1 | +50.9% | 614 (13.92% of groups) | 702 (15.58% of groups) | 746 (16.50% of groups) |
| 6x12-uneven | p r3/host | 4 | +47.2% | 2357 (12.87% of groups) | 2645 (14.39% of groups) | 2753 (14.97% of groups) |
| 6x12-uneven | p r3/host | 16 | +36.5% | 9603 (13.04% of groups) | 10791 (14.67% of groups) | 11187 (15.21% of groups) |
| 6x12-uneven | p r3/host | 64 | +32.2% | 38488 (12.95% of groups) | 43204 (14.53% of groups) | 44788 (15.07% of groups) |
| 6x12-uneven | p 4+2/host | 1 | +190.0% | stuck | stuck | stuck |
| 6x12-uneven | p 4+2/host | 4 | +168.6% | stuck | stuck | stuck |
| 6x12-uneven | p 4+2/host | 16 | +159.2% | stuck | stuck | stuck |
| 6x12-uneven | p 4+2/host | 64 | +153.8% | stuck | stuck | stuck |
| 6x12-uneven | p 8+3/device | 1 | +23.0% | 418 (6.59% of groups) | 620 (8.74% of groups) | 736 (9.81% of groups) |
| 6x12-uneven | p 8+3/device | 4 | +14.4% | 883 (3.53% of groups) | 1878 (6.10% of groups) | 2237 (6.96% of groups) |
| 6x12-uneven | p 8+3/device | 16 | +9.9% | 3485 (2.91% of groups) | 7778 (5.58% of groups) | 9218 (6.59% of groups) |
| 6x12-uneven | p 8+3/device | 64 | +8.7% | 14444 (2.61% of groups) | 31760 (5.75% of groups) | 37520 (6.80% of groups) |
| 6x12-slices | p 4+2/host | 1 | +11.0% | 124 (2.25% of groups) | 305 (4.74% of groups) | 412 (5.76% of groups) |
| 6x12-slices | p 4+2/host | 4 | +5.8% | 12 (0.07% of groups) | 209 (1.13% of groups) | 426 (1.93% of groups) |
| 6x12-slices | p 4+2/host | 16 | +2.7% | 0 (0.00% of groups) | 72 (0.11% of groups) | 577 (0.63% of groups) |
| 6x12-slices | p 4+2/host | 64 | +1.3% | 0 (0.00% of groups) | 0 (0.00% of groups) | 201 (0.07% of groups) |
| 6x12-slices | p 8+3/device | 1 | +9.1% | 74 (1.44% of groups) | 351 (4.30% of groups) | 488 (5.25% of groups) |
| 6x12-slices | p 8+3/device | 4 | +4.0% | 0 (0.00% of groups) | 150 (0.73% of groups) | 536 (1.73% of groups) |
| 6x12-slices | p 8+3/device | 16 | +2.6% | 0 (0.00% of groups) | 94 (0.14% of groups) | 636 (0.71% of groups) |
| 6x12-slices | p 8+3/device | 64 | +1.6% | 0 (0.00% of groups) | 0 (0.00% of groups) | 321 (0.11% of groups) |
| 50x24 | p r3/host | 1 | +105.1% | stuck | stuck | stuck |
| 50x24 | p r3/host | 4 | +48.9% | 2029 (10.24% of groups) | 3086 (14.26% of groups) | 3086 (14.26% of groups) |
| 50x24 | p r3/host | 16 | +26.3% | 2350 (3.15% of groups) | 4303 (5.24% of groups) | 5336 (6.23% of groups) |
| 50x24 | p r3/host | 64 | +12.9% | 1679 (0.61% of groups) | 6227 (2.02% of groups) | 9216 (2.82% of groups) |
| 50x24 | p 8+3/host | 1 | +57.1% | 2067 (23.34% of groups) | 2586 (26.20% of groups) | stuck |
| 50x24 | p 8+3/host | 4 | +26.5% | 2602 (8.41% of groups) | 4249 (11.19% of groups) | 5313 (12.64% of groups) |
| 50x24 | p 8+3/host | 16 | +12.5% | 1821 (2.07% of groups) | 6511 (4.88% of groups) | 9102 (5.88% of groups) |
| 50x24 | p 8+3/host | 64 | +7.9% | 235 (0.09% of groups) | 4877 (1.27% of groups) | 11705 (2.22% of groups) |
| 50x24 | p 10+4/host | 1 | +46.5% | 2113 (21.48% of groups) | 3162 (26.29% of groups) | 3162 (26.29% of groups) |
| 50x24 | p 10+4/host | 4 | +28.2% | 2616 (7.68% of groups) | 4997 (11.14% of groups) | 5510 (11.66% of groups) |
| 50x24 | p 10+4/host | 16 | +11.0% | 1386 (1.61% of groups) | 6470 (4.42% of groups) | 9387 (5.35% of groups) |
| 50x24 | p 10+4/host | 64 | +6.4% | 72 (0.03% of groups) | 3975 (1.05% of groups) | 11754 (2.02% of groups) |
| two-classes | bulk 4+2/host | 1 | +9.4% | 64 (1.34% of groups) | 247 (3.81% of groups) | 338 (4.59% of groups) |
| two-classes | bulk 4+2/host | 4 | +5.3% | 7 (0.04% of groups) | 158 (0.80% of groups) | 417 (1.66% of groups) |
| two-classes | bulk 4+2/host | 16 | +2.8% | 0 (0.00% of groups) | 145 (0.21% of groups) | 481 (0.63% of groups) |
| two-classes | bulk 4+2/host | 64 | +1.6% | 0 (0.00% of groups) | 0 (0.00% of groups) | 189 (0.07% of groups) |
| two-classes | fast r3/host | 1 | +8.8% | 37 (0.88% of groups) | 117 (2.49% of groups) | 156 (3.30% of groups) |
| two-classes | fast r3/host | 4 | +4.3% | 0 (0.00% of groups) | 142 (0.79% of groups) | 253 (1.38% of groups) |
| two-classes | fast r3/host | 16 | +2.5% | 0 (0.00% of groups) | 63 (0.10% of groups) | 246 (0.36% of groups) |
| two-classes | fast r3/host | 64 | +1.0% | 0 (0.00% of groups) | 0 (0.00% of groups) | 5 (0.00% of groups) |

## Fitted weights

16 groups a tablet. Placement weights, one a device, fitted over 24 rounds, and judged on a consumer they were not fitted to. They are fitted once to one consumer's groups, and once to a sample of 8 other consumers' groups, which is what a planner would fit to. Each fill cell is the fullest device over the mean. The exceptions are what the unfitted consumer still needs to be within 2%, before and after the sample's fit. The movement is what adding a device moves under `rendezvous, positions kept` once the sample's weights are fitted again from where they were, over the least that change could move: the cost of keeping the fit.

| shape | pool | rendezvous | fitted to one consumer, on another | fitted to the sample, on a consumer in it | fitted to the sample, on another | exceptions to 2%, before → after | add a device, fitted again |
| --- | --- | --- | --- | --- | --- | --- | --- |
| lab-2 | p r3/host | +0.8% | +1.3% | +0.1% | +0.8% | 0 → 0 | 1.00× (11.18%) |
| lab-2 | p 2+1/host | +0.8% | +1.3% | +0.1% | +0.8% | 0 → 0 | 1.00× (11.18%) |
| lab-2 | p 4+2/device | +0.0% | +0.0% | +0.0% | +0.0% | 0 → 0 | 1.00× (14.28%) |
| lab-fitted | p r3/host | +51.9% | +51.9% | +51.9% | +51.9% | stuck → stuck | 1.00× (6.03%) |
| lab-fitted | p 2+1/host | +51.9% | +51.9% | +51.9% | +51.9% | stuck → stuck | 1.00× (6.03%) |
| lab-fitted | p 3+1/device | +103.4% | +103.4% | +103.4% | +103.4% | stuck → stuck | 1.00× (13.61%) |
| 6x12 | p r3/host | +3.8% | +5.0% | +5.3% | +3.6% | 158 → 211 | 1.23× (1.69%) |
| 6x12 | p 4+2/host | +2.7% | +3.6% | +3.0% | +2.5% | 70 → 65 | 1.00× (1.30%) |
| 6x12 | p 8+3/device | +2.0% | +3.5% | +1.9% | +2.2% | 4 → 33 | 1.01× (1.38%) |
| 6x12-mixed | p 4+2/host | +5.1% | +4.0% | +4.6% | +6.0% | 108 → 150 | 1.00× (0.55%) |
| 6x12-mixed | p 8+3/device | +10.6% | +3.4% | +3.5% | +3.6% | 7708 → 141 | 1.03× (0.57%) |
| 6x12-uneven | p r3/host | +37.7% | +9.2% | +6.8% | +9.5% | 10809 → 435 | 1.16× (0.63%) |
| 6x12-uneven | p 4+2/host | +156.7% | +159.2% | +156.7% | +155.7% | stuck → stuck | 1.01× (1.29%) |
| 6x12-uneven | p 8+3/device | +11.1% | +4.0% | +3.0% | +3.4% | 7936 → 55 | 1.04× (0.56%) |
| 6x12-slices | p 4+2/host | +4.2% | +3.1% | +2.5% | +4.1% | 218 → 307 | 1.00× (1.28%) |
| 6x12-slices | p 8+3/device | +2.7% | +2.1% | +2.2% | +3.1% | 66 → 115 | 1.01× (1.39%) |
| 50x24 | p r3/host | +26.3% | +38.5% | +27.6% | +34.3% | 4472 → 4846 | 1.44× (0.14%) |
| 50x24 | p 8+3/host | +11.7% | +18.7% | +14.0% | +14.0% | 6219 → 6730 | 1.30× (0.12%) |
| 50x24 | p 10+4/host | +13.4% | +18.0% | +13.3% | +13.8% | 6587 → 7150 | 1.33× (0.12%) |
| two-classes | bulk 4+2/host | +3.0% | +3.1% | +1.8% | +3.0% | 147 → 141 | 1.00× (1.84%) |
| two-classes | fast r3/host | +1.6% | +2.1% | +1.4% | +1.7% | 0 → 0 | 1.20× (4.89%) |

## Movement

4 placement groups a tablet. Each change is made to host zero's first device of the pool's class. `least` is the chunks the change had to move: what the departed devices held, or what a new device ends up holding, or what a reweighted device gave up, under `rendezvous`. A cell is the chunks moved over that least, and the share of all the pool's chunks moved. An erasure coded pool counts a chunk that changed position as moved; a replicated pool counts only a changed set.

### lab-1

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | add | 8159 | 1.00× (25.00%) | 1.00× (16.60%) | 1.00× (16.81%) | 1.00× (16.36%) | 1.00× (16.41%) | 1.00× (16.60%) | 1.00× (16.36%) | 1.00× (16.60%) | 1.00× (16.67%) |
| p r3/host | remove | - | infeasible: 2 domains for a width of 3 | | | | | | | | |
| p r3/host | reweight ½ | 0 | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed |
| p r3/host | replace | 16384 | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) |
| p r3/host | replace, seat kept | 16384 | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) |
| p r3/host | host lost | - | infeasible: 2 domains for a width of 3 | | | | | | | | |
| p 2+1/host | add | 8159 | 3.00× (74.99%) | 1.67× (27.75%) | 1.78× (29.98%) | 1.99× (32.51%) | 2.05× (33.72%) | 1.00× (16.60%) | 1.00× (16.36%) | 1.00× (16.60%) | 1.00× (16.67%) |
| p 2+1/host | remove | - | infeasible: 2 domains for a width of 3 | | | | | | | | |
| p 2+1/host | reweight ½ | 0 | 0 moved, none needed | 10360 moved, none needed | 8583 moved, none needed | 10233 moved, none needed | 8687 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed |
| p 2+1/host | replace | 16384 | 1.00× (33.33%) | 1.67× (55.78%) | 1.69× (56.32%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) |
| p 2+1/host | replace, seat kept | 16384 | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) | 1.00× (33.33%) |
| p 2+1/host | host lost | - | infeasible: 2 domains for a width of 3 | | | | | | | | |

### lab-2

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | add | 5547 | 4.00× (57.13%) | 1.00× (11.29%) | 1.52× (16.93%) | 1.00× (10.78%) | 1.72× (19.09%) | 1.00× (11.29%) | 1.00× (10.78%) | 1.00× (11.29%) | 1.00× (11.11%) |
| p r3/host | remove | 8182 | 3.00× (49.99%) | 1.00× (16.65%) | 1.40× (23.67%) | 1.00× (16.68%) | 1.40× (22.97%) | 1.00× (16.65%) | 1.00× (16.68%) | 1.00× (16.65%) | 1.00× (16.67%) |
| p r3/host | reweight ½ | 2670 | 0 moved, none needed | 1.00× (5.43%) | 1.61× (9.16%) | 1.00× (5.51%) | 2.00× (10.76%) | 1.00× (5.43%) | 1.00× (5.51%) | 1.00× (5.43%) | 1.00× (5.56%) |
| p r3/host | replace | 8182 | 2.00× (33.33%) | 1.34× (22.27%) | 1.89× (32.06%) | 1.32× (22.08%) | 1.34× (22.00%) | 1.34× (22.27%) | 1.32× (22.08%) | 1.34× (22.27%) | 1.00× (16.67%) |
| p r3/host | replace, seat kept | 8182 | 2.00× (33.33%) | 1.00× (16.65%) | 1.00× (16.96%) | 1.00× (16.68%) | 1.00× (16.37%) | 1.00× (16.65%) | 1.00× (16.68%) | 1.00× (16.65%) | 1.00× (16.67%) |
| p r3/host | host lost | - | infeasible: 2 domains for a width of 3 | | | | | | | | |
| p 2+1/host | add | 5547 | 6.00× (85.69%) | 1.60× (18.09%) | 1.86× (20.77%) | 2.02× (21.74%) | 2.24× (24.79%) | 1.00× (11.29%) | 1.00× (10.78%) | 1.00× (11.29%) | 1.00× (11.11%) |
| p 2+1/host | remove | 8182 | 5.00× (83.33%) | 1.68× (27.94%) | 1.70× (28.84%) | 1.96× (32.73%) | 1.93× (31.61%) | 1.00× (16.65%) | 1.00× (16.68%) | 1.00× (16.65%) | 1.00× (16.67%) |
| p 2+1/host | reweight ½ | 2670 | 0 moved, none needed | 2.55× (13.85%) | 2.25× (12.83%) | 2.56× (14.12%) | 2.76× (14.87%) | 1.00× (5.43%) | 1.00× (5.51%) | 1.00× (5.43%) | 1.00× (5.56%) |
| p 2+1/host | replace | 8182 | 2.00× (33.33%) | 2.16× (35.88%) | 2.32× (39.27%) | 1.32× (22.08%) | 1.34× (22.00%) | 1.34× (22.27%) | 1.32× (22.08%) | 1.34× (22.27%) | 1.00× (16.67%) |
| p 2+1/host | replace, seat kept | 8182 | 2.00× (33.33%) | 1.00× (16.65%) | 1.00× (16.96%) | 1.00× (16.68%) | 1.00× (16.37%) | 1.00× (16.65%) | 1.00× (16.68%) | 1.00× (16.65%) | 1.00× (16.67%) |
| p 2+1/host | host lost | - | infeasible: 2 domains for a width of 3 | | | | | | | | |
| p 4+2/device | add | 14007 | 6.00× (85.70%) | 3.51× (50.06%) | 1.78× (25.42%) | 3.46× (49.34%) | 1.78× (25.30%) | 2.05× (29.28%) | 2.06× (29.42%) | 1.00× (14.25%) | 1.00× (14.29%) |
| p 4+2/device | remove | - | infeasible: 5 domains for a width of 6 | | | | | | | | |
| p 4+2/device | reweight ½ | 0 | 0 moved, none needed | 23024 moved, none needed | 14979 moved, none needed | 22768 moved, none needed | 14643 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed |
| p 4+2/device | replace | 16384 | 2.00× (33.33%) | 2.67× (44.44%) | 2.47× (41.17%) | 2.65× (44.17%) | 2.47× (41.12%) | 2.05× (34.22%) | 2.05× (34.22%) | 1.00× (16.67%) | 1.00× (16.67%) |
| p 4+2/device | replace, seat kept | 16384 | 2.00× (33.33%) | 1.00× (16.67%) | 1.00× (16.67%) | 1.00× (16.67%) | 1.00× (16.67%) | 1.00× (16.67%) | 1.00× (16.67%) | 1.00× (16.67%) | 1.00× (16.67%) |
| p 4+2/device | host lost | - | infeasible: 4 domains for a width of 6 | | | | | | | | |

### lab-fitted

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | add | 2982 | 2.00× (39.99%) | 1.00× (6.07%) | 1.00× (6.06%) | 1.00× (6.06%) | 1.13× (6.65%) | 1.00× (6.07%) | 1.00× (6.06%) | 1.00× (6.07%) | 1.00× (5.99%) |
| p r3/host | remove | 3624 | 1.00× (25.00%) | 1.00× (7.37%) | 1.00× (7.17%) | 1.00× (7.36%) | 1.00× (7.42%) | 1.00× (7.37%) | 1.00× (7.36%) | 1.00× (7.37%) | 1.00× (7.30%) |
| p r3/host | reweight ½ | 1578 | 0 moved, none needed | 1.00× (3.21%) | 1.00× (3.21%) | 1.00× (3.14%) | 1.12× (3.51%) | 1.00× (3.21%) | 1.00× (3.14%) | 1.00× (3.21%) | 1.00× (0.75%) |
| p r3/host | replace | 3624 | 1.33× (33.33%) | 1.64× (12.08%) | 1.66× (11.90%) | 1.64× (12.08%) | 1.60× (11.88%) | 1.64× (12.08%) | 1.64× (12.08%) | 1.64× (12.08%) | 1.00× (7.30%) |
| p r3/host | replace, seat kept | 3624 | 1.33× (33.33%) | 1.00× (7.37%) | 1.00× (7.17%) | 1.00× (7.36%) | 1.00× (7.42%) | 1.00× (7.37%) | 1.00× (7.36%) | 1.00× (7.37%) | 1.00× (7.30%) |
| p r3/host | host lost | - | infeasible: 2 domains for a width of 3 | | | | | | | | |
| p 2+1/host | add | 2982 | 4.00× (79.99%) | 1.41× (8.55%) | 1.82× (11.00%) | 1.76× (10.64%) | 2.19× (12.90%) | 1.00× (6.07%) | 1.00× (6.06%) | 1.00× (6.07%) | 1.00× (5.99%) |
| p 2+1/host | remove | 3624 | 3.00× (74.99%) | 1.49× (10.96%) | 1.82× (13.06%) | 1.81× (13.30%) | 2.07× (15.34%) | 1.00× (7.37%) | 1.00× (7.36%) | 1.00× (7.37%) | 1.00× (7.30%) |
| p 2+1/host | reweight ½ | 1578 | 0 moved, none needed | 1.83× (5.89%) | 1.97× (6.32%) | 1.93× (6.06%) | 2.32× (7.30%) | 1.00× (3.21%) | 1.00× (3.14%) | 1.00× (3.21%) | 1.00× (0.75%) |
| p 2+1/host | replace | 3624 | 2.00× (50.00%) | 2.33× (17.17%) | 2.94× (21.09%) | 1.64× (12.08%) | 1.60× (11.88%) | 1.64× (12.08%) | 1.64× (12.08%) | 1.64× (12.08%) | 1.00× (7.30%) |
| p 2+1/host | replace, seat kept | 3624 | 2.00× (50.00%) | 1.00× (7.37%) | 1.00× (7.17%) | 1.00× (7.36%) | 1.00× (7.42%) | 1.00× (7.37%) | 1.00× (7.36%) | 1.00× (7.37%) | 1.00× (7.30%) |
| p 2+1/host | host lost | - | infeasible: 2 domains for a width of 3 | | | | | | | | |
| p 3+1/device | add | 10807 | 4.00× (79.99%) | 2.19× (36.15%) | 1.43× (23.67%) | 2.18× (35.78%) | 1.42× (23.36%) | 1.76× (29.07%) | 1.76× (28.87%) | 1.00× (16.49%) | 1.00× (10.94%) |
| p 3+1/device | remove | - | infeasible: 3 domains for a width of 4 | | | | | | | | |
| p 3+1/device | reweight ½ | 0 | 0 moved, none needed | 11496 moved, none needed | 7216 moved, none needed | 11320 moved, none needed | 7224 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed | 0 moved, none needed |
| p 3+1/device | replace | 16384 | 2.00× (50.00%) | 1.96× (49.06%) | 1.70× (42.54%) | 1.97× (49.17%) | 1.69× (42.30%) | 1.77× (44.18%) | 1.77× (44.18%) | 1.00× (25.00%) | 1.00× (25.00%) |
| p 3+1/device | replace, seat kept | 16384 | 2.00× (50.00%) | 1.00× (25.00%) | 1.00× (25.00%) | 1.00× (25.00%) | 1.00× (25.00%) | 1.00× (25.00%) | 1.00× (25.00%) | 1.00× (25.00%) | 1.00× (25.00%) |
| p 3+1/device | host lost | - | infeasible: 2 domains for a width of 4 | | | | | | | | |

### 6x12

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | add | 664 | 69.61× (95.17%) | 1.00× (1.35%) | 1.45× (2.06%) | 1.62× (2.20%) | 2.11× (3.01%) | 1.00× (1.35%) | 1.62× (2.20%) | 1.00× (1.35%) | 1.00× (1.36%) |
| p r3/host | remove | 685 | 68.76× (95.40%) | 1.00× (1.39%) | 1.43× (2.00%) | 1.57× (2.26%) | 2.31× (3.15%) | 1.00× (1.39%) | 1.57× (2.26%) | 1.00× (1.39%) | 1.00× (1.39%) |
| p r3/host | reweight ½ | 344 | 0 moved, none needed | 1.00× (0.70%) | 1.46× (1.04%) | 1.58× (1.11%) | 2.22× (1.59%) | 1.00× (0.70%) | 1.58× (1.11%) | 1.00× (0.70%) | 1.00× (0.69%) |
| p r3/host | replace | 685 | 12.01× (16.66%) | 1.94× (2.70%) | 2.86× (4.00%) | 1.81× (2.60%) | 1.91× (2.61%) | 1.94× (2.70%) | 1.81× (2.60%) | 1.94× (2.70%) | 1.00× (1.39%) |
| p r3/host | replace, seat kept | 685 | 12.01× (16.66%) | 1.00× (1.39%) | 1.00× (1.40%) | 1.00× (1.44%) | 1.00× (1.37%) | 1.00× (1.39%) | 1.00× (1.44%) | 1.00× (1.39%) | 1.00× (1.39%) |
| p r3/host | host lost | 8253 | 5.75× (95.85%) | 1.00× (16.79%) | 1.30× (21.36%) | 1.00× (16.46%) | 1.31× (21.47%) | 1.00× (16.79%) | 1.00× (16.46%) | 1.00× (16.79%) | 1.00× (16.67%) |
| p 4+2/host | add | 1256 | 71.87× (98.26%) | 2.26× (2.89%) | 2.64× (3.36%) | 3.54× (4.53%) | 3.20× (4.20%) | 1.00× (1.28%) | 1.00× (1.28%) | 1.00× (1.28%) | 1.00× (1.28%) |
| p 4+2/host | remove | 1362 | 70.95× (98.37%) | 2.34× (3.24%) | 2.45× (3.40%) | 3.52× (4.97%) | 3.34× (4.53%) | 1.00× (1.39%) | 1.00× (1.41%) | 1.00× (1.39%) | 1.00× (1.39%) |
| p 4+2/host | reweight ½ | 655 | 0 moved, none needed | 3.00× (2.00%) | 2.57× (1.72%) | 3.69× (2.48%) | 3.36× (2.22%) | 1.00× (0.67%) | 1.00× (0.67%) | 1.00× (0.67%) | 1.00× (0.67%) |
| p 4+2/host | replace | 1362 | 12.02× (16.67%) | 4.24× (5.87%) | 4.70× (6.52%) | 1.83× (2.58%) | 1.90× (2.58%) | 1.85× (2.56%) | 1.83× (2.58%) | 1.85× (2.56%) | 1.00× (1.39%) |
| p 4+2/host | replace, seat kept | 1362 | 12.02× (16.67%) | 1.00× (1.39%) | 1.00× (1.39%) | 1.00× (1.41%) | 1.00× (1.36%) | 1.00× (1.39%) | 1.00× (1.41%) | 1.00× (1.39%) | 1.00× (1.39%) |
| p 4+2/host | host lost | - | infeasible: 5 domains for a width of 6 | | | | | | | | |
| p 8+3/device | add | 2470 | 71.88× (98.27%) | 6.00× (8.23%) | 1.16× (1.57%) | 5.91× (8.12%) | 1.13× (1.58%) | 2.52× (3.46%) | 2.53× (3.48%) | 1.00× (1.37%) | 1.00× (1.37%) |
| p 8+3/device | remove | 2488 | 70.98× (98.39%) | 6.05× (8.35%) | 1.14× (1.56%) | 5.98× (8.27%) | 1.15× (1.54%) | 2.52× (3.48%) | 2.52× (3.49%) | 1.00× (1.38%) | 1.00× (1.39%) |
| p 8+3/device | reweight ½ | 1173 | 0 moved, none needed | 7.16× (4.66%) | 1.24× (0.82%) | 6.94× (4.57%) | 1.26× (0.79%) | 2.49× (1.62%) | 2.55× (1.68%) | 1.00× (0.65%) | 1.00× (0.69%) |
| p 8+3/device | replace | 2488 | 12.02× (16.67%) | 10.84× (14.96%) | 2.26× (3.10%) | 10.71× (14.80%) | 2.32× (3.09%) | 4.66× (6.43%) | 4.63× (6.40%) | 1.84× (2.54%) | 1.00× (1.39%) |
| p 8+3/device | replace, seat kept | 2488 | 12.02× (16.67%) | 1.00× (1.38%) | 1.00× (1.38%) | 1.00× (1.38%) | 1.00× (1.34%) | 1.00× (1.38%) | 1.00× (1.38%) | 1.00× (1.38%) | 1.00× (1.39%) |
| p 8+3/device | host lost | 30255 | 5.92× (98.63%) | 3.70× (62.11%) | 1.12× (18.75%) | 3.74× (62.19%) | 1.13× (18.83%) | 1.98× (33.27%) | 1.99× (33.08%) | 1.00× (16.79%) | 1.00× (16.67%) |

### 6x12-mixed

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p 4+2/host | add | 527 | 71.87× (98.26%) | 2.26× (1.21%) | 2.38× (1.34%) | 3.51× (1.97%) | 3.17× (1.80%) | 1.00× (0.54%) | 1.00× (0.56%) | 1.00× (0.54%) | 1.00× (0.54%) |
| p 4+2/host | remove | 539 | 70.95× (98.37%) | 2.17× (1.19%) | 2.49× (1.31%) | 3.26× (1.92%) | 3.18× (1.82%) | 1.00× (0.55%) | 1.00× (0.59%) | 1.00× (0.55%) | 1.00× (0.56%) |
| p 4+2/host | reweight ½ | 295 | 0 moved, none needed | 2.47× (0.74%) | 2.53× (0.61%) | 3.46× (0.97%) | 3.40× (0.98%) | 1.00× (0.30%) | 1.00× (0.28%) | 1.00× (0.30%) | 1.00× (0.27%) |
| p 4+2/host | replace | 539 | 12.02× (16.67%) | 4.32× (2.37%) | 4.94× (2.61%) | 1.93× (1.14%) | 1.95× (1.11%) | 1.94× (1.07%) | 1.93× (1.14%) | 1.94× (1.07%) | 1.00× (0.56%) |
| p 4+2/host | replace, seat kept | 539 | 12.02× (16.67%) | 1.00× (0.55%) | 1.00× (0.53%) | 1.00× (0.59%) | 1.00× (0.57%) | 1.00× (0.55%) | 1.00× (0.59%) | 1.00× (0.55%) | 1.00× (0.56%) |
| p 4+2/host | host lost | - | infeasible: 5 domains for a width of 6 | | | | | | | | |
| p 8+3/device | add | 1100 | 71.88× (98.27%) | 5.93× (3.62%) | 1.12× (0.67%) | 5.79× (3.54%) | 1.11× (0.69%) | 2.50× (1.52%) | 2.51× (1.54%) | 1.00× (0.61%) | 1.00× (0.55%) |
| p 8+3/device | remove | 1069 | 70.98× (98.39%) | 5.84× (3.47%) | 1.12× (0.65%) | 5.91× (3.62%) | 1.12× (0.68%) | 2.49× (1.48%) | 2.48× (1.52%) | 1.00× (0.59%) | 1.00× (0.56%) |
| p 8+3/device | reweight ½ | 537 | 0 moved, none needed | 6.77× (2.02%) | 1.15× (0.31%) | 6.74× (2.01%) | 1.15× (0.32%) | 2.49× (0.74%) | 2.50× (0.74%) | 1.00× (0.30%) | 1.00× (0.28%) |
| p 8+3/device | replace | 1069 | 12.02× (16.67%) | 11.46× (6.80%) | 2.26× (1.31%) | 11.17× (6.85%) | 2.24× (1.37%) | 4.90× (2.91%) | 4.80× (2.94%) | 1.96× (1.16%) | 1.00× (0.56%) |
| p 8+3/device | replace, seat kept | 1069 | 12.02× (16.67%) | 1.00× (0.59%) | 1.00× (0.58%) | 1.00× (0.61%) | 1.00× (0.61%) | 1.00× (0.59%) | 1.00× (0.61%) | 1.00× (0.59%) | 1.00× (0.56%) |
| p 8+3/device | host lost | 30005 | 5.92× (98.63%) | 3.73× (62.12%) | 1.17× (19.30%) | 3.72× (62.20%) | 1.17× (19.45%) | 2.00× (33.29%) | 1.99× (33.30%) | 1.00× (16.65%) | 1.00× (16.67%) |

### 6x12-uneven

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | add | 341 | 69.61× (95.17%) | 1.00× (0.69%) | 1.31× (0.91%) | 1.72× (1.21%) | 2.20× (1.53%) | 1.00× (0.69%) | 1.72× (1.21%) | 1.00× (0.69%) | 1.00× (0.55%) |
| p r3/host | remove | 357 | 68.76× (95.40%) | 1.00× (0.73%) | 1.27× (0.95%) | 1.69× (1.29%) | 2.24× (1.58%) | 1.00× (0.73%) | 1.69× (1.29%) | 1.00× (0.73%) | 1.00× (0.56%) |
| p r3/host | reweight ½ | 163 | 0 moved, none needed | 1.00× (0.33%) | 1.33× (0.43%) | 1.69× (0.65%) | 2.13× (0.84%) | 1.00× (0.33%) | 1.69× (0.65%) | 1.00× (0.33%) | 1.00× (0.28%) |
| p r3/host | replace | 357 | 12.01× (16.66%) | 1.94× (1.41%) | 2.48× (1.84%) | 1.78× (1.37%) | 1.82× (1.29%) | 1.94× (1.41%) | 1.78× (1.37%) | 1.94× (1.41%) | 1.00× (0.56%) |
| p r3/host | replace, seat kept | 357 | 12.01× (16.66%) | 1.00× (0.73%) | 1.00× (0.74%) | 1.00× (0.76%) | 1.00× (0.71%) | 1.00× (0.73%) | 1.00× (0.76%) | 1.00× (0.73%) | 1.00× (0.56%) |
| p r3/host | host lost | 4160 | 5.75× (95.85%) | 1.00× (8.46%) | 1.23× (10.60%) | 1.00× (8.76%) | 1.24× (10.68%) | 1.00× (8.46%) | 1.00× (8.76%) | 1.00× (8.46%) | 1.00× (6.67%) |
| p 4+2/host | add | 1285 | 71.87× (98.26%) | 2.03× (2.65%) | 1.82× (2.29%) | 2.93× (3.69%) | 2.68× (3.36%) | 1.00× (1.31%) | 1.00× (1.26%) | 1.00× (1.31%) | 1.00× (0.55%) |
| p 4+2/host | remove | 1406 | 70.95× (98.37%) | 2.00× (2.87%) | 1.70× (2.50%) | 2.92× (4.02%) | 2.52× (3.67%) | 1.00× (1.43%) | 1.00× (1.37%) | 1.00× (1.43%) | 1.00× (1.39%) |
| p 4+2/host | reweight ½ | 670 | 0 moved, none needed | 2.63× (1.79%) | 1.76× (1.26%) | 2.96× (2.05%) | 2.69× (1.86%) | 1.00× (0.68%) | 1.00× (0.69%) | 1.00× (0.68%) | 1.00× (0.67%) |
| p 4+2/host | replace | 1406 | 12.02× (16.67%) | 3.74× (5.35%) | 3.14× (4.62%) | 1.84× (2.52%) | 1.79× (2.60%) | 1.84× (2.64%) | 1.84× (2.52%) | 1.84× (2.64%) | 1.00× (1.39%) |
| p 4+2/host | replace, seat kept | 1406 | 12.02× (16.67%) | 1.00× (1.43%) | 1.00× (1.47%) | 1.00× (1.37%) | 1.00× (1.45%) | 1.00× (1.43%) | 1.00× (1.37%) | 1.00× (1.43%) | 1.00× (1.39%) |
| p 4+2/host | host lost | - | infeasible: 5 domains for a width of 6 | | | | | | | | |
| p 8+3/device | add | 1080 | 71.88× (98.27%) | 5.67× (3.40%) | 1.14× (0.66%) | 5.88× (3.47%) | 1.11× (0.65%) | 2.53× (1.52%) | 2.50× (1.48%) | 1.00× (0.60%) | 1.00× (0.55%) |
| p 8+3/device | remove | 1094 | 70.98× (98.39%) | 5.93× (3.60%) | 1.12× (0.69%) | 5.71× (3.35%) | 1.12× (0.65%) | 2.53× (1.54%) | 2.53× (1.48%) | 1.00× (0.61%) | 1.00× (0.56%) |
| p 8+3/device | reweight ½ | 543 | 0 moved, none needed | 6.64× (2.00%) | 1.15× (0.35%) | 6.41× (1.94%) | 1.16× (0.36%) | 2.55× (0.77%) | 2.50× (0.76%) | 1.00× (0.30%) | 1.00× (0.28%) |
| p 8+3/device | replace | 1094 | 12.02× (16.67%) | 11.08× (6.72%) | 2.19× (1.34%) | 11.07× (6.49%) | 2.20× (1.29%) | 4.87× (2.96%) | 4.84× (2.84%) | 1.93× (1.17%) | 1.00× (0.56%) |
| p 8+3/device | replace, seat kept | 1094 | 12.02× (16.67%) | 1.00× (0.61%) | 1.00× (0.61%) | 1.00× (0.59%) | 1.00× (0.59%) | 1.00× (0.61%) | 1.00× (0.59%) | 1.00× (0.61%) | 1.00× (0.56%) |
| p 8+3/device | host lost | 12910 | 5.92× (98.63%) | 4.73× (33.89%) | 1.12× (7.98%) | 4.78× (34.12%) | 1.12× (7.98%) | 2.28× (16.34%) | 2.26× (16.12%) | 1.00× (7.16%) | 1.00× (6.67%) |

### 6x12-slices

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p 4+2/host | add | 1259 | 180.07× (98.92%) | 2.29× (2.94%) | 2.56× (3.27%) | 3.46× (4.42%) | 3.48× (4.32%) | 1.00× (1.28%) | 1.00× (1.28%) | 1.00× (1.28%) | 1.00× (1.28%) |
| p 4+2/host | remove | 1367 | 178.14× (99.13%) | 2.35× (3.26%) | 2.46× (3.54%) | 3.37× (4.55%) | 3.21× (4.41%) | 1.00× (1.39%) | 1.00× (1.35%) | 1.00× (1.39%) | 1.00× (1.39%) |
| p 4+2/host | reweight ½ | 654 | 0 moved, none needed | 3.05× (2.03%) | 2.62× (1.74%) | 3.41× (2.24%) | 3.45× (2.16%) | 1.00× (0.67%) | 1.00× (0.66%) | 1.00× (0.67%) | 1.00× (0.67%) |
| p 4+2/host | replace | 1367 | 29.95× (16.67%) | 4.26× (5.92%) | 4.53× (6.52%) | 1.86× (2.51%) | 1.83× (2.52%) | 1.84× (2.56%) | 1.86× (2.51%) | 1.84× (2.56%) | 1.00× (1.39%) |
| p 4+2/host | replace, seat kept | 1367 | 29.95× (16.67%) | 1.00× (1.39%) | 1.00× (1.44%) | 1.00× (1.35%) | 1.00× (1.37%) | 1.00× (1.39%) | 1.00× (1.35%) | 1.00× (1.39%) | 1.00× (1.39%) |
| p 4+2/host | host lost | - | infeasible: 5 domains for a width of 6 | | | | | | | | |
| p 8+3/device | add | 2474 | 180.10× (98.93%) | 6.05× (8.30%) | 1.15× (1.55%) | 6.02× (8.21%) | 1.13× (1.53%) | 2.52× (3.47%) | 2.53× (3.45%) | 1.00× (1.37%) | 1.00× (1.37%) |
| p 8+3/device | remove | 2520 | 178.31× (99.14%) | 5.95× (8.32%) | 1.14× (1.65%) | 5.99× (8.17%) | 1.13× (1.61%) | 2.56× (3.57%) | 2.52× (3.44%) | 1.00× (1.40%) | 1.00× (1.39%) |
| p 8+3/device | reweight ½ | 1203 | 0 moved, none needed | 7.06× (4.71%) | 1.20× (0.82%) | 7.02× (4.53%) | 1.21× (0.83%) | 2.52× (1.68%) | 2.55× (1.65%) | 1.00× (0.67%) | 1.00× (0.69%) |
| p 8+3/device | replace | 2520 | 29.98× (16.67%) | 10.66× (14.91%) | 2.18× (3.16%) | 10.92× (14.89%) | 2.18× (3.11%) | 4.64× (6.49%) | 4.70× (6.41%) | 1.83× (2.55%) | 1.00× (1.39%) |
| p 8+3/device | replace, seat kept | 2520 | 29.98× (16.67%) | 1.00× (1.40%) | 1.00× (1.45%) | 1.00× (1.36%) | 1.00× (1.42%) | 1.00× (1.40%) | 1.00× (1.36%) | 1.00× (1.40%) | 1.00× (1.39%) |
| p 8+3/device | host lost | 30026 | 5.97× (99.45%) | 3.72× (61.91%) | 1.12× (18.70%) | 3.72× (61.62%) | 1.13× (18.79%) | 1.99× (33.15%) | 1.99× (32.94%) | 1.00× (16.66%) | 1.00× (16.67%) |

### 50x24

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| p r3/host | add | 57 | 1075.77× (85.36%) | 1.00× (0.12%) | 1.07× (0.09%) | 2.00× (0.14%) | 2.10× (0.22%) | 1.00× (0.12%) | 2.00× (0.14%) | 1.00× (0.12%) | 1.00× (0.08%) |
| p r3/host | remove | 47 | 1056.10× (85.95%) | 1.00× (0.10%) | 1.05× (0.09%) | 1.77× (0.13%) | 1.79× (0.15%) | 1.00× (0.10%) | 1.77× (0.13%) | 1.00× (0.10%) | 1.00× (0.08%) |
| p r3/host | reweight ½ | 27 | 0 moved, none needed | 1.00× (0.05%) | 1.09× (0.05%) | 2.27× (0.05%) | 1.76× (0.08%) | 1.00× (0.05%) | 2.27× (0.05%) | 1.00× (0.05%) | 1.00× (0.04%) |
| p r3/host | replace | 47 | 24.55× (2.00%) | 2.21× (0.21%) | 2.10× (0.17%) | 1.94× (0.14%) | 2.12× (0.18%) | 2.21× (0.21%) | 1.94× (0.14%) | 2.21× (0.21%) | 1.00× (0.08%) |
| p r3/host | replace, seat kept | 47 | 24.55× (2.00%) | 1.00× (0.10%) | 1.00× (0.08%) | 1.00× (0.07%) | 1.00× (0.09%) | 1.00× (0.10%) | 1.00× (0.07%) | 1.00× (0.10%) | 1.00× (0.08%) |
| p r3/host | host lost | 989 | 49.90× (99.70%) | 1.00× (2.01%) | 1.04× (2.01%) | 1.00× (1.91%) | 1.04× (2.06%) | 1.00× (2.01%) | 1.00× (1.91%) | 1.00× (2.01%) | 1.00× (2.00%) |
| p 8+3/host | add | 170 | 1168.38× (92.71%) | 5.88× (0.55%) | 1.25× (0.11%) | 11.20× (0.88%) | 2.24× (0.20%) | 2.28× (0.22%) | 3.12× (0.24%) | 1.00× (0.09%) | 1.00× (0.08%) |
| p 8+3/host | remove | 156 | 1165.78× (93.15%) | 5.88× (0.51%) | 1.31× (0.11%) | 9.21× (0.84%) | 2.20× (0.18%) | 2.17× (0.19%) | 3.18× (0.29%) | 1.00× (0.09%) | 1.00× (0.08%) |
| p 8+3/host | reweight ½ | 71 | 0 moved, none needed | 7.62× (0.30%) | 1.28× (0.06%) | 9.65× (0.45%) | 2.09× (0.09%) | 2.04× (0.08%) | 2.79× (0.13%) | 1.00× (0.04%) | 1.00× (0.04%) |
| p 8+3/host | replace | 156 | 24.99× (2.00%) | 12.21× (1.06%) | 2.55× (0.22%) | 1.79× (0.16%) | 2.05× (0.17%) | 4.64× (0.40%) | 1.79× (0.16%) | 2.08× (0.18%) | 1.00× (0.08%) |
| p 8+3/host | replace, seat kept | 156 | 24.99× (2.00%) | 1.00× (0.09%) | 1.00× (0.09%) | 1.00× (0.09%) | 1.00× (0.08%) | 1.00× (0.09%) | 1.00× (0.09%) | 1.00× (0.09%) | 1.00× (0.08%) |
| p 8+3/host | host lost | 3655 | 50.09× (100.00%) | 6.00× (12.17%) | 1.21× (2.37%) | 5.86× (11.69%) | 1.22× (2.42%) | 2.54× (5.16%) | 2.54× (5.06%) | 1.00× (2.03%) | 1.00× (2.01%) |
| p 10+4/host | add | 214 | 1168.50× (92.72%) | 7.07× (0.66%) | 1.36× (0.12%) | 13.08× (1.06%) | 2.41× (0.20%) | 2.33× (0.22%) | 3.16× (0.25%) | 1.00× (0.09%) | 1.00× (0.08%) |
| p 10+4/host | remove | 197 | 1167.63× (93.16%) | 6.94× (0.60%) | 1.43× (0.12%) | 11.30× (1.04%) | 2.24× (0.19%) | 2.28× (0.20%) | 3.20× (0.29%) | 1.00× (0.09%) | 1.00× (0.08%) |
| p 10+4/host | reweight ½ | 91 | 0 moved, none needed | 8.43× (0.33%) | 1.42× (0.06%) | 10.86× (0.55%) | 2.17× (0.10%) | 2.27× (0.09%) | 3.16× (0.16%) | 1.00× (0.04%) | 1.00× (0.04%) |
| p 10+4/host | replace | 197 | 25.02× (2.00%) | 14.51× (1.25%) | 2.83× (0.24%) | 1.82× (0.17%) | 1.93× (0.16%) | 4.77× (0.41%) | 1.82× (0.17%) | 2.07× (0.18%) | 1.00× (0.08%) |
| p 10+4/host | replace, seat kept | 197 | 25.02× (2.00%) | 1.00× (0.09%) | 1.00× (0.08%) | 1.00× (0.09%) | 1.00× (0.08%) | 1.00× (0.09%) | 1.00× (0.09%) | 1.00× (0.09%) | 1.00× (0.08%) |
| p 10+4/host | host lost | 4632 | 50.09× (100.00%) | 7.52× (15.19%) | 1.28× (2.52%) | 7.34× (14.75%) | 1.28× (2.52%) | 2.73× (5.50%) | 2.76× (5.55%) | 1.00× (2.02%) | 1.00× (2.01%) |

### two-classes

| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bulk 4+2/host | add | 1780 | 48.06× (97.96%) | 2.34× (4.23%) | 2.59× (4.62%) | 3.42× (6.20%) | 3.27× (6.18%) | 1.00× (1.81%) | 1.00× (1.81%) | 1.00× (1.81%) | 1.00× (1.85%) |
| bulk 4+2/host | remove | 2090 | 46.96× (97.79%) | 2.36× (5.03%) | 2.44× (5.22%) | 3.36× (7.08%) | 3.03× (6.56%) | 1.00× (2.13%) | 1.00× (2.10%) | 1.00× (2.13%) | 1.00× (2.08%) |
| bulk 4+2/host | reweight ½ | 932 | 0 moved, none needed | 3.17× (3.01%) | 2.56× (2.62%) | 3.64× (3.55%) | 3.21× (3.21%) | 1.00× (0.95%) | 1.00× (0.98%) | 1.00× (0.95%) | 1.00× (0.97%) |
| bulk 4+2/host | replace | 2090 | 8.00× (16.67%) | 4.09× (8.69%) | 4.36× (9.33%) | 1.74× (3.67%) | 1.77× (3.82%) | 1.75× (3.71%) | 1.74× (3.67%) | 1.75× (3.71%) | 1.00× (2.08%) |
| bulk 4+2/host | replace, seat kept | 2090 | 8.00× (16.67%) | 1.00× (2.13%) | 1.00× (2.14%) | 1.00× (2.10%) | 1.00× (2.16%) | 1.00× (2.13%) | 1.00× (2.10%) | 1.00× (2.13%) | 1.00× (2.08%) |
| bulk 4+2/host | host lost | - | infeasible: 5 domains for a width of 6 | | | | | | | | |
| fast r3/host | add | 1860 | 23.37× (93.27%) | 1.00× (3.78%) | 1.46× (5.43%) | 1.46× (5.52%) | 2.08× (8.09%) | 1.00× (3.78%) | 1.46× (5.52%) | 1.00× (3.78%) | 1.00× (3.92%) |
| fast r3/host | remove | 2064 | 22.36× (93.26%) | 1.00× (4.20%) | 1.39× (5.93%) | 1.52× (6.44%) | 2.08× (8.70%) | 1.00× (4.20%) | 1.52× (6.44%) | 1.00× (4.20%) | 1.00× (4.17%) |
| fast r3/host | reweight ½ | 944 | 0 moved, none needed | 1.00× (1.92%) | 1.47× (2.99%) | 1.48× (3.04%) | 2.14× (4.37%) | 1.00× (1.92%) | 1.48× (3.04%) | 1.00× (1.92%) | 1.00× (2.02%) |
| fast r3/host | replace | 2064 | 4.00× (16.66%) | 1.81× (7.59%) | 2.54× (10.80%) | 1.59× (6.74%) | 1.60× (6.68%) | 1.81× (7.59%) | 1.59× (6.74%) | 1.81× (7.59%) | 1.00× (4.17%) |
| fast r3/host | replace, seat kept | 2064 | 4.00× (16.66%) | 1.00× (4.20%) | 1.00× (4.25%) | 1.00× (4.24%) | 1.00× (4.17%) | 1.00× (4.20%) | 1.00× (4.24%) | 1.00× (4.20%) | 1.00× (4.17%) |
| fast r3/host | host lost | 8192 | 5.63× (93.75%) | 1.00× (16.67%) | 1.29× (21.41%) | 1.00× (16.89%) | 1.30× (21.76%) | 1.00× (16.67%) | 1.00× (16.89%) | 1.00× (16.67%) | 1.00× (16.67%) |

## Feasibility

Groups whose answer breaks the domain rule, which only the window can give; and for the two by-position candidates the mean rounds drawn and the groups that ran out of rounds and fell back.

| shape | pool | groups a tablet | window breaks the rule | by position: rounds, fell back | by domain, by position: rounds, fell back |
| --- | --- | --- | --- | --- | --- |
| lab-1 | p r3/host | 1 | 0 (0.0%) | 3.42, 0 | 3.39, 0 |
| lab-1 | p r3/host | 4 | 0 (0.0%) | 3.33, 0 | 3.35, 0 |
| lab-1 | p r3/host | 16 | 0 (0.0%) | 3.37, 0 | 3.36, 0 |
| lab-1 | p r3/host | 64 | 0 (0.0%) | 3.37, 1 | 3.37, 2 |
| lab-1 | p 2+1/host | 1 | 0 (0.0%) | 3.42, 0 | 3.39, 0 |
| lab-1 | p 2+1/host | 4 | 0 (0.0%) | 3.33, 0 | 3.35, 0 |
| lab-1 | p 2+1/host | 16 | 0 (0.0%) | 3.37, 0 | 3.36, 0 |
| lab-1 | p 2+1/host | 64 | 0 (0.0%) | 3.37, 1 | 3.37, 2 |
| lab-2 | p r3/host | 1 | 0 (0.0%) | 3.39, 0 | 3.41, 0 |
| lab-2 | p r3/host | 4 | 0 (0.0%) | 3.38, 0 | 3.39, 0 |
| lab-2 | p r3/host | 16 | 0 (0.0%) | 3.39, 0 | 3.37, 0 |
| lab-2 | p r3/host | 64 | 0 (0.0%) | 3.39, 0 | 3.37, 0 |
| lab-2 | p 2+1/host | 1 | 0 (0.0%) | 3.39, 0 | 3.41, 0 |
| lab-2 | p 2+1/host | 4 | 0 (0.0%) | 3.38, 0 | 3.39, 0 |
| lab-2 | p 2+1/host | 16 | 0 (0.0%) | 3.39, 0 | 3.37, 0 |
| lab-2 | p 2+1/host | 64 | 0 (0.0%) | 3.39, 0 | 3.37, 0 |
| lab-2 | p 4+2/device | 1 | 0 (0.0%) | 8.03, 20 | 7.94, 17 |
| lab-2 | p 4+2/device | 4 | 0 (0.0%) | 7.92, 74 | 7.92, 68 |
| lab-2 | p 4+2/device | 16 | 0 (0.0%) | 7.93, 302 | 7.92, 283 |
| lab-2 | p 4+2/device | 64 | 0 (0.0%) | 7.90, 1099 | 7.93, 1168 |
| lab-fitted | p r3/host | 1 | 2048 (50.0%) | 4.73, 2 | 4.76, 0 |
| lab-fitted | p r3/host | 4 | 8192 (50.0%) | 4.80, 8 | 4.77, 6 |
| lab-fitted | p r3/host | 16 | 32768 (50.0%) | 4.79, 31 | 4.77, 25 |
| lab-fitted | p r3/host | 64 | 131072 (50.0%) | 4.78, 102 | 4.78, 101 |
| lab-fitted | p 2+1/host | 1 | 2048 (50.0%) | 4.73, 2 | 4.76, 0 |
| lab-fitted | p 2+1/host | 4 | 8192 (50.0%) | 4.80, 8 | 4.77, 6 |
| lab-fitted | p 2+1/host | 16 | 32768 (50.0%) | 4.79, 31 | 4.77, 25 |
| lab-fitted | p 2+1/host | 64 | 131072 (50.0%) | 4.78, 102 | 4.78, 101 |
| lab-fitted | p 3+1/device | 1 | 0 (0.0%) | 7.10, 36 | 7.20, 39 |
| lab-fitted | p 3+1/device | 4 | 0 (0.0%) | 7.14, 130 | 7.27, 167 |
| lab-fitted | p 3+1/device | 16 | 0 (0.0%) | 7.20, 569 | 7.23, 606 |
| lab-fitted | p 3+1/device | 64 | 0 (0.0%) | 7.20, 2285 | 7.18, 2293 |
| 6x12 | p r3/host | 1 | 0 (0.0%) | 1.67, 0 | 1.70, 0 |
| 6x12 | p r3/host | 4 | 0 (0.0%) | 1.66, 0 | 1.68, 0 |
| 6x12 | p r3/host | 16 | 0 (0.0%) | 1.67, 0 | 1.67, 0 |
| 6x12 | p r3/host | 64 | 0 (0.0%) | 1.67, 0 | 1.67, 0 |
| 6x12 | p 4+2/host | 1 | 0 (0.0%) | 7.98, 14 | 7.86, 15 |
| 6x12 | p 4+2/host | 4 | 0 (0.0%) | 7.87, 65 | 7.95, 58 |
| 6x12 | p 4+2/host | 16 | 0 (0.0%) | 7.89, 290 | 7.91, 275 |
| 6x12 | p 4+2/host | 64 | 0 (0.0%) | 7.90, 1130 | 7.90, 1102 |
| 6x12 | p 8+3/device | 1 | 0 (0.0%) | 1.67, 0 | 1.68, 0 |
| 6x12 | p 8+3/device | 4 | 0 (0.0%) | 1.66, 0 | 1.67, 0 |
| 6x12 | p 8+3/device | 16 | 0 (0.0%) | 1.66, 0 | 1.66, 0 |
| 6x12 | p 8+3/device | 64 | 0 (0.0%) | 1.66, 0 | 1.66, 0 |
| 6x12-mixed | p 4+2/host | 1 | 0 (0.0%) | 7.94, 20 | 8.09, 18 |
| 6x12-mixed | p 4+2/host | 4 | 0 (0.0%) | 7.98, 83 | 7.94, 64 |
| 6x12-mixed | p 4+2/host | 16 | 0 (0.0%) | 7.91, 308 | 7.90, 291 |
| 6x12-mixed | p 4+2/host | 64 | 0 (0.0%) | 7.92, 1180 | 7.90, 1131 |
| 6x12-mixed | p 8+3/device | 1 | 0 (0.0%) | 1.88, 0 | 1.88, 0 |
| 6x12-mixed | p 8+3/device | 4 | 0 (0.0%) | 1.87, 0 | 1.88, 0 |
| 6x12-mixed | p 8+3/device | 16 | 0 (0.0%) | 1.87, 0 | 1.88, 0 |
| 6x12-mixed | p 8+3/device | 64 | 0 (0.0%) | 1.87, 0 | 1.87, 0 |
| 6x12-uneven | p r3/host | 1 | 0 (0.0%) | 2.15, 0 | 2.12, 0 |
| 6x12-uneven | p r3/host | 4 | 0 (0.0%) | 2.13, 0 | 2.12, 0 |
| 6x12-uneven | p r3/host | 16 | 0 (0.0%) | 2.12, 0 | 2.11, 0 |
| 6x12-uneven | p r3/host | 64 | 0 (0.0%) | 2.10, 0 | 2.11, 0 |
| 6x12-uneven | p 4+2/host | 1 | 0 (0.0%) | 16.45, 611 | 15.76, 561 |
| 6x12-uneven | p 4+2/host | 4 | 0 (0.0%) | 16.31, 2395 | 15.96, 2272 |
| 6x12-uneven | p 4+2/host | 16 | 0 (0.0%) | 16.22, 9468 | 16.09, 9297 |
| 6x12-uneven | p 4+2/host | 64 | 0 (0.0%) | 16.17, 37571 | 16.16, 37465 |
| 6x12-uneven | p 8+3/device | 1 | 0 (0.0%) | 1.87, 0 | 1.89, 0 |
| 6x12-uneven | p 8+3/device | 4 | 0 (0.0%) | 1.88, 0 | 1.87, 0 |
| 6x12-uneven | p 8+3/device | 16 | 0 (0.0%) | 1.87, 0 | 1.88, 0 |
| 6x12-uneven | p 8+3/device | 64 | 0 (0.0%) | 1.87, 0 | 1.87, 0 |
| 6x12-slices | p 4+2/host | 1 | 0 (0.0%) | 7.92, 21 | 8.02, 18 |
| 6x12-slices | p 4+2/host | 4 | 0 (0.0%) | 7.88, 60 | 7.89, 64 |
| 6x12-slices | p 4+2/host | 16 | 0 (0.0%) | 7.90, 291 | 7.91, 285 |
| 6x12-slices | p 4+2/host | 64 | 0 (0.0%) | 7.90, 1149 | 7.91, 1127 |
| 6x12-slices | p 8+3/device | 1 | 3006 (73.4%) | 1.68, 0 | 1.67, 0 |
| 6x12-slices | p 8+3/device | 4 | 12014 (73.3%) | 1.67, 0 | 1.67, 0 |
| 6x12-slices | p 8+3/device | 16 | 48062 (73.3%) | 1.66, 0 | 1.66, 0 |
| 6x12-slices | p 8+3/device | 64 | 192238 (73.3%) | 1.66, 0 | 1.66, 0 |
| 50x24 | p r3/host | 1 | 0 (0.0%) | 1.06, 0 | 1.06, 0 |
| 50x24 | p r3/host | 4 | 0 (0.0%) | 1.06, 0 | 1.06, 0 |
| 50x24 | p r3/host | 16 | 0 (0.0%) | 1.06, 0 | 1.06, 0 |
| 50x24 | p r3/host | 64 | 0 (0.0%) | 1.06, 0 | 1.06, 0 |
| 50x24 | p 8+3/host | 1 | 0 (0.0%) | 1.94, 0 | 1.91, 0 |
| 50x24 | p 8+3/host | 4 | 0 (0.0%) | 1.94, 0 | 1.93, 0 |
| 50x24 | p 8+3/host | 16 | 0 (0.0%) | 1.93, 0 | 1.93, 0 |
| 50x24 | p 8+3/host | 64 | 0 (0.0%) | 1.93, 0 | 1.93, 0 |
| 50x24 | p 10+4/host | 1 | 0 (0.0%) | 2.37, 0 | 2.35, 0 |
| 50x24 | p 10+4/host | 4 | 0 (0.0%) | 2.38, 0 | 2.35, 0 |
| 50x24 | p 10+4/host | 16 | 0 (0.0%) | 2.36, 0 | 2.36, 0 |
| 50x24 | p 10+4/host | 64 | 0 (0.0%) | 2.36, 0 | 2.35, 0 |
| two-classes | bulk 4+2/host | 1 | 0 (0.0%) | 7.98, 23 | 7.97, 18 |
| two-classes | bulk 4+2/host | 4 | 0 (0.0%) | 7.95, 72 | 7.89, 74 |
| two-classes | bulk 4+2/host | 16 | 0 (0.0%) | 7.90, 252 | 7.88, 278 |
| two-classes | bulk 4+2/host | 64 | 0 (0.0%) | 7.91, 1047 | 7.90, 1082 |
| two-classes | fast r3/host | 1 | 0 (0.0%) | 1.67, 0 | 1.66, 0 |
| two-classes | fast r3/host | 4 | 0 (0.0%) | 1.67, 0 | 1.66, 0 |
| two-classes | fast r3/host | 16 | 0 (0.0%) | 1.67, 0 | 1.67, 0 |
| two-classes | fast r3/host | 64 | 0 (0.0%) | 1.67, 0 | 1.67, 0 |

## The table's logarithm against libm's

Every chunk of every group at the largest count a tablet, placed by `rendezvous` with the fixed-point table and again with `f64::ln`.

| shape | pool | chunks | chosen differently |
| --- | --- | --- | --- |
| lab-1 | p r3/host | 786432 | 0 |
| lab-1 | p 2+1/host | 786432 | 0 |
| lab-2 | p r3/host | 786432 | 0 |
| lab-2 | p 2+1/host | 786432 | 0 |
| lab-2 | p 4+2/device | 1572864 | 0 |
| lab-fitted | p r3/host | 786432 | 0 |
| lab-fitted | p 2+1/host | 786432 | 0 |
| lab-fitted | p 3+1/device | 1048576 | 0 |
| 6x12 | p r3/host | 786432 | 0 |
| 6x12 | p 4+2/host | 1572864 | 0 |
| 6x12 | p 8+3/device | 2883584 | 0 |
| 6x12-mixed | p 4+2/host | 1572864 | 0 |
| 6x12-mixed | p 8+3/device | 2883584 | 2 |
| 6x12-uneven | p r3/host | 786432 | 0 |
| 6x12-uneven | p 4+2/host | 1572864 | 0 |
| 6x12-uneven | p 8+3/device | 2883584 | 2 |
| 6x12-slices | p 4+2/host | 1572864 | 0 |
| 6x12-slices | p 8+3/device | 2883584 | 0 |
| 50x24 | p r3/host | 786432 | 0 |
| 50x24 | p 8+3/host | 2883584 | 0 |
| 50x24 | p 10+4/host | 3670016 | 0 |
| two-classes | bulk 4+2/host | 1572864 | 0 |
| two-classes | fast r3/host | 786432 | 0 |

## A bucket of few stripes

`rendezvous` on lab-2, each placement group holding a Poisson number of stripes with the mean the bucket's stripes over its groups. The last column is the placement alone, every group holding the same.

| pool | groups a tablet | 10⁴ stripes | 10⁵ | 10⁶ | 10⁷ | placement alone |
| --- | --- | --- | --- | --- | --- | --- |
| p r3/host | 1 | +4.2% | +3.8% | +3.7% | +3.7% | +3.7% |
| p r3/host | 4 | +0.7% | +0.8% | +1.0% | +1.0% | +0.9% |
| p r3/host | 16 | +0.7% | +0.6% | +0.3% | +0.5% | +0.5% |
| p r3/host | 64 | +1.2% | +0.3% | +0.2% | +0.3% | +0.3% |
| p 2+1/host | 1 | +4.2% | +3.8% | +3.7% | +3.7% | +3.7% |
| p 2+1/host | 4 | +0.7% | +0.8% | +1.0% | +1.0% | +0.9% |
| p 2+1/host | 16 | +0.7% | +0.6% | +0.3% | +0.5% | +0.5% |
| p 2+1/host | 64 | +1.2% | +0.3% | +0.2% | +0.3% | +0.3% |
| p 4+2/device | 1 | +0.0% | +0.0% | +0.0% | +0.0% | +0.0% |
| p 4+2/device | 4 | +0.0% | +0.0% | +0.0% | +0.0% | +0.0% |
| p 4+2/device | 16 | +0.0% | +0.0% | +0.0% | +0.0% | +0.0% |
| p 4+2/device | 64 | +0.0% | +0.0% | +0.0% | +0.0% | +0.0% |

