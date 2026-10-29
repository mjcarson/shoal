# X14 E6: Ceph's CRUSH (crushtool, v20.2.0) on X2's shapes

16384 placement groups a pool; every change is to host zero's first device (osd.0).

| Shape | Pool | Change | Least | Moved | Moved / least | Moved off untouched hosts |
| --- | --- | --- | --- | --- | --- | --- |
| lab-1 | 2+1/host | remove, after out | 0 | 14,153 | 0.86× the out step's least | 8,244 |
| lab-1 | 2+1/host | add | 8,193 | 16,781 | 2.05× | 6,549 |
| lab-1 | 2+1/host | remove | 16,384 | 26,734 | 1.63× | 10,350 |
| lab-1 | 2+1/host | reweight ½ | 0 | 8,448 | 8448 moved, none needed | 4,802 |
| lab-1 | 2+1/host | out | 16,384 | 21,355 | 1.30× | 4,971 |
| lab-1 | 2+1/host | host out | 16,384 | 21,355 | 1.30× | 4,971 |
| lab-2 | 2+1/host | remove, after out | 0 | 9,373 | 1.14× the out step's least | 5,676 |
| lab-2 | 2+1/host | add | 5,414 | 12,246 | 2.26× | 4,764 |
| lab-2 | 2+1/host | remove | 8,225 | 15,652 | 1.90× | 5,608 |
| lab-2 | 2+1/host | reweight ½ | 2,777 | 7,270 | 2.62× | 2,704 |
| lab-2 | 2+1/host | out | 8,225 | 8,427 | 1.02× | 202 |
| lab-2 | 2+1/host | host out | 16,384 | 21,355 | 1.30× | 4,971 |
| lab-2 | 4+2/device | remove, after out | 0 | 30,414 | 1.86× the out step's least | 14,004 |
| lab-2 | 4+2/device | add | 14,064 | 32,597 | 2.32× | 17,915 |
| lab-2 | 4+2/device | remove | 16,384 | 40,326 | 2.46× | 14,493 |
| lab-2 | 4+2/device | reweight ½ | 1 | 19,243 | 19243.00× | 8,693 |
| lab-2 | 4+2/device | out | 16,384 | 25,759 | 1.57× | 7,417 |
| lab-2 | 4+2/device | host out | 32,768 | 43,612 | 1.33× | 10,844 |
| 6x12 | r3/host | remove, after out | 0 | 1,364 | 1.99× the out step's least | 680 |
| 6x12 | r3/host | add | 640 | 1,429 | 2.23× | 702 |
| 6x12 | r3/host | remove | 685 | 1,401 | 2.05× | 242 |
| 6x12 | r3/host | reweight ½ | 337 | 675 | 2.00× | 123 |
| 6x12 | r3/host | out | 685 | 685 | 1.00× | 0 |
| 6x12 | r3/host | host out | 8,080 | 8,080 | 1.00× | 0 |
| 6x12 | 4+2/host | remove, after out | 0 | 4,107 | 2.94× the out step's least | 1,869 |
| 6x12 | 4+2/host | add | 1,197 | 4,128 | 3.45× | 1,965 |
| 6x12 | 4+2/host | remove | 1,396 | 4,229 | 3.03× | 1,869 |
| 6x12 | 4+2/host | reweight ½ | 655 | 2,078 | 3.17× | 943 |
| 6x12 | 4+2/host | out | 1,396 | 1,396 | 1.00× | 0 |
| 6x12 | 4+2/host | host out | 16,384 | 25,867 | 1.58× | 9,483 |
| 6x12 | 8+3/device | remove, after out | 0 | 4,841 | 1.97× the out step's least | 2,438 |
| 6x12 | 8+3/device | add | 2,484 | 5,071 | 2.04× | 2,571 |
| 6x12 | 8+3/device | remove | 2,456 | 5,024 | 2.05× | 499 |
| 6x12 | 8+3/device | reweight ½ | 1,160 | 2,534 | 2.18× | 289 |
| 6x12 | 8+3/device | out | 2,456 | 2,473 | 1.01× | 14 |
| 6x12 | 8+3/device | host out | 29,735 | 29,907 | 1.01× | 172 |
| 50x24 | r3/host | remove, after out | 0 | 85 | 1.55× the out step's least | 54 |
| 50x24 | r3/host | add | 35 | 69 | 1.97× | 36 |
| 50x24 | r3/host | remove | 55 | 85 | 1.55× | 3 |
| 50x24 | r3/host | reweight ½ | 25 | 39 | 1.56× | 1 |
| 50x24 | r3/host | out | 55 | 55 | 1.00× | 0 |
| 50x24 | r3/host | host out | 1,006 | 1,006 | 1.00× | 0 |
| 50x24 | 10+4/host | remove, after out | 0 | 433 | 2.27× the out step's least | 86 |
| 50x24 | 10+4/host | add | 225 | 449 | 2.00× | 214 |
| 50x24 | 10+4/host | remove | 191 | 440 | 2.30× | 86 |
| 50x24 | 10+4/host | reweight ½ | 99 | 231 | 2.33× | 48 |
| 50x24 | 10+4/host | out | 191 | 191 | 1.00× | 0 |
| 50x24 | 10+4/host | host out | 4,585 | 4,703 | 1.03× | 118 |

## Fill and holes, before any change

| Shape | Pool | Fullest device over the mean | Emptiest under it | Holes (CRUSH_ITEM_NONE) |
| --- | --- | --- | --- | --- |
| lab-1 | 2+1/host | +0.0% | +0.0% | 0 |
| lab-2 | 2+1/host | +0.4% | -0.4% | 0 |
| lab-2 | 4+2/device | +0.0% | +0.0% | 0 |
| 6x12 | r3/host | +9.3% | -9.6% | 0 |
| 6x12 | 4+2/host | +6.8% | -5.5% | 0 |
| 6x12 | 8+3/device | +4.1% | -4.3% | 0 |
| 50x24 | r3/host | +46.5% | -41.4% | 0 |
| 50x24 | 10+4/host | +25.6% | -22.0% | 0 |
