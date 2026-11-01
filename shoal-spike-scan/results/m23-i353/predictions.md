# Item 353's run: predictions, written before it (2026-11-01)

Item 353 (`docs/src/appendix/known-issues.md`): light scan steps, 1-row encode steps of `Big` and
captures of about ten keys, pass 100 µs on their own cpu, 0 to 14 an arm on every member, in item
350's two runs. The lab's kernels charge a cpu's interrupts to the thread they interrupt
(`CONFIG_IRQ_TIME_ACCOUNTING` unset, tick-based accounting at HZ=1000). This run counts each long
step's page faults and switches (`getrusage(RUSAGE_THREAD)` either side of it), keeps the scan steps
by shard with each shard's cpu, samples every cpu's interrupts and softirqs each second
(`m23-cpu-sample.sh`), and in rounds 3 and 6 moves titan's and hyperion's NIC interrupt from cpu 5,
one of their five shards' cpus, to cpu 7, which no shard runs on.

## What is known before the run

- titan and hyperion: one NIC queue, its interrupt on cpu 5 (`effective_affinity_list`), irqbalance
  off. Shards on cpus 2 to 6, the control thread on 0. Every shard's cpu has an NVMe queue of its own.
  Over the hosts' uptime cpu 5 took about 10 to 20 times the NET_RX softirqs of any other cpu.
- europa: one NIC queue on cpu 23, which no shard of the side node runs on (shards on 1 to 6).
- Item 350's runs, four rounds each, `Big`'s arms: on the Zen1 hosts 28 to 40 captures and 12 to 19
  encode steps past 100 µs on their own cpu, from about 3,000 captures and 20,000 encode steps an
  arm: a capture passes about twenty times as often per step, while its mean step is about twice as
  long. Europa: 2 captures and 11 encode steps.

## The outcomes, and what each would show

1. **The NIC's interrupts.** On titan and hyperion the shard on cpu 5 holds most of the unfaulted
   light steps past 100 µs on their own cpu, at several times the rest's rate a second of step cpu.
   With the NIC moved to cpu 7 its rate falls to the rest's. A shard's rate climbs with its cpu's
   softirq share window by window. Europa's are few and spread.
2. **Page faults.** Most light steps past 100 µs on their own cpu took a fault, spread over a host's
   shards by their step cpu, with no change when the NIC moves and no climb with softirq share.
3. **Neither.** Unfaulted and spread evenly over the shards, with no change when the NIC moves: the
   steps' own work, or interrupts every shard's cpu takes alike (the NVMe queues', the local timer's).

## Predicted

- **The Zen1 hosts' light steps: outcome 1, in the main.** The shard on cpu 5 holds at least half of
  its host's unfaulted light steps past 100 µs on their own cpu, against about a fifth of the step
  cpu, and with the NIC moved it holds no more than its share. Its rate climbs with its softirq share.
  A NET_RX softirq that drains a burst of a 1 GbE link's segments can run for hundreds of microseconds,
  and cpu 5 takes tens of thousands a second under the cell.
- **The captures' excess over encode steps is not explained by exposure.** I expect it to sit on cpu 5
  with the rest, a capture running from the loop's second inbox when it has just gone idle, and so
  near the receive softirqs of the messages that woke it. This is the least confident of these.
- **Faults: a minority of the light steps**, under a quarter on every host.
- **Europa's `Keyword` residual** (steps of 70 to 90 rows at ten times the cpu a row): **faulted**, the
  copied rows' allocations taking memory the allocator has given back. Europa has no NIC on a shard's
  cpu, so if these steps are not faulted, outcome 3 is left for them.
- **Steps past 100 µs by the wall alone are switched**, every one of them: preempted (O147's io_uring
  workers), never blocked.

Any of these the run contradicts is said so on item 353's record, beside what it showed.
