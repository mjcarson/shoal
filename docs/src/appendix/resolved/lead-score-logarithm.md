# 212. A lead's rendezvous score took libm's logarithm

[F58](../../features/weighted-leadership.md) gives a group's lead to the voter with the highest
weighted rendezvous score. Its docstring promised that every node and every client computes the
same score whatever build it runs, and the logarithm under the score was libm's, which promises no
such thing. [X2](../../object-storage/placement-simulation.md#the-logarithm) found the gap while
choosing placement's logarithm, and [F85](../../features/devices-and-pools.md), which built that
logarithm, moved the lead score onto it.

## Symptom

None was seen. On the lab every node runs one binary against one glibc, so every node computed the
same score. Two nodes on different libms, or a client on another host since
[F74](../../features/client-routing.md) routes writes by the same score, could prefer different
voters for a group whose two best scores were within a rounding of each other:
- a new leader that computed another answer would hand the lead back, at most one group a shard
  every five seconds;
- a client that computed another answer would send that group's writes to a node that forwards them.

Neither loses data. Both are a group's lead or route moving back and forth for no reason.

## Cause

`rendezvous_score` (`shoal-proto/src/shared/placement.rs`) drew `u` in (0, 1) from SplitMix64 over
the group's and the node's identities, and returned `-weight / u.ln()`. `f64::ln` is libm's, and
IEEE 754 requires correct rounding of `+ - * / sqrt`, not of `ln`. Two libms, two builds of one, or
one on two cpus may return answers a unit in the last place apart, and at a near tie that unit
decides.

## Evidence

**Established by reading the source**, then **reproduced as a test written before the fix**.
`the_lead_score_takes_no_logarithm_from_libm` computes each of 192 scores (64 groups and nodes, at
weights 1, 2 and 7) as the voter's weight over the table's `neglog2` of the same draw, and holds
`rendezvous_score` to it bit for bit. Against the unfixed tree it failed at its first case:

```text
---- shared::placement::tests::the_lead_score_takes_no_logarithm_from_libm stdout ----
assertion `left == right` failed: group GroupId(0) node NodeId(00000000-0000-0000-0000-000000000000)
weight 1: 0.0267165748312771 against the table's 0.00000000000000005551115123125783
  left: 4583357749821528289
 right: 4363988038922010624
```

The failure shows the score was not the table's. It does not show two libms disagreeing, which
needs two hosts. X2's measurement of the same comparison stands for that: a fixed-point logarithm
and `f64::ln` chose 4 of 39 million chunks differently, all near ties.

## The fix

The score is now `weight / neglog2(draw) as f64`:
- the draw is `rendezvous::mix64`, SplitMix64's output function, over the same fold of the two
  identities as before;
- `neglog2` is placement's logarithm, read from the frozen `LOG2_TABLE` of 4,097 integers with 48
  fraction bits and interpolated in integers;
- the one floating point operation left is a divide, which IEEE 754 rounds correctly everywhere.

So every node, client, build and cpu computes the same bits. `-ln(u)` and `-log2(u)` differ by the
constant `ln 2`, so the order of scores is the same as before up to rounding. An upgrade moves the
lead of a group only where its two best scores were within the old rounding of each other.
`the_rendezvous_score_is_frozen` was rewritten to the new bits, `0x3ce8_5568_bc80_99de`.

## Alternatives rejected

| Alternative | Why not |
| --- | --- |
| Compare draws without a logarithm (`u1^(1/w1)` against `u2^(1/w2)`) | A power is libm's too. Comparing exactly in integers needs big integers or a logarithm anyway |
| A logarithm of our own computed at run time in `f64` | Its operations would be specified, but every step would have to be audited for a fused multiply-add the compiler is allowed to contract on one target and not another |
| Leave the lead score on libm and give placement the table | Two rendezvous in one crate with two promises, and the one a client routes by would be the weaker. The table was free to share |
| A crate's logarithm (`libm` the crate) | Correctly rounded on some functions and not promised on all, and its output could move with a version: X5's rule is that anything compared across builds is written out here |

## Invariants to uphold

- **No score that more than one process compares takes a logarithm, power or exponential from
  libm.** The lead score and placement both read `neglog2`. A new weighted choice that must agree
  across nodes reads it too.
- **`LOG2_TABLE` is frozen.** `the_log2_table_is_frozen` holds a CRC of it, so changing an entry
  moves every lead and every placement group, and is a migration, not a fix.
- **The only floating point after the table is one IEEE operation**, a divide here and a multiply in
  placement. Adding a second invites a contraction that differs between targets.

## Still open

Nothing of this item. Changing the score moved some groups' leads once, on the upgrade that carries
it, and every client of the old build routes those groups' writes through a forward until it is
upgraded too.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-proto` `placement::tests::the_lead_score_takes_no_logarithm_from_libm` | The score is not the table's: it fails at its first case, as above |
| `shoal-proto` `placement::tests::the_rendezvous_score_is_frozen` | The frozen bits are the table's, so libm's score fails it |
| `shoal-proto` `rendezvous::tests::the_log2_table_is_frozen`, `every_log2_entry_is_within_one_of_libms` | The table the score now depends on changes, or was wrong to start with |

## Related

- [F58](../../features/weighted-leadership.md): the lead weights the score draws by.
- [F74](../../features/client-routing.md): a client computes the same score to route a write.
- [F85](../../features/devices-and-pools.md): placement's table, which this shares.
- [X2](../../object-storage/placement-simulation.md#the-logarithm): the measurement that found it.
- [S5](../../object-storage/placement.md#the-score): the score placement is held to.
