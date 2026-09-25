# 149. A deployed node's memory budget was every shard's, so a node could hold its core count times it

## Symptom

During the O61 experiment on the lab, hyperion was killed by the kernel's OOM killer:
`anon-rss:14003400kB` on a 14 GB host, with the inventory's `memory: 8Gi`. Its unit's memory peak
had grown run by run, 5.5, 9.9, 10.5 and 13.3 GB, and nothing was ever evicted. Writes to its
groups failed for twenty seconds while it restarted, and no acknowledged write was lost.

## Cause

`resources.memory` in `shoal.yml` is a **shard's** budget. Each shard keeps its own
`memory_usage` counter and evicts once that counter passes `resources.memory`
(`shard.rs`, the check at the bottom of the shard loop). The configuration page says so, if
briefly: "the shard-wide budget that drives eviction".

A deployment's inventory names a **node's** memory (`Resources::memory`, "the memory limit for
every node"), and the renderer wrote it unchanged as `resources.memory`. With six cores the
lab's 8 GiB nodes had six 8 GiB budgets: 48 GiB of table data before any shard evicted, on hosts
with 14 GB. The whole dataset fitted, so nothing was ever evicted, and a node's resident memory
grew with every row it loaded until the kernel ended it.

## Evidence

**Found on the lab.** hyperion's kernel log at 16:40:35:

```text
oom-kill:…,task_memcg=/system.slice/shoal-tmdb.service,task=tmdb-dataset-no,pid=361868,uid=101
Out of memory: Killed process 361868 (tmdb-dataset-no) total-vm:24044628kB, anon-rss:14003400kB
```

Its journal has no eviction pass at all over the half hour before it. The rendered `shoal.yml`
on every node says `memory: 8Gi` under `resources: {cores: 6}`.

**Tested** by `a_nodes_memory_is_shared_among_its_shards` (`shoal-core`) and
`a_node_file_names_its_memory_as_the_nodes_budget` (`shoalctl`), written with the fix. The defect
was established from the lab and the source, not by a test run against the unfixed tree: the
unfixed renderer has no `node_memory` to assert on.

## The fix

- **`resources.node_memory`**, optional, is the most the node's shards hold together. A shard's
  budget is the smaller of `memory` and `node_memory / shards`, computed once when the shard is
  built (`Resources::shard_budget`), where the number of shards the node actually runs is known.
  `memory` keeps its meaning, so every existing configuration, the benchmark's `shoal.yml`
  included, evicts exactly as before.
- **The renderer writes the inventory's memory as `node_memory`** as well as `memory`. So a
  deployed node of six shards gives each a sixth of its memory, and `memory` bounds a shard only
  on a node of one.

## Alternatives rejected

- **Divide in the renderer.** A node whose inventory leaves `cores` unset runs a shard on every
  core the control plane leaves, which the renderer cannot know. The server knows at start.
- **Make `resources.memory` a node's budget.** It would change what every existing
  configuration means, including the benchmark's `shoal.yml`, whose captures are compared across
  commits. A new key keeps the old meaning where it is written and gives deployments the one they
  meant.
- **One node-wide counter shared by every shard.** Thread per core means nothing a shard does
  on its hot path is shared. A shard's share of the node is the same bound without the sharing.

## Invariants to uphold

- **A deployed node's table data is bounded by the node's memory**: the renderer writes
  `node_memory`, and `shard_budget` divides it by the shards the node runs.
- **`node_memory` never raises a shard above `memory`.**
- **The budget still counts table data alone.** The archive maps, the WAL's index and caches,
  and every buffer sit outside it, so a node's resident memory is its budget plus those. The lab's
  inventory leaves the headroom: 8 GiB of budget on 14 GB hosts.

## Still open

- **What sits outside the budget is not measured.** The archive map in particular holds an entry
  per partition, several million on a lab node. Filed in [todos](../todos.md): a node's memory
  figures on `Stats`, so the headroom can be read, not guessed.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_nodes_memory_is_shared_among_its_shards` (`shoal-core`, `conf.rs`) | A node budget is not divided among shards, or lifts a shard past `memory`, or does not parse |
| `a_node_file_names_its_memory_as_the_nodes_budget` (`shoalctl`, `render.rs`) | A deployed node's memory is written as every shard's budget again |
| The O61 experiment on the lab ([findings](../../cluster-testing/findings.md)) | A lab node grows past its host's memory under a long insert load and is killed |

## Related

- [Memory and eviction](../../tables/memory-and-eviction.md), the per-shard counter.
- [Known issue 59](../known-issues.md#59-a-shard-that-cannot-free-anything-keeps-trying-on-every-message-in-silence),
  why a shard over its budget is silent.
- [F51](../../features/cluster-deployment.md), the inventory and its renderer.
