# 18, 50. Core exclusion was silently ignored, and shard placement was random

Filed as a spelling mistake in a config file. Fixing it meant checking that the corrected key
actually did anything, and that check turned up a much larger defect underneath it: which cpu a
shard runs on was decided by a hash seed and changed on every start.

## Symptom

The shipped `shoal.yml` set `exluded_cores: [28, 29, 30, 31]`. Nothing was excluded. The
benchmark that config existed for believed it had isolated cores for the client and had not,
and no message anywhere said so.

Underneath that, and worse: with `cores: 16` on a 16 core machine, three consecutive runs of
the same binary placed shards like this.

```
run 1  cpus 2,4,5,8,10,11,12,13,15,17,18,19,21,24,25,30  -> cores 2,4,5,8,10,11,12,13,15,1,2,3,5,8,9,14
run 2  cpus 3,5,7,10,11,13,17,18,19,22,25,26,27,28,29,31 -> cores 3,5,7,10,11,13,1,2,3,6,9,10,11,12,13,15
run 3  cpus 2,4,7,11,13,14,16,18,20,23,24,26,27,28,29,30 -> cores 2,4,7,11,13,14,0,2,4,7,8,10,11,12,13,14
```

Run 1 puts two shards on each of cores 2, 5, 8, 11, 12, 13 and none at all on cores 0, 6, 7,
14. Run 3 doubles up cores 2, 4, 7, 11, 13, 14. The set is different every time, and the
docs' claim that `cores: 16` "puts one shard thread on every physical core"
([Benchmarking](../../operations/benchmarking.md)) was never true.

## Cause

Two separate causes that happened to sit one line apart.

The typo is the simple one. The field is `exclude_cores`
(`shoal-core/src/server/conf.rs:21`) and the config said `exluded_cores`. The `config` crate
ignores unknown keys, so the value was parsed, discarded, and never mentioned. `CLAUDE.md`
reproduced the same typo, which is how it survived.

The placement defect is in `Resources::cpus`:

```rust
let online = CpuSet::online()?
    .filter(|location| location.cpu != 0)
    .filter(|location| !self.exclude_cores.contains(&location.core));
if let Some(cores) = self.cores {
    let cpus = online.into_iter().take(cores).collect::<CpuSet>();
```

`CpuSet` is `CpuSet(HashSet<CpuLocation>)` (glommio `executor/placement/mod.rs:295`). Rust's
default hasher is seeded randomly per process, so `into_iter` yields a different order on every
run and `.take(cores)` takes an arbitrary subset of the eligible cpus.

Both halves of that hurt, and they are worth separating:

- **The order is unstable**, so two runs of one binary are not comparable. This is a variance
  source that is invisible, unbounded, and sits underneath every measurement.
- **Nothing knows about SMT.** Even sorted, cpu `n` and cpu `n + 16` are two threads of one
  physical core on this machine, so any selection that does not treat cores as the unit will
  pair shards onto one core while leaving another idle.

The second is not a consequence of the first. Sorting alone would have fixed the churn and
kept the pairing.

## Evidence

**Established by reproducing both.** The typo first — a config carrying it was loaded and the
result printed, before any fix:

```
thread 'server::conf::tests::misspelled_resource_key_is_rejected' panicked at
shoal-core/src/server/conf.rs:336:26:
a misspelled resource key was accepted: Conf { resources: Resources { cores: None,
exclude_cores: [], memory: 4294967296 }, ... }
```

`exclude_cores: []`, from a file that asked for four cores to be excluded.

The placement defect was found while checking that the corrected key did anything. The three
runs quoted under **Symptom** are the actual output of a probe that printed
`Resources::cpus()` for a fixed config three times in a row. Nothing about the config changed
between them.

## The fix

The typo is corrected in `shoal.yml` and `CLAUDE.md`, and `shoal.yml` is now **committed** — it
was untracked while four pages described it as "the checked-in `shoal.yml`".

Correcting the spelling is not the fix, though. A config key that is silently dropped is a
class of defect, not one instance, so `Resources` is now `#[serde(deny_unknown_fields)]`. A
misspelled resource setting fails the load and names the key it could not place.

`Resources::cpus` selects in two passes over a deterministically sorted candidate list:

```rust
let mut candidates = online.into_iter().collect::<Vec<_>>();
candidates.sort_unstable_by_key(|location| {
    (location.numa_node, location.package, location.core, location.cpu)
});
// one cpu per physical core first ...
// ... and only then the sibling threads of cores already taken
```

On the development machine, `cores: 12` with `exclude_cores: [12, 13, 14, 15]` now yields cpus
1–11 plus cpu 16, which is twelve distinct physical cores with cores 12–15 left entirely free
for the client. It yields exactly that on every run.

## Alternatives rejected

**Correcting the spelling and stopping there.** It would have closed item 18 and left the
defect that made item 18 matter.

**`deny_unknown_fields` on `Conf` itself.** `Conf::from_file` overlays
`config::Environment::with_prefix("shoal")` on top of the file, so an unrelated `SHOAL_*`
variable in the environment would fail the load. The nested structs are where config keys are
actually written.

**Sorting the candidates and taking the first `n`.** Fixes the run-to-run churn, which is the
visible symptom, and leaves shards pairing onto SMT siblings while whole cores idle. The
visible half was not the expensive half.

**Selecting by core and ignoring `numa_node` and `package`.** Correct on this single socket
machine and wrong on the first multi socket one. Sorting by the full topology tuple costs
nothing here and does not have to be revisited there.

**Renaming the field to `exluded_cores` to match the config.** Not seriously, but it is worth
recording that it was possible: the typo had been in the shipped config long enough that it
was the more widely deployed spelling.

## Invariants to uphold

- **`Resources::cpus` must stay deterministic.** The sort is what makes two runs of one build
  comparable. Nothing may take cpus off a `CpuSet` — or any other `HashSet` — in iteration
  order.
- **Physical cores are filled before SMT siblings.** Reverting the second pass reintroduces
  idle cores under load without changing any output.
- **`exclude_cores` filters on `location.core`, not `location.cpu`.** Excluding a core is meant
  to remove both of its threads; filtering on the cpu id would remove one and leave the other
  schedulable, which is worse than not excluding it at all.
- **New config structs get `deny_unknown_fields`.** The point is the class, not the key.
- **`shoal.yml` stays committed**, and changing it invalidates the recorded baseline
  ([Performance Baseline](../../operations/performance-baseline.md)).

## Still open

`Conf::from_file` marks the config file `required(false)`, so a typo in the *path* still
silently runs on defaults. `deny_unknown_fields` catches a bad key inside a file that was
found; it cannot catch a file that was never read. Confirm the config printed at startup is
the one you meant.

`Resources::cpus` also still silently hands back fewer cpus than `cores` asked for when
exclusions leave too few — `cores: 16` with twelve cores excluded starts four shards and says
nothing.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `conf::tests::misspelled_resource_key_is_rejected` | A misspelled resource key is silently dropped again |
| `conf::tests::exclude_cores_is_honored` | The correctly spelled key stops being parsed |
| `conf::tests::excluded_cores_leave_the_cpuset` | An excluded core can be scheduled on |
| `conf::tests::cpu_selection_is_deterministic` | Placement depends on the hash seed again |
| `conf::tests::cpu_selection_fills_physical_cores_first` | Shards pair onto one core while another idles |

## Related

- [F3. A three layer performance harness](../../features/performance-harness.md) — this was found while building it
- [Performance Baseline](../../operations/performance-baseline.md) — the numbers this defect would have invalidated
- [Benchmarking](../../operations/benchmarking.md) — the core layout section this corrects
- [Configuration](../../getting-started/configuration.md) — where the typo was documented as a hazard
