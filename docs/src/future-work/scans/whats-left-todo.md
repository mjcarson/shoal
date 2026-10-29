# What's left to do

**The order of the work, drawn.** The pages before this one say what has to be done and why; this one
says in what order, on two diagrams: the work up to the first gate, and the gates after it. ~~As of
2026-10-25, when this plan was drawn, nothing on either diagram is done.~~ As of 2026-10-29 the contract
is agreed ([W13's record](contract.md#before-m22-the-contract-agreed-2026-10-29)), and the bench's scan
kind and event ([F115](../../features/bench-scans.md)), the admission class
([F116](../../features/scan-admission.md)), the scan class ([F117](../../features/scan-class.md)) and the
budget ([F118](../../features/scan-budget.md)) are built; nothing else on either diagram is done.

**How to read them.**

- An arrow points from a piece of work to what requires it. Things on one level, with no arrow
  between them, can be done in parallel.
- A green box with a ✅ is done.
- A box titled *(optional)*, with a dashed border, can be left out without anything built having to
  change; a dashed arrow means *helps*, not *requires*. Each one's reason is on the page that lists
  it.
- A box beside a gate lands no later than the start of that gate, and nothing stops it landing
  sooner ([W1](prerequisites.md#the-order)).

The full list of what each gate requires is on [the milestones page](milestones.md#the-gates-at-a-glance);
the diagrams draw an arrow only where no longer path already implies it.

## Before the first gate

Everything in the first box can start now. The spikes after it wait on X16's baseline, which says
which part of the walk hurts a foreground first.

```mermaid
flowchart LR
    classDef done fill:#2e7d32,stroke:#1b5e20,color:#ffffff
    classDef optional stroke-dasharray: 5 5
    subgraph now["Can start now, in parallel"]
        Contract["✅ Agree the contract,<br>P20 to P26"]:::done
        X16["X16 The walk beside<br>the foreground"]
        Bench["✅ The bench's scan kind<br>and scan event"]:::done
        Class["✅ A scan class on<br>every table shard"]:::done
        Budget["✅ A budget for transient<br>read buffers"]:::done
        Pace["Table reads paced by<br>a device's idle time"]
        Admit["✅ An admission class<br>apart from the drivers'"]:::done
        X20["X20 ScyllaDB's scan path<br>at the source (optional)"]:::optional
    end
    X17["X17 A page rebuilt"]
    X18["X18 Table reads paced<br>by idle time"]
    X19["X19 The edges"]
    M22["M22 The harness<br>and the facts"]
    X16 --> X17
    X16 --> X18
    X17 --> X19
    Contract --> M22
    Bench --> M22
    X16 --> M22
```

X20 feeds no gate. It reads ScyllaDB's scan path at a pinned release and, if run, a ScyllaDB on the
lab beside a foreground; [W3](prior-art.md) marks what it would confirm.

## The gates

```mermaid
flowchart TB
    classDef done fill:#2e7d32,stroke:#1b5e20,color:#ffffff
    classDef optional stroke-dasharray: 5 5
    M22["M22 The harness<br>and the facts"]
    Rows["W1's rows: the scan class,<br>the budget, the pacing,<br>the admission class"]
    X17["X17 A page rebuilt"]
    X18["X18 Table reads paced<br>by idle time"]
    X19["X19 The edges"]
    Q39["Q39 decided<br>with the user"]
    X21["X21 Declared order,<br>priced"]
    X22["X22 The ordered listing,<br>two ways"]
    M23["M23 The walk rebuilt<br>for load"]
    M24["M24 Scans on<br>one node"]
    M25["M25 Scans across<br>a cluster"]
    M26["M26 Listing a<br>bucket's objects"]
    M27["M27 Ordered scans<br>by declared order"]
    M28["M28 The ordered<br>listing"]
    M22 --> M23 --> M24 --> M25
    Rows --> M23
    X17 --> M23
    X18 --> M23
    X19 --> M24
    X19 --> M26
    Q39 --> M25
    M25 --> M26
    M25 --> X21 --> M27
    M26 --> M28
    M27 --> X22 --> M28
    subgraph anytime["Optional, any time"]
        direction TB
        Saved["Saved positions<br>between pages (optional)"]:::optional
        Agg["Aggregates pushed<br>to the shards (optional)"]:::optional
        Coord["A scan coordinated by a node,<br>for thin clients (optional)"]:::optional
        Handoff["A scan connection handed<br>to its slot (optional)"]:::optional
        Whole["Whole partitions<br>in answers (optional)"]:::optional
        Apart["A listing's fields kept<br>apart from inline bytes (optional)"]:::optional
    end
```

The chain is the claim [the milestones page](milestones.md#the-order-is-a-claim) argues for: the engine
before the wire, one node before a cluster, scans before a listing, the listing before ordered scans,
ordered scans before the ordered listing. Two of its places are convenience: M26 needs M25 and nothing
after it, and M27 can start beside M26 once X21 reports, which the diagram draws as the siblings they
are. The rest of the optional work, a stepped-keys helper, scan work on executors of its own, the next
cursor sent ahead of a page's rows and a count of scans a principal, is on
[the milestones page](milestones.md#optional-any-time); it changes no box here.

## Keeping it current

**Whoever finishes a piece of work turns its box green in the same change**, with a ✅ at the start of
its label and `:::done` after it, as the object storage part's boxes were turned
([its page](../../object-storage/whats-left-todo.md#keeping-it-current)). A piece of work that is added,
split or dropped is added, split or dropped here too, and an optional one says *(optional)* in its
title. The conventions are `CLAUDE.md`'s, under *Planning Docs*.

## Related

[W1](prerequisites.md) for the rows the gates begin with; [Spikes](spikes.md) for what has to report
first; [Milestones](milestones.md) for what each gate delivers; [W13](contract.md) for the questions and
the record.
