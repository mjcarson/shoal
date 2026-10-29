# What's left to do

**The order of the work, drawn.** The pages before this one say what has to be done and why; this one
says in what order, on two diagrams: the work up to the first gate, and the gates after it. As of
2026-10-26, when this plan was drawn, nothing on either diagram is done.

**How to read them.**

- An arrow points from a piece of work to what requires it. Things on one level, with no arrow between
  them, can be done in parallel.
- A green box with a ✅ is done.
- A box titled *(optional)*, with a dashed border, can be left out without anything built having to
  change; a dashed arrow means *helps*, not *requires*. Each one's reason is on the page that lists it.
- A box beside a gate lands no later than the start of that gate, and nothing stops it landing sooner
  ([W14](prerequisites.md#the-order)).

The full list of what each gate requires is on [the milestones page](milestones.md#the-gates-at-a-glance);
the diagrams draw an arrow only where no longer path already implies it.

## Before the first gate

Everything in the first box can start now. Three spikes wait on the spike build X24 makes, a holder
and a writer of the lane's shape, which they reuse.

```mermaid
flowchart LR
    classDef done fill:#2e7d32,stroke:#1b5e20,color:#ffffff
    classDef optional stroke-dasharray: 5 5
    subgraph now["Can start now, in parallel"]
        Contract["Agree the contract,<br>P27 to P35"]
        X23["X23 The lane's model"]
        X24["X24 One 4 KiB write,<br>three ways"]
        X25["X25 A volume pool's<br>geometry"]
        X29["X29 A snapshot's<br>copy-on-write"]
        Bench["The bench's block kinds<br>and volume events"]
        X30["X30 RBD, DRBD and Mayastor<br>at the source (optional)"]:::optional
    end
    X26["X26 Working sets and<br>takeover time"]
    X27["X27 ublk beside NBD"]
    X28["X28 The lane beside<br>the reference cell"]
    M29["M29 The harness"]
    X24 --> X26
    X24 --> X27
    X24 --> X28
    Contract --> M29
    X23 --> M29
    Bench --> M29
```

X30 feeds no gate. It reads RBD's, DRBD's and Mayastor's write and fencing paths at pinned releases
and, if run, measures RBD and DRBD on the lab beside the lane; [W15](prior-art.md) marks what it would
confirm.

## The gates

```mermaid
flowchart TB
    classDef done fill:#2e7d32,stroke:#1b5e20,color:#ffffff
    classDef optional stroke-dasharray: 5 5
    M29["M29 The harness"]
    Rows30["W14's rows: bindings and the<br>write policy, the client lane,<br>the lane queue, grants"]
    Q46["Q46 decided<br>with the user"]
    X25["X25 Geometry"]
    X28["X28 The lane beside<br>the reference cell"]
    Ceiling["A node ceiling on<br>client-lane bytes"]
    X26["X26 Working sets and<br>takeover time"]
    X27["X27 ublk beside NBD"]
    X29["X29 Copy-on-write"]
    M30["M30 The lane<br>on one node"]
    M31["M31 Across nodes"]
    M32["M32 Placement<br>under a writer"]
    M33["M33 The host<br>block device"]
    M34["M34 Snapshots<br>and clones"]
    FS["The filesystem's M38:<br>file data on the lane"]
    M29 --> M30 --> M31 --> M32 --> M33 --> M34
    Rows30 --> M30
    Q46 --> M30
    X25 --> M30
    X28 --> M30
    Ceiling --> M31
    X26 --> M31
    X27 --> M33
    X29 --> M34
    M32 --> FS
    subgraph anytime["Optional, any time"]
        direction TB
        Fwd["Forwarding large<br>writes (optional)"]:::optional
        Nbd["NBD and vhost-user-blk<br>(optional)"]:::optional
        Uring["An io_uring client<br>(optional)"]:::optional
        Reflink["XFS reflink<br>(optional)"]:::optional
        Push["A push to a barred<br>writer (optional)"]:::optional
        Gate["An NVMe-oF/TCP gateway,<br>a gate of its own (optional)"]:::optional
    end
```

The chain is the claim [the milestones page](milestones.md#the-order-is-a-claim) argues for: the model
before any code, one node before a cluster, placement under a writer before a host can attach, the
lane before the device, snapshots last. One of its places is convenience: M33 needs M31 and could
start beside M32. The filesystem's M38 needs M32 and nothing after it. The rest of the optional work,
flattening a clone and write-back as a deployment's default for devices with power-loss protection, is
on [the milestones page](milestones.md#optional-any-time); it changes no box here.

## Keeping it current

**Whoever finishes a piece of work turns its box green in the same change**, with a ✅ at the start of
its label and `:::done` after it, as the object storage part's boxes were turned
([its page](../../object-storage/whats-left-todo.md#keeping-it-current)). A piece of work that is added,
split or dropped is added, split or dropped here too, and an optional one says *(optional)* in its
title. The conventions are `CLAUDE.md`'s, under *Planning Docs*.

## Related

[W14](prerequisites.md) for the rows the gates begin with; [Spikes](spikes.md) for what has to report
first; [Milestones](milestones.md) for what each gate delivers; [W27](contract.md) for the questions and
the record; [the filesystem's What's left to do](../filesystem/whats-left-todo.md) for the plan that
follows.
