# What's left to do

**The order of the work, drawn.** The pages before this one say what has to be done and why;
this one says in what order, on two diagrams: the work up to the first gate, and the gates
after it. As of 2026-10-05, when X11 reported and F73 delivered the frames it set, X13 reported
what F69 had left of the benchmark's shape, and X14 read Ceph and S3 at the source; X6 and X10 had
reported the day before, and X2, X4 and X5 the day before that.

**How to read them.**

- An arrow points from a piece of work to what requires it. Things on one level, with no arrow
  between them, can be done in parallel.
- A green box with a ✅ is done.
- A box titled *(optional)*, with a dashed border, can be left out without anything built
  having to change; a dashed arrow means *helps*, not *requires*. Each one's reason is on the
  page that lists it.
- A box beside a gate lands no later than the start of that gate, and nothing stops it landing
  sooner ([S1](prerequisites.md#the-order)).

## Before the first gate

Everything here can be worked on now except what an arrow ~~points into~~ from an unfinished
box points into: ~~X9's arrows now come only from green ones, so it can start~~ X8's and X9's
arrows now come only from green ones, so both can start. The spikes are on
[their page](spikes.md), and what each needed first on
[What a spike needs first](spikes.md#what-a-spike-needs-first).

```mermaid
flowchart LR
    classDef done fill:#2e7d32,stroke:#1b5e20,color:#ffffff
    classDef optional stroke-dasharray: 5 5
    subgraph now["Can start now, in parallel"]
        R1["✅ X1's model held<br>to S7's schedules"]:::done
        R2["✅ Item 210: the bench's<br>preload fits a frame"]:::done
        XFS["✅ Fit an XFS filesystem"]:::done
        Disks["Fit rotational disks"]
        X2["✅ X2 Placement simulation"]:::done
        X4["✅ X4 Erasure coding crates"]:::done
        X5["✅ X5 Checksums"]:::done
        X6["✅ X6 The device store on SSD"]:::done
        X10["✅ X10 What a stripe row costs"]:::done
        X11["✅ X11 Streamed bodies"]:::done
        X13["✅ X13 The benchmark's shape,<br>what F69 left"]:::done
        X14["✅ X14 Ceph and S3 at the source"]:::done
        Counters["✅ F71 Device counters and node memory<br>in a bench capture (optional)"]:::done
        Neighbour["✅ F72 A paced neighbour stream<br>in the bench (optional)"]:::done
    end
    X1["X1 The stripe protocol as a model"]
    X3["X3 Bytes through the tablet groups"]
    X6x["✅ X6, its XFS leg"]:::done
    X7["X7 The device store on HDD"]
    X8["X8 One small write, three ways"]
    X9["X9 Table latency beside object work"]
    X12["X12 Recovery and scrub rates"]
    X12r["X12, its rotational half<br>(only M19 waits on it)"]
    Gate["Before M11: the contract agreed,<br>every decision on S18's record"]
    R1 --> X1
    R2 --> X3
    X6 --> X6x
    XFS --> X6x
    X6 --> X7
    Disks --> X7
    X6 --> X8
    X4 --> X9
    X5 --> X9
    X4 --> X12
    X6 --> X12
    X7 --> X12r
    X12 --> X12r
    Counters -.-> X3
    Neighbour -.-> X3
    X1 --> Gate
    X3 --> Gate
    X6x --> Gate
    X8 --> Gate
    X9 --> Gate
    X12 --> Gate
    X2 ---> Gate
    X10 ---> Gate
    X11 ---> Gate
    X13 ---> Gate
    X14 ---> Gate
```

~~An optional box, "a kind a schema supplies, for X10's cold commit", stood beside X10.~~ It is
gone: X10 drove its commits with a driver of its own, which aimed each one at a group's leader
and timed a read before it, and no operation kind the bench drives could have done that
([X10](stripe-row-costs.md#the-harness)). Buckets still supply their kinds at M12.

The failure domain and free bytes for every root waited on Q19, which said what placement reads.
X2 decided that part of it: a device's id and its member's host, and each device's size,
placement weight and free bytes ([Q19, in part](contract.md#q19-in-part-placement-2026-10-03)).
Both can start now, though they land no later than M14.

The gate waits on every spike because the [milestones](milestones.md) stop being provisional
only once every decision is on the record
([spikes](spikes.md#what-has-to-be-on-the-record-before-the-milestones-are-real)), and a spike
that needs nothing is run before the plan is drawn. Each question also has a gate of its own,
the last moment it can be answered; those are on the
[milestones page's table](milestones.md#the-gates-at-a-glance). The rotational half of X12 and
X7 are the exception: if the disks come late, only M19 waits for them.

## The gates

```mermaid
flowchart TB
    classDef done fill:#2e7d32,stroke:#1b5e20,color:#ffffff
    classDef optional stroke-dasharray: 5 5
    Gate["Before M11: the contract agreed,<br>every decision on S18's record"]
    F69["✅ F69 Operation kinds and<br>byte counters in the driver"]:::done
    F70["✅ F70 Storage faults<br>in the fixture"]:::done
    F68["✅ F68 Conditional writes"]:::done
    I198["✅ Items 92 and 198:<br>composite partition keys"]:::done
    I202["✅ Item 202: a byte bound<br>on an append batch"]:::done
    I46["✅ Item 46: an unmarked<br>directory refused"]:::done
    Frames["✅ F73 More than one<br>frame for one query"]:::done
    Domain["A failure domain on<br>a member"]
    Free["Free bytes for every<br>root"]
    Walk["The walk of one tablet's<br>rows (waits on Q17)"]
    Handoff["Handing a connection to another<br>executor (optional; X11<br>found the hop dear)"]:::optional
    Rack["A failure domain above<br>the host (optional)"]:::optional
    X12r["X12, its rotational half"]
    Q31["Q31 decided, by design"]
    M11["M11 The harness and the facts"]
    M12["M12 Tables: what the<br>metadata needs"]
    M13["M13 The wire and the baseline"]
    M14["M14 Devices and pools<br>on one node"]
    M15["M15 Replicated pools<br>across nodes"]
    M16["M16 Recovery"]
    M17["M17 Scrub and repair"]
    M18["M18 Erasure coding"]
    M19["M19 Rotational devices"]
    M20["M20 Reclamation and<br>device lifecycle"]
    M21["M21 Operations and<br>the real cluster"]
    Gate --> M11 --> M12 --> M13 --> M14 --> M15 --> M16 --> M17
    M17 --> M18 --> M21
    M17 --> M19 --> M21
    M17 --> M20 --> M21
    F69 --> M11
    F70 --> M11
    F68 --> M12
    I198 --> M12
    I202 --> M12
    Frames --> M13
    I46 --> M14
    Domain --> M14
    Free --> M14
    Handoff -.-> M14
    Domain -.-> Rack
    Walk --> M16
    X12r --> M19
    Q31 --> M21
    subgraph anytime["Optional, any time"]
        direction TB
        D7["D7 client routing<br>by topology (optional)"]:::optional
        Cancel["Cancel on the<br>client wire (optional)"]:::optional
        Paging["Paging the<br>archive map (optional)"]:::optional
        Authz["Authorization, built once for<br>tables and buckets (optional)"]:::optional
        I93["Item 93: the archived hash<br>of a string key (optional)"]:::optional
    end
```

~~A clone call in the glommio fork (optional, if X6 picks a clone)~~ fed M14 until 2026-10-04,
and is dropped: [X6](device-store-ssd.md#3-a-partial-write) rejected the clone, so nothing waits
on it.

More than one frame for one query waited on Q26 until 2026-10-05, when
[X11](streamed-bodies.md) answered the part it needed: object bytes travel on connections of
their own, in frames of 1 MiB. [F73](../features/bodies-across-frames.md) delivered it the same
day. The box handing a connection to another executor stays optional,
since adding it later changes no format, and X11 is what makes it worth building at M14: a
connection under kTLS can be handed over at no cost, while its bytes hopping between executors
cost 1.3 to 1.7 times the cpu a gibibyte ([X11](streamed-bodies.md#4-a-connection-handed-over-and-bytes-that-hop)).

The chain is the claim [the milestones page](milestones.md#the-order-is-a-claim) argues for:
the wire before the devices, the device store before distribution, replication before erasure
coding, recovery before scrub. Two of its places are convenience and not dependency, and the
diagram draws them as the dependencies they are: M19 needs M14 to M17 and not M18, and M20
waits on nothing after M17 and could move forward if devices fill in testing.

## Keeping it current

**Whoever finishes a piece of work turns its box green in the same change**, with a ✅ at the
start of its label and `:::done` after it, as the ✅ rows on S1 and the struck-through rows on
the milestones page record the same thing. A piece of work that is added, split or dropped is
added, split or dropped here too, and an optional one says *(optional)* in its title. The
conventions are written down in `CLAUDE.md`, under *Planning docs*, for every plan after this
one.

## Related

[S1](prerequisites.md) for the rows the gates begin with; [Spikes](spikes.md) for what has to
report first; [Milestones](milestones.md) for what each gate delivers; [S18](contract.md) for
the questions and the record.
