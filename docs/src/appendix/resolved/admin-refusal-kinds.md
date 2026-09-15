# 98. An admin refusal's error code was derived from its reason text

## Symptom

A client that sent an administrative mutation and was refused got an `AdminError` whose code
was `Internal` for every refusal but one: a node that is not a member, a node that is not up,
a duplicate in the placement, a second initialization, a voter count outside {1, 3, 5}, a
malformed request - all `Internal`, with the sentence the state machine wrote as the message.
Only a reason whose text happened to contain `stale version` became `StaleVersion`. A client
that wanted to act on *why* it was refused had a string to parse, and a reason whose wording
changed changed a code; [F39](../../features/membership.md)'s admin test asserted on the
message for the already-initialized case because there was no code to assert on.

## Cause

`ControlResponse::Refused { reason }` carried a sentence and nothing else, and the two places
that turned it into a code - `handle_admin` in `shoal-core/src/server/control/plane.rs` and
the plan-move retry loop in the same file - read the sentence:

```rust
if reason.contains("stale version") { ErrorCode::StaleVersion } else { ErrorCode::Internal }
```

The state machine knew why it refused at every one of its hundred and thirty-seven refusal
sites; the knowledge was thrown away at the boundary where it became a string.

## Evidence

**Reproduced.** The assertion added to `admin_mutations_require_principal_and_operation_identity`
in `shoal/tests/cluster_fixture.rs`, run against the tree at `af249c6` with the code spelled as
its number so it compiled there:

```text
thread 'admin_mutations_require_principal_and_operation_identity' panicked at shoal/tests/cluster_fixture.rs:3589:5:
assertion `left == right` failed: a second initialization was refused with the wrong code: AdminResponse { node: NodeId(d2b1bfa6-…), topology_version: 11, outcome: Err(AdminError { code: 1, msg: "the placement is already initialized; a replica set moves between nodes by a Move operation, not a second initialization" }) }
  left: Internal
```

`code: 1` is `Internal`. With the fix the same refusal is `AlreadyInitialized`.

## The fix

`ControlResponse::Refused` carries a `kind: RefusalKind` beside the sentence - `NotMember`,
`NotUp`, `WrongPhase`, `Duplicate`, `AlreadyInitialized`, `NotInitialized`, `StaleVersion`,
`BadVoterCount`, `UnknownOperation`, `Queued`, `WireVersion`, `Invalid` and `Other` - set at
every refusal site through `ControlResponse::refused(kind, reason)`. `handle_admin` maps the
kind to a code through one function, `refusal_code`, and the plan-move retry loop matches on
`kind == StaleVersion`. The sentence stays for the log and for the message.

Nine codes were added to the 60-band of `ErrorCode`, appended after `Quarantined = 64` and
pinned by the round-trip test: `NotMember = 65`, `NotUp = 66`, `Duplicate = 67`,
`AlreadyInitialized = 68`, `BadVoterCount = 69`, `InvalidRequest = 70`, `WrongPhase = 71`,
`UnknownOperation = 72`, `WireVersion = 73`. A `Queued` refusal is answered `Unavailable`,
because the same request succeeds once the transition ahead of it is done; `Other` alone
reaches `Internal`.

The field is `#[serde(default)]`. A `Refused` crosses nodes inside `ProposeResponse::Applied`
when a follower forwards a proposal, so a leader on this build answering a follower on the
previous one - and the other way round - decodes to `Other`, which is the code it got before.
An `Operation` outcome is persisted in the control state, but only `Applied` outcomes are
remembered, so no refusal is read back from disk.

## Alternatives rejected

**Keep the sentence and parse it better.** The sentences are written for a person reading a
log, and a match on their wording is a second copy of the state machine's decision that drifts
the first time one of them is reworded.

**One code, `Refused`, with the kind in the message.** The client would still parse a string,
and the codes that already exist - `StaleVersion`, `NotInitialized`, `Unauthorized` - are the
precedent for a code per kind.

**A code per refusal site.** A hundred and thirty-seven codes would say nothing a client can act
on that the thirteen kinds do not; the kinds are the decisions, and the sentence says which
site made it.

## Invariants to uphold

- **Every refusal site names a kind.** `ControlResponse::refused` is the only constructor
  worth using; a `Refused { reason, kind }` written by hand with `Other` is the old behaviour
  wearing the new field.
- **`refusal_code` is total and the wording of no reason decides a code.** A `contains` on a
  reason anywhere in `plane.rs` is a regression of this item.
- **The 60-band numbers are pinned.** `every_error_code_round_trips_through_its_discriminant`
  fails if one moves; a new admin code is appended at 74, never inserted.
- **`kind` stays `#[serde(default)]`** until every build that lacks it is off the floor.

## Still open

- `Fenced` and `Removed` responses are still answered `Internal` to an admin client, with a
  message. They are not refusals the client caused and no code names them; a client that meets
  one is a client running on a fenced node, which is about to stop.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_refusal_names_its_kind` | `shoal-core/src/server/control/types.rs` | Ten refusals of nine kinds driven through the state machine and read by kind alone, and a refusal without a kind decoding to `Other` |
| `admin_mutations_require_principal_and_operation_identity` | `shoal/tests/cluster_fixture.rs` | A second initialization is refused `Internal` rather than `AlreadyInitialized` |
| `every_error_code_round_trips_through_its_discriminant` | `shoal-proto/src/shared/protocol/tests.rs` | One of the nine new codes moves off its number, or the list falls behind the enum |

## Related

[F39. Membership](../../features/membership.md), which filed this while writing the admin
test; [C9. Operations](../../distributed/operations.md), the admin path;
[F11. The error channel](../../features/error-channel.md), which the codes ride.
