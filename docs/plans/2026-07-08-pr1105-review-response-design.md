# PR #1105 maintainer-review response — design

**Branch:** `feat/1067/boundary-instrumentation`
**Reviewed commit:** `be1aefd`
**Reviewer:** shane-moore (6 inline comments)
**Issues:** #1067 (this PR), parent #921, taxonomy follow-up #1104 (deferred, untouched)

## Objective

Address the six maintainer review comments on PR #1105. All six are about the
correctness, test coverage, and API shape of the boundary-layer wiring — none
touch the #1104 taxonomy-drift debate. This work therefore proceeds
independently of taxonomy consensus; #1104 stays deferred.

## Comment disposition

| # | File / line | Category | Decision |
|---|-------------|----------|----------|
| 1 | `validator_store/src/lib.rs:590` | Correctness — handoff budget uses current wall-clock slot, not duty slot | Superseded by #2 |
| 2 | `validator_store/src/lib.rs:590` | API shape — move computation into `QbftManager::decide_instance`, delete threaded `Option<u64>` | **Adopt (relocate)** |
| 3 | `qbft_manager/src/instance.rs:331` | Diagnostic gap — init-replay window unobserved | **Adopt (make visible)** |
| 4 | `qbft_manager/src/instance.rs:441` | Test coverage — ChannelClosed path + histogram gate untested | **Adopt** |
| 5 | `qbft_manager/src/tests/timeout_tests.rs:131` | Test coverage — handoff wiring untested | **Adopt** |
| 6 | `qbft_manager/src/instance.rs:465` | Correctness — decided-round records local cursor, not certificate round | **Adopt (fix in `common/qbft`)** |

## Root-cause verification (grounded in source)

- **#1/#2:** `compute_handoff_budget_ms` → `millis_from_current_slot_start()` →
  `now_duration() % slot_duration`, i.e. modulo the *current* wall-clock slot.
  #1067 needs remaining budget for `block.slot()` (the duty slot). These coincide
  only while QBFT starts within its own slot; on a late start (the exact case the
  metric exists to detect) the modulo wraps and reports next-slot budget instead
  of ~0. Metric is wrong precisely when it matters.
- **#3:** Observer is created *after* `initialize()` returns (`instance.rs:331-337`),
  so round advances during buffered-message replay (`instance.rs:164-174`) are
  unobserved. Not recoverable cheaply; making the post-replay start state visible
  flags the gap.
- **#6:** Both success sites — local commit-quorum (`common/qbft/src/lib.rs:1026`)
  and `received_decided` (`:1218`) — set `completed = Success` **without** touching
  `current_round`. `get_round()` at finish therefore diverges from the decided
  certificate round on cross-round decided commits. `anchor_proposer_qbft_decided_round`
  is a named #921 acceptance criterion, so this is a correctness defect, not cosmetics.

## Design

### A. Handoff-budget relocation (#1 + #2)

**`qbft_manager/src/lib.rs`**
- New pure helper `compute_handoff_budget_ms<S: SlotClock>(slot_clock: &S, duty_slot: Slot) -> Option<u64>`
  returning `(start_of(duty_slot) + slot_duration).saturating_sub(now_duration())`
  as ms. All three `SlotClock` methods verified to exist and share the UNIX-epoch
  reference frame.
- In `decide_instance`, compute only for `matches!(message_id.role(), Some(Role::Proposer))`
  (use `matches!`, not `==`, to avoid assuming `PartialEq` on `Role`). `instance_height`
  already equals the duty slot for proposers.
- Remove `handoff_budget_ms` from the inherent `decide_instance`, the `ConsensusDecider`
  trait, and the trait impl. `QbftInitialization` keeps the field — only the computation
  moves.

**`validator_store/src/lib.rs` + `testing/common.rs`**
- Delete `compute_handoff_budget_ms` (and `determine_slot_elapsed_ms` if it becomes
  unused — confirm) plus its 4 unit tests. Drop the computed arg from the proposer
  call and `None` from the 4 non-proposer call sites. Drop `_handoff_budget_ms` from
  the mock decider.

**Risk:** `QbftInitialization` field retained, so `instance.rs` capture and tests'
manual construction are unaffected.

### B. Decided-round correctness (#6)

**`common/qbft/src/lib.rs`**
- Add `decided_round: Option<Round>`, init `None`.
- Local commit-quorum success: `self.decided_round = Some(round);`
- `received_decided` success: `self.decided_round = Some(wrapped_msg.qbft_message.round.into());`
  (`.into()` adds no new panic surface — `receive()` already converts round at dispatch, `:543`.)
- Add getter `pub fn decided_round(&self) -> Option<Round>`.
- `current_round` is never mutated → behavior-neutral.

**`qbft_manager/src/instance.rs`** — terminal finish site:
`let terminal_round = decided_round().unwrap_or_else(|| get_round());`
Timeout keeps local-round semantics; only cross-round decided commits change.

**Scope note:** #1067/#921 said `common/qbft` gains "only a getter." This is a
field + two inert assignments + getter — still behavior-neutral, but a deliberate
stretch to be called out in the commit/PR body.

### C. Init-replay visibility (#3)

**`qbft_manager/src/instance.rs`** — capture `start_round`/`start_state`
(already in scope post-initialize) and pass into `ProposerObserver::start`.

**`qbft_manager/src/instrumentation.rs`** — `start()` records `start_round`/`start_state`
as span fields and on the `qbft_instance_started` event, so `start_round > 1` /
`start_state != AwaitingProposal` surfaces the blind spot.

## Test plan (all authored via `tester-subagent`)

- **#4:** `histogram_sample_count` helper + ChannelClosed e2e test (drop `message_tx`,
  assert `channel_closed` counter +1 and both gated histograms unchanged). Module-level
  `tokio::sync::Mutex` serializes the three proposer-metric tests against process-global
  Prometheus state.
- **#5:** in max-timeout test, assert `HANDOFF_BUDGET_SECONDS` sample count +1 **and**
  sum += `budget/1000.0` (catches ms↔s regressions).
- **#2:** qbft_manager unit tests for the relocated helper, incl. a late-start case that
  would have caught the original modulo bug.
- **#6:** `common/qbft` unit test driving a cross-round decided certificate, asserting
  `decided_round()` returns the certificate round.

## Agents

- `tester-subagent` — every test (mandatory).
- `qbft-subagent` — confirm "decided round = certificate round" against EEA QBFT v1 (#6).
- `logging-subagent` — span/event field additions (#3).
- `code-reviewer-subagent` — after implementation.

## Verification

`make cargo-fmt && make cargo-fmt-check`, `make lint`, `make test`.

## Commit grouping

1. Handoff relocation (A).
2. Decided-round + replay visibility (B, C).
3. Tests (test plan).
