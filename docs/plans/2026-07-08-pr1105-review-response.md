# PR #1105 Review Response — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use kit:team-dev to implement this plan task-by-task.

**Goal:** Address the six maintainer review comments on PR #1105 (`be1aefd`) — fixing two correctness bugs (handoff-budget slot anchor, decided-round certificate), one diagnostic gap (init-replay visibility), and two test coverage gaps, while simplifying the cross-crate API.

**Architecture:** Move handoff-budget computation from `validator_store` (wrong reference frame) into `qbft_manager::decide_instance` (has the duty slot and role in scope). Add a `decided_round` field to `common/qbft::Qbft` set at both success sites so the observer records the certificate round. Surface post-replay start state on the observer span.

**Tech Stack:** Rust, Tokio, `tracing`, `prometheus` (via lighthouse `metrics` crate), `slot_clock` (lighthouse), `qbft` (`common/qbft`).

---

## Task 1: Relocate handoff-budget computation into `qbft_manager`

**Files:**
- Modify: `anchor/qbft_manager/src/lib.rs:197-260`
- Modify: `anchor/validator_store/src/lib.rs:155-168,588-601`
- Modify: `anchor/validator_store/src/testing/common.rs:50-61`

**Step 1: Add the helper to `qbft_manager/src/lib.rs`**

After the existing imports at the top (before the `QbftManager` impl block), add:

```rust
/// Compute how much slot time remains for `duty_slot` at the current instant.
///
/// Returns `(start_of(duty_slot) + slot_duration) − now`, saturating to 0.
/// Returns `None` when the slot clock cannot determine the required times.
fn compute_handoff_budget_ms<S: SlotClock>(slot_clock: &S, duty_slot: Slot) -> Option<u64> {
    let slot_end = slot_clock.start_of(duty_slot)? + slot_clock.slot_duration();
    let now = slot_clock.now_duration()?;
    Some(slot_end.saturating_sub(now).as_millis() as u64)
}
```

**Step 2: Use the helper in `decide_instance`**

In `QbftManager::decide_instance` (currently at line ~197), **before** the `QbftInitialization` construction (line ~245), insert:

```rust
let handoff_budget_ms = if message_id.role() == Some(Role::Proposer) {
    let duty_slot = Slot::new(*instance_height as u64);
    compute_handoff_budget_ms(&self.slot_clock, duty_slot)
} else {
    None
};
```

The `QbftInitialization` construction already has `handoff_budget_ms` — no change there.

**Step 3: Remove `handoff_budget_ms` from `decide_instance` method signature**

In the inherent method `pub async fn decide_instance<D: QbftDecidable<E>>` (line ~197), remove the parameter `handoff_budget_ms: Option<u64>`.

**Step 4: Remove from `ConsensusDecider` trait and impl**

In `trait ConsensusDecider<E: EthSpec>` (line ~400), remove `handoff_budget_ms: Option<u64>` from `fn decide_instance`.

In `impl<E: EthSpec, S: SlotClock + 'static> ConsensusDecider<E> for QbftManager<E, S>` (line ~412), remove the same parameter and remove it from the inner call to `self.decide_instance(...)`.

**Step 5: Remove from `validator_store` call sites**

In `anchor/validator_store/src/lib.rs`:
- Delete `let handoff_budget_ms = compute_handoff_budget_ms(&self.slot_clock);` (line 590).
- Remove the trailing `handoff_budget_ms,` arg from the proposer `decide_instance` call (line 601).
- At each of the 4 non-proposer calls (lines ~432, ~993, ~1148, ~1410, ~1523), remove the trailing `None,` argument and its comment.

**Step 6: Remove from mock decider**

In `anchor/validator_store/src/testing/common.rs`, remove `_handoff_budget_ms: Option<u64>,` from the `MockConsensusDecider::decide_instance` signature (line ~57).

**Step 7: Delete `compute_handoff_budget_ms` from `validator_store`**

Delete lines 161-168 (`fn compute_handoff_budget_ms`) from `anchor/validator_store/src/lib.rs`. Keep `determine_slot_elapsed_ms` — it has 4 other callers.

**Step 8: Delete the 4 unit tests for the removed helper**

Delete the tests at lines ~3426-3474 in `anchor/validator_store/src/lib.rs`:
- `test_compute_handoff_budget_ms_full_slot_remains_at_clock_start`
- `test_compute_handoff_budget_ms_partial_elapsed`
- `test_compute_handoff_budget_ms_near_end_of_slot`
- `test_compute_handoff_budget_ms_returns_none_when_pre_genesis`

**Step 9: Verify compilation**

Run: `cargo build -p qbft_manager -p validator_store --release 2>&1 | tail -5`
Expected: Compiles successfully.

---

## Task 2: Add unit tests for the relocated helper

**Files:**
- Modify: `anchor/qbft_manager/src/lib.rs` (add `#[cfg(test)]` module at bottom or in a `tests` submodule)

**Step 1: Write tests for `compute_handoff_budget_ms`**

Add to `qbft_manager/src/lib.rs` (at the bottom, in a new `#[cfg(test)] mod tests` block, or integrate with any existing one):

```rust
#[cfg(test)]
mod handoff_budget_tests {
    use super::compute_handoff_budget_ms;
    use slot_clock::{ManualSlotClock, SlotClock};
    use std::time::Duration;
    use types::Slot;

    fn clock_at(genesis_secs: u64, slot_duration_secs: u64, current_secs: u64) -> ManualSlotClock {
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(genesis_secs),
            Duration::from_secs(slot_duration_secs),
        );
        clock.set_current_time(Duration::from_secs(current_secs));
        clock
    }

    #[test]
    fn full_budget_at_slot_start() {
        // Duty slot 1 starts at 12s, slot duration 12s, clock at 12s (slot start).
        let clock = clock_at(0, 12, 12);
        let budget = compute_handoff_budget_ms(&clock, Slot::new(1));
        assert_eq!(budget, Some(12_000));
    }

    #[test]
    fn partial_elapsed() {
        // Duty slot 1 starts at 12s, clock at 15s (3s elapsed).
        let clock = clock_at(0, 12, 15);
        let budget = compute_handoff_budget_ms(&clock, Slot::new(1));
        assert_eq!(budget, Some(9_000));
    }

    #[test]
    fn saturates_to_zero_past_slot_end() {
        // Duty slot 1 ends at 24s, clock at 25s (1s past end → late start).
        let clock = clock_at(0, 12, 25);
        let budget = compute_handoff_budget_ms(&clock, Slot::new(1));
        assert_eq!(budget, Some(0));
    }

    #[test]
    fn returns_none_pre_genesis() {
        // Clock before genesis: now_duration() returns a value but start_of may return None
        // for slots that haven't started yet, or works correctly. Test with ManualSlotClock
        // set before genesis.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(100),
            Duration::from_secs(12),
        );
        clock.set_current_time(Duration::from_secs(50));
        // start_of(slot 1) = genesis(100) + 12 = 112. now = 50.
        // Budget = (112 + 12) - 50 = 74s — this is actually valid (pre-genesis but calculable).
        // A truly None case is when start_of returns None (invalid slot):
        // ManualSlotClock returns None for start_of when slot < genesis_slot is impossible.
        // Instead test with slot that makes start_of overflow:
        let budget = compute_handoff_budget_ms(&clock, Slot::new(u64::MAX));
        assert_eq!(budget, None);
    }
}
```

**Step 2: Run tests**

Run: `cargo test -p qbft_manager handoff_budget_tests --release 2>&1 | tail -10`
Expected: 4 tests pass.

---

## Task 3: Add `decided_round` to `common/qbft`

**Files:**
- Modify: `anchor/common/qbft/src/lib.rs`

**Step 1: Add the field to `Qbft` struct**

After `completed: Option<Completed<D::Hash>>,` (line ~138), add:

```rust
    /// The round recorded in the decided certificate. Set only on success paths.
    decided_round: Option<Round>,
```

**Step 2: Initialize in `Qbft::new`**

In the `Qbft { ... }` construction (line ~189), add:

```rust
            decided_round: None,
```

**Step 3: Set at local commit-quorum success**

In `received_commit`, after `self.completed = Some(Completed::Success(hash));` (line ~1026), add:

```rust
            self.decided_round = Some(round);
```

**Step 4: Set at `received_decided` success**

In `received_decided`, after `self.completed = Some(Completed::Success(wrapped_msg.qbft_message.root));` (line ~1218), add:

```rust
        self.decided_round = Some(wrapped_msg.qbft_message.round.into());
```

**Step 5: Add getter**

After `pub fn get_round(&self) -> Round` (line ~225), add:

```rust
    /// Returns the round from the decided certificate, if consensus was reached.
    /// `None` for timeout/incomplete instances.
    pub fn decided_round(&self) -> Option<Round> {
        self.decided_round
    }
```

**Step 6: Verify compilation**

Run: `cargo build -p qbft --release 2>&1 | tail -5`
Expected: Compiles successfully.

---

## Task 4: Use `decided_round` in the observer finish site

**Files:**
- Modify: `anchor/qbft_manager/src/instance.rs:458-466`

**Step 1: Update the terminal-outcome `finish` call**

Replace the current block at line ~458-465:

```rust
            if let Some(observer) = &observer
                && let Some(completed) = initialized.qbft.completed()
            {
                let outcome = match completed {
                    Completed::Success(_) => ProposerOutcome::Decided,
                    Completed::TimedOut => ProposerOutcome::MaxRoundTimeout,
                };
                observer.finish(outcome, u64::from(initialized.qbft.get_round()));
            }
```

With:

```rust
            if let Some(observer) = &observer
                && let Some(completed) = initialized.qbft.completed()
            {
                let outcome = match completed {
                    Completed::Success(_) => ProposerOutcome::Decided,
                    Completed::TimedOut => ProposerOutcome::MaxRoundTimeout,
                };
                let terminal_round = initialized
                    .qbft
                    .decided_round()
                    .unwrap_or_else(|| initialized.qbft.get_round());
                observer.finish(outcome, u64::from(terminal_round));
            }
```

**Step 2: Update the ChannelClosed `finish` call similarly**

At line ~441-445:

```rust
                    if let Some(observer) = &observer {
                        observer.finish(
                            ProposerOutcome::ChannelClosed,
                            u64::from(initialized.qbft.get_round()),
                        );
                    }
```

Replace with:

```rust
                    if let Some(observer) = &observer {
                        let terminal_round = initialized
                            .qbft
                            .decided_round()
                            .unwrap_or_else(|| initialized.qbft.get_round());
                        observer.finish(ProposerOutcome::ChannelClosed, u64::from(terminal_round));
                    }
```

**Step 3: Verify compilation**

Run: `cargo build -p qbft_manager --release 2>&1 | tail -5`
Expected: Compiles successfully.

---

## Task 5: Surface init-replay start state on observer

**Files:**
- Modify: `anchor/qbft_manager/src/instrumentation.rs:138`
- Modify: `anchor/qbft_manager/src/instance.rs:331-337`

**Step 1: Extend `ProposerObserver::start` signature**

Change `pub fn start(instance_height: u64, handoff_budget_ms: Option<u64>) -> Self` to:

```rust
    pub fn start(
        instance_height: u64,
        handoff_budget_ms: Option<u64>,
        start_round: u64,
        start_state: &str,
    ) -> Self {
```

**Step 2: Add span fields and event fields**

Update the `info_span!` to include the new fields:

```rust
        let span = info_span!(
            "proposer_qbft_instance",
            role = "proposer",
            instance_height,
            start_round,
            start_state,
            handoff_budget_ms = field::Empty,
            decided_round = field::Empty,
            outcome = field::Empty,
            duration_ms = field::Empty,
        );
```

And update the start event:

```rust
        span.in_scope(|| {
            info!(
                checkpoint = checkpoints::QBFT_INSTANCE_STARTED,
                start_round,
                start_state,
                "Proposer QBFT instance started"
            );
        });
```

**Step 3: Update the call site in `instance.rs`**

Replace lines 334-337:

```rust
                            let instance_height = *initialized.qbft.get_instance_height() as u64;
                            observer =
                                Some(ProposerObserver::start(instance_height, handoff_budget_ms));
```

With:

```rust
                            let instance_height = *initialized.qbft.get_instance_height() as u64;
                            let start_round = u64::from(initialized.qbft.get_round());
                            let start_state = format!("{:?}", initialized.qbft.state_kind());
                            observer = Some(ProposerObserver::start(
                                instance_height,
                                handoff_budget_ms,
                                start_round,
                                &start_state,
                            ));
```

**Step 4: Verify compilation**

Run: `cargo build -p qbft_manager --release 2>&1 | tail -5`
Expected: Compiles successfully.

---

## Task 6: Add ChannelClosed regression test (comment #4)

**Files:**
- Modify: `anchor/qbft_manager/src/tests/timeout_tests.rs`

**Step 1: Add a module-level Mutex for metric isolation**

At the top of `timeout_tests.rs`, after existing imports, add:

```rust
use tokio::sync::Mutex as TokioMutex;
use std::sync::LazyLock;

/// Serializes proposer-metric e2e tests. These tests assert before/after deltas on process-global
/// Prometheus counters/histograms, so they must not overlap.
static PROPOSER_METRIC_LOCK: LazyLock<TokioMutex<()>> = LazyLock::new(|| TokioMutex::new(()));
```

**Step 2: Add histogram helper**

```rust
fn histogram_sample_count(metric: &metrics::Result<metrics::Histogram>) -> u64 {
    metric
        .as_ref()
        .map(|h| h.get_sample_count())
        .unwrap_or(0)
}

fn histogram_sample_sum(metric: &metrics::Result<metrics::Histogram>) -> f64 {
    metric
        .as_ref()
        .map(|h| h.get_sample_sum())
        .unwrap_or(0.0)
}
```

**Step 3: Add the ChannelClosed test**

```rust
/// Objective: Verify the ChannelClosed outcome path and its histogram gate.
/// Dropping `message_tx` closes the channel; the observer should record
/// `outcome="channel_closed"` but must NOT record decided-round or duration histograms.
#[tokio::test(start_paused = true)]
async fn test_proposer_instance_channel_closed_runs_observer() {
    let _guard = PROPOSER_METRIC_LOCK.lock().await;

    const CHANNEL_CLOSED_OUTCOME: &str = "channel_closed";

    let outcome_before = proposer_outcome_count(CHANNEL_CLOSED_OUTCOME);
    let decided_round_count_before = histogram_sample_count(&metrics::PROPOSER_QBFT_DECIDED_ROUND);
    let duration_count_before = histogram_sample_count(&metrics::PROPOSER_QBFT_DURATION_SECONDS);

    let config = qbft::ConfigBuilder::new(
        OperatorId(1),
        InstanceHeight::from(0),
        IndexSet::from([1, 2, 3, 4].map(OperatorId)),
    )
    .build()
    .unwrap();

    let (message_tx, result_rx, _start_data) = spawn_proposer_instance(config, Some(2_000));

    // Give the instance a turn to initialize and start the observer.
    tokio::task::yield_now().await;

    // Drop sender to close the channel.
    drop(message_tx);

    assert!(
        matches!(result_rx.await, Ok(Completed::TimedOut)),
        "ChannelClosed should signal TimedOut to listeners"
    );

    let outcome_after = proposer_outcome_count(CHANNEL_CLOSED_OUTCOME);
    assert!(
        outcome_after > outcome_before,
        "ProposerObserver::finish should increment outcome counter for channel_closed \
         (before={outcome_before}, after={outcome_after})"
    );

    // Histogram gate: decided-round and duration must NOT be recorded for ChannelClosed.
    let decided_round_count_after = histogram_sample_count(&metrics::PROPOSER_QBFT_DECIDED_ROUND);
    let duration_count_after = histogram_sample_count(&metrics::PROPOSER_QBFT_DURATION_SECONDS);
    assert_eq!(
        decided_round_count_before, decided_round_count_after,
        "decided-round histogram must not record on ChannelClosed"
    );
    assert_eq!(
        duration_count_before, duration_count_after,
        "duration histogram must not record on ChannelClosed"
    );
}
```

**Step 4: Wrap existing proposer e2e tests with the mutex**

Add `let _guard = PROPOSER_METRIC_LOCK.lock().await;` as the first line of:
- `test_proposer_instance_max_round_timeout_runs_observer`
- `test_proposer_instance_decided_runs_observer`

**Step 5: Run test to verify pass**

Run: `cargo test -p qbft_manager test_proposer_instance_channel_closed --release 2>&1 | tail -10`
Expected: PASS.

---

## Task 7: Assert handoff-budget wiring in max-timeout test (comment #5)

**Files:**
- Modify: `anchor/qbft_manager/src/tests/timeout_tests.rs` (the max-timeout test)

**Step 1: Add assertions after `result_rx.await`**

In `test_proposer_instance_max_round_timeout_runs_observer`, before the existing outcome assertion, capture metric state; after the outcome assertion, assert the handoff budget was recorded:

Before spawning (after `let _guard = ...`):

```rust
    let budget_count_before =
        histogram_sample_count(&metrics::PROPOSER_QBFT_HANDOFF_BUDGET_SECONDS);
    let budget_sum_before =
        histogram_sample_sum(&metrics::PROPOSER_QBFT_HANDOFF_BUDGET_SECONDS);
```

After `let outcome_after = proposer_outcome_count(MAX_ROUND_TIMEOUT_OUTCOME);` assertion block, add:

```rust
    let budget_count_after =
        histogram_sample_count(&metrics::PROPOSER_QBFT_HANDOFF_BUDGET_SECONDS);
    let budget_sum_after =
        histogram_sample_sum(&metrics::PROPOSER_QBFT_HANDOFF_BUDGET_SECONDS);
    assert_eq!(
        budget_count_after,
        budget_count_before + 1,
        "handoff budget histogram should record exactly one sample"
    );
    let expected_budget_secs = HANDOFF_BUDGET_MS as f64 / 1000.0;
    assert!(
        (budget_sum_after - budget_sum_before - expected_budget_secs).abs() < f64::EPSILON,
        "handoff budget histogram sum should increase by {expected_budget_secs}s \
         (before={budget_sum_before}, after={budget_sum_after})"
    );
```

**Step 2: Run test**

Run: `cargo test -p qbft_manager test_proposer_instance_max_round_timeout --release 2>&1 | tail -10`
Expected: PASS.

---

## Task 8: Add `decided_round` unit test in `common/qbft`

**Files:**
- Modify: `anchor/common/qbft/src/lib.rs` (existing `#[cfg(test)]` module, or new one)

**Step 1: Write test for cross-round decided certificate**

Find the existing test module. Add a test that creates a fresh instance at round 1, feeds it a quorum-signed decided commit for round 3, and asserts `decided_round() == Some(Round::from(3))` while `get_round() == Round::from(1)` (unchanged):

```rust
#[test]
fn decided_round_set_by_received_decided() {
    // Fresh instance at round 1. Feed it a quorum-signed decided commit at round 3.
    // decided_round() should return Some(3), get_round() should stay at 1.
    let mut inst = /* create a fresh instance using existing test helpers */;
    
    assert_eq!(inst.decided_round(), None);
    assert_eq!(inst.get_round(), Round::from(1u64));

    // Build a decided message (multi-signer commit) for round 3.
    // This requires the message to pass has_quorum and data validation.
    // Use the same pattern as existing `received_decided` tests if they exist,
    // or construct a SignedSSVMessage with quorum operator_ids.
    
    // After receiving:
    // assert_eq!(inst.decided_round(), Some(Round::from(3u64)));
    // assert_eq!(inst.get_round(), Round::from(1u64)); // unchanged
}
```

**NOTE:** The exact message construction depends on existing test helpers in `common/qbft`. Delegate to `tester-subagent` for the full implementation — the agent should grep for existing `received_decided` test patterns or `has_quorum` usage in tests.

**Step 2: Run test**

Run: `cargo test -p qbft decided_round --release 2>&1 | tail -10`
Expected: PASS.

---

## Task 9: Full verification

**Step 1: Format**

Run: `make cargo-fmt && make cargo-fmt-check`
Expected: No formatting issues.

**Step 2: Lint**

Run: `make lint`
Expected: No warnings (`-D warnings`).

**Step 3: Full test suite**

Run: `make test`
Expected: All tests pass.

---

## Dependency graph

```
Task 1 (relocate handoff) ─┐
                            ├─→ Task 2 (relocated helper tests)
                            │
Task 3 (decided_round in qbft) ──→ Task 4 (use in instance.rs) ──→ Task 8 (qbft test)
                                                                │
Task 5 (init-replay visibility) ────────────────────────────────┘
                                                                │
Task 6 (ChannelClosed test) ────────────────────────────────────┤
Task 7 (handoff assertion) ─────────────────────────────────────┘
                                                                │
                                                                └─→ Task 9 (full verification)
```

Tasks 1, 3, and 5 are independent and can be done in parallel.
Tasks 6 and 7 depend on Tasks 1 and 5 (for compilation).
Task 8 depends on Task 3.
Task 9 is the final gate.
