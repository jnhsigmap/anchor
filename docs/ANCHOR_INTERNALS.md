# Anchor Internals: A Maintainer's Field Guide to SSV Validator Duties

> **Audience.** This document is written for someone who is becoming a maintainer of
> the Anchor codebase and who is *new* to Ethereum, the consensus layer, and SSV.
> It is framed from the point of view of a **node operator** running an Anchor node
> that is performing validator duties on behalf of many validators it co-owns with
> other operators. For each major concept we give: (1) what it *is*, (2) the
> **hot-path code** that implements it (with `file:line` anchors), (3) the **types and
> data** involved, and (4) **where it fits into a slot** and the consensus layer's
> operation.
>
> All line numbers reference the tree at the time of writing (`unstable`). When code
> moves, search for the quoted function/struct names — they are stable identifiers.

---

## Table of contents

1. [The mental model: why Anchor exists](#1-the-mental-model-why-anchor-exists)
2. [The slot is the heartbeat](#2-the-slot-is-the-heartbeat)
3. [The data model: operators, shares, clusters, committees](#3-the-data-model-operators-shares-clusters-committees)
4. [The wire format: SSV messages and message IDs](#4-the-wire-format-ssv-messages-and-message-ids)
5. [The processor: Anchor's central nervous system](#5-the-processor-anchors-central-nervous-system)
6. [Learning your responsibilities: the Eth event syncer](#6-learning-your-responsibilities-the-eth-event-syncer)
7. [Discovering duties: the duties tracker](#7-discovering-duties-the-duties-tracker)
8. [Network topology: subnets and gossipsub topics](#8-network-topology-subnets-and-gossipsub-topics)
9. [The message pipeline: ingress and egress](#9-the-message-pipeline-ingress-and-egress)
10. [QBFT: agreeing on what to sign](#10-qbft-agreeing-on-what-to-sign)
11. [Threshold signatures: the signature collector](#11-threshold-signatures-the-signature-collector)
12. [The validator store: every duty, walked through](#12-the-validator-store-every-duty-walked-through)
13. [End-to-end: an attestation through one slot](#13-end-to-end-an-attestation-through-one-slot)
14. [Forks: how behavior changes over time](#14-forks-how-behavior-changes-over-time)
15. [ePBS / Gloas: Anchor's EIP-7732 support (in progress)](#15-epbs--gloas-anchors-eip-7732-support-in-progress)
16. [A maintainer's debugging map](#16-a-maintainers-debugging-map)

---

## 1. The mental model: why Anchor exists

### The problem SSV solves

On Ethereum's **consensus layer (CL)**, a *validator* is an entity with a single BLS
key pair, identified by a `validator_index`, that earns rewards by performing
**duties**: attesting to the chain head every epoch, occasionally proposing blocks,
participating in sync committees, and aggregating others' work. Missing duties or
signing two conflicting messages (getting **slashed**) costs money. A single machine
holding the validator's private key is a single point of failure: if it goes down you
miss duties; if it's compromised the key is stolen.

**SSV (Secret Shared Validators)**, also called **DVT (Distributed Validator
Technology)**, removes that single point of failure. The validator's BLS private key is
split — using **Shamir secret sharing** — into *N* **key shares**, one per **operator**.
No operator ever holds the full key. To produce a valid signature for a duty, a
**threshold** of operators (typically `2f+1` of `3f+1`) must each sign with their share
and combine the partial signatures, via **Lagrange interpolation**, into the one
signature the beacon chain expects. The group of operators co-running a validator is a
**cluster**.

Two hard problems fall out of this:

1. **Agreement.** All operators must sign *the same* attestation data / block. If
   operator A signs a vote for head `0xAAA` and operator B signs a vote for `0xBBB`,
   their partial signatures won't combine into anything valid — and worse, they could
   produce a slashable mess. Operators reach agreement using a Byzantine-fault-tolerant
   consensus protocol: **QBFT**.
2. **Reconstruction.** Once everyone agrees on the data and produces a partial
   signature over it, someone must collect a threshold of those partials and
   reconstruct the full BLS signature. That's the **signature collector**.

Anchor is Sigma Prime's Rust implementation of an SSV operator node. It does **not**
re-implement the beacon chain logic; it leans on **Lighthouse** crates (`types`,
`slot_clock`, `validator_services`, `slashing_protection`, `beacon_node_fallback`,
`eth2`) for the Ethereum side, and adds the SSV-specific layers: event syncing from
the SSV smart contracts, the QBFT engine, partial-signature collection, and a libp2p
gossip network distinct from the beacon network.

### The 30-second architecture

```
            ┌─────────────────────── Execution Layer (Eth L1) ──────────────────────┐
            │  SSV smart contract emits ValidatorAdded / OperatorAdded / ... events  │
            └───────────────────────────────┬───────────────────────────────────────┘
                                             │ (eth::SsvEventSyncer)
                                             ▼
                              ┌──────────────────────────────┐
                              │  NetworkDatabase (SQLite +    │
                              │  in-memory NetworkState)      │  ← "who am I, what do I co-own"
                              └───────────────┬──────────────┘
                                              │ watch::Receiver<NetworkState>
        ┌──────────────────┬──────────────────┼──────────────────┬───────────────────┐
        ▼                  ▼                   ▼                  ▼                   ▼
 DutiesTracker      SubnetService      ValidatorStore       MessageValidator    QbftManager /
 (when do I have    (which gossip      (Lighthouse asks     (gate inbound       SignatureCollector
  duties?)           topics to join?)   me to sign X)        gossip)            (consensus + threshold sig)
        │                  │                   │                  │                   │
        └──────────────────┴─────────┬─────────┴──────────────────┴───────────────────┘
                                      ▼
                        ┌──────────────────────────┐        ┌────────────────────────┐
                        │  Processor (task queues)  │◀──────▶│  Network (libp2p        │
                        │  every CPU-bound or async │        │  gossipsub over SSV     │
                        │  job flows through here   │        │  subnets)               │
                        └──────────────────────────┘        └────────────────────────┘
                                      ▲                                  ▲
                                      │                                  │
                       Beacon Node (Lighthouse / any CL) ───────────────┘
                       (HTTP: duties, attestation data, block bodies, publish)
```

Everything is wired together in one place: **`anchor/client/src/lib.rs:92`**,
`Client::run`. Read that function top to bottom once — it is the spine of the whole
program. It builds the beacon-node HTTP clients, opens the database, starts the event
syncer, then constructs the network, message pipeline, QBFT manager, signature
collector, and finally the Lighthouse-style duty services (attestation, block,
sync-committee, preparation, registration).

A critical ordering detail worth internalizing: **services do not start until
historical sync of the SSV contract is complete.** From `client/src/lib.rs:698`:

```rust
// Wait for sync to complete before starting services
info!("Waiting for sync to complete before starting services...");
is_synced
    .clone()
    .wait_for(|&is_synced| is_synced)
    .await
    .map_err(|_| "Sync watch channel closed")?;
info!("Sync complete, starting services...");
```

So if your node "does nothing" on startup, the first question is always: *has the
event syncer finished its historical pass?* (See §6.)

---

## 2. The slot is the heartbeat

Everything in the consensus layer is timed to **slots** (12 seconds on mainnet) and
**epochs** (32 slots = 6.4 minutes). Anchor uses Lighthouse's `SystemTimeSlotClock`,
constructed from genesis time at `client/src/lib.rs:334`:

```rust
let slot_clock = SystemTimeSlotClock::new(
    spec.genesis_slot,
    Duration::from_secs(genesis_time),
    Duration::from_secs(spec.seconds_per_slot),
);
```

The CL defines well-known **offsets within a slot** at which honest validators act.
Anchor honors these because its consensus must finish in time for the partial
signatures to be produced and the final signature published before the beacon chain
stops caring.

Anchor treats these offsets as trigger points where a consensus action starts at the offest, not before or after it. Anchor's typical happy path then completes 100-500ms after the first 1/3 deadline.

| Offset in slot | Vanilla CL action | What Anchor does at this point |
|---|---|---|
| **t = 0** (slot start) | New slot begins | `MetadataService` Phase 1: compute which of our validators attest / are in sync committees this slot (`metadata_service.rs`, "VotingAssignments") |
| **t = 1/3 slot** (4s) | Attestation deadline — validators attest to the head | `MetadataService` Phase 2 fires (or a head-event arrives): the `BeaconVote` (head/source/target) is fixed; **attestation & sync-message QBFT instances start** (`validator_store/src/lib.rs:1375`, `1486` — `instance_start_time = slot_start + slot/3`) |
| **t = 2/3 slot** (8s) | Aggregation deadline — aggregators bundle attestations | `MetadataService` Phase 3 fires (`metadata_service.rs:306`, `duration_to_next_slot + slot*2/3`): aggregate/contribution consensus data is built; **aggregator QBFT instances start** (`validator_store/src/lib.rs:408`, `941`, `1093` — `slot/3 ... slot*2/3` window) |
| **block proposal** | Proposer builds & publishes a block at slot start | Block proposal uses `TimeoutMode::Relative` (per-round timers) rather than slot-cumulative (`validator_store/src/lib.rs:535`) |

The reason the timing matters in code: a QBFT instance has a **timeout schedule**
(§10.4). Attestation-type duties use `TimeoutMode::SlotTime` — round timeouts are
*cumulative from a fixed instance-start instant* tied to the 1/3 or 2/3 mark — so that
all operators' timers are aligned to the slot, not to when each operator happened to
spin up its instance. Block proposals use `TimeoutMode::Relative` (timer resets each
round) to match go-ssv. This is `qbft_manager/src/lib.rs:52`:

```rust
/// Determines how round timeouts are calculated.
pub enum TimeoutMode {
    /// Cumulative timeouts from instance start. Never resets.
    /// Used for: attestations, aggregations, sync committee.
    SlotTime { instance_start_time: Instant },
    /// Per-round timeouts. Resets on round changes.
    /// Used for: block proposals.
    Relative { current_round_start_time: Instant },
}
```

**Maintainer takeaway:** when a duty is "sometimes late," the offset clock and the
timeout mode are the first things to check. A wrong `instance_start_time` means every
operator's first round expires before the proposal can land.

---

## 3. The data model: operators, shares, clusters, committees

These types live in `anchor/common/ssv_types/` and are the vocabulary of the whole
system. Learn them first; everything else is built from them.

### Operator — a participant in the network

`anchor/common/ssv_types/src/operator.rs:14` and `:70`:

```rust
pub struct OperatorId(pub u64);

pub struct Operator {
    /// ID to uniquely identify this operator
    pub id: OperatorId,
    /// Base-64 encoded PEM RSA public key
    pub rsa_pubkey: Rsa<Public>,
    /// Owner of the operator
    pub owner: Address,
}
```

An operator authenticates network messages with an **RSA** key pair (not BLS — RSA is
used for SSV message authenticity; BLS is used only for the validator-duty signatures).
*Your* node's operator identity is read from disk at startup
(`client/src/lib.rs:130`, `read_or_generate_private_key`) and your `OperatorId` is
discovered later from the chain (see `OwnOperatorId`, §6).

### Share — one slice of a validator key

`anchor/common/ssv_types/src/share.rs:8`:

```rust
pub struct Share {
    /// Public Key of the validator
    pub validator_pubkey: PublicKeyBytes,
    /// Operator this share belongs to
    pub operator_id: OperatorId,
    /// Cluster the operator who owns this share belongs to
    pub cluster_id: ClusterId,
    /// The public key of this Share
    pub share_pubkey: PublicKeyBytes,
    /// The encrypted private key of the share
    pub encrypted_private_key: [u8; ENCRYPTED_KEY_LENGTH], // 256 bytes
}
```

The `encrypted_private_key` is encrypted to *your* operator RSA key. It stays encrypted
at rest in the DB and is decrypted lazily, the first time you need to sign for that
validator, then cached in an LRU (`validator_store/src/lib.rs`, the
`decrypted_keys: Mutex<LruCache<...>>` field).

### Cluster — the operators co-running a set of validators

`anchor/common/ssv_types/src/cluster.rs:25`:

```rust
pub struct Cluster {
    pub cluster_id: ClusterId,          // [u8; 32]
    pub owner: Address,                 // the EOA that registered the cluster
    pub fee_recipient: Address,         // where execution-layer tips go
    pub liquidated: bool,               // if true, this cluster is inactive
    pub cluster_members: IndexSet<OperatorId>, // the operator set
}
```

A cluster's `cluster_members` is the operator set that holds shares for every validator
registered under that cluster. `liquidated` is important operationally: an SSV cluster
that runs out of pre-paid fees is **liquidated** on-chain and must stop performing
duties.

### Committee — the consensus group, identified by a hash of operators

A **committee** in SSV terms is the deterministic identity of an operator set.
`anchor/common/ssv_types/src/committee.rs:36`:

```rust
impl From<&[OperatorId]> for CommitteeId {
    fn from(operator_ids: &[OperatorId]) -> Self {
        let mut hasher = Sha256::new();
        for id in operator_ids {
            hasher.update((id.0 as u32).to_le_bytes());
        }
        <[u8; COMMITTEE_ID_LEN]>::from(hasher.finalize()).into() // 32 bytes
    }
}
```

> **Cluster vs. committee** — a frequent source of confusion. A *cluster* is the
> on-chain billing/ownership entity (owner + fee recipient + a specific operator set).
> A *committee* (`CommitteeId`) is just the hash of the sorted operator set. Many
> clusters with the same operators share one `CommitteeId`, and SSV **batches duties at
> the committee level**: one QBFT instance per committee per slot decides the head
> vote for *all* validators that committee runs. This batching is the key scalability
> trick — see §13.

### Validator metadata and index

`anchor/common/ssv_types/src/cluster.rs:102` and `:73`:

```rust
pub struct ValidatorMetadata {
    pub public_key: PublicKeyBytes,
    pub cluster_id: ClusterId,
    pub index: Option<ValidatorIndex>, // None until resolved from the beacon node
    pub graffiti: Graffiti,
}

pub struct ValidatorIndex(pub usize);
```

`index` is `None` when a validator is first seen on-chain (the contract only knows the
pubkey). The **index syncer** (§6.4) resolves it from the beacon node. Most duties need
the index, so a validator with `index: None` is effectively dormant.

### Threshold / quorum arithmetic — `2f+1`

`anchor/common/ssv_types/src/lib.rs:29`:

```rust
/// Maximum Byzantine/faulty members a committee of `members` can tolerate:
/// `f = ⌊(N − 1) / 3⌋`.
pub fn get_f(members: usize) -> usize {
    members.saturating_sub(1) / 3
}

/// QBFT quorum threshold: `2f + 1`.
pub fn quorum_size(committee_size: usize) -> usize {
    get_f(committee_size) * 2 + 1
}
```

For the canonical SSV sizes: 4 operators → f=1, quorum=3; 7 → f=2, quorum=5; 10 → f=3,
quorum=7; 13 → f=4, quorum=9. This single number governs **both** how many QBFT votes
make a decision **and** how many partial signatures reconstruct a full signature.

---

## 4. The wire format: SSV messages and message IDs

Every byte that crosses the SSV network is a `SignedSSVMessage`.
`anchor/common/ssv_types/src/message.rs:352`:

```rust
pub type SignatureList = VariableList<VariableList<u8, U256>, U13>; // ≤13 RSA sigs

pub struct SignedSSVMessage {
    signatures: SignatureList,              // RSA signatures (operator authenticity)
    operator_ids: VariableList<OperatorId, U13>,
    ssv_message: SSVMessage,                // the actual payload
    full_data: VariableList<u8, SSVMessageFullDataLen>, // e.g. the block/attestation data
}
```

The inner `SSVMessage` (`message.rs:172`) carries a type, an ID, and SSZ-encoded data:

```rust
pub struct SSVMessage {
    msg_type: MsgType,                       // Consensus (0) or PartialSignature (1)
    msg_id: MessageId,                       // 56 bytes — see below
    data: VariableList<u8, SSVMessageDataLen>,
}

pub enum MsgType {
    SSVConsensusMsgType = 0,         // a QBFT message
    SSVPartialSignatureMsgType = 1,  // a partial-signature message
}
```

### The 56-byte MessageId — routing identity

The `MessageId` answers "which fork, which duty, which executor" and is the basis for
topic routing and for finding the right QBFT instance / collector.
`anchor/common/ssv_types/src/msgid.rs:115`:

```rust
impl MessageId {
    pub fn new(domain: &DomainType, role: Role, duty_executor: &DutyExecutor) -> Self {
        let mut id = [0; 56];
        id[0..4].copy_from_slice(&domain.0);              // fork domain
        id[4..8].copy_from_slice(&<[u8; 4]>::from(role));  // duty role
        match duty_executor {
            DutyExecutor::Committee(committee_id) => id[24..].copy_from_slice(committee_id.as_slice()),
            DutyExecutor::Validator(public_key)   => id[8..].copy_from_slice(public_key.as_serialized()),
        }
        MessageId(id)
    }
}
```

The **role** (`msgid.rs:14`) tells you what kind of duty the message is about:

```rust
pub enum Role {
    Committee,              // batched attestation + sync-committee message
    Aggregator,             // attestation aggregation (pre-Boole, per-validator)
    Proposer,               // block proposal (and randao)
    SyncCommittee,          // sync committee contribution (pre-Boole)
    ValidatorRegistration,  // builder/MEV registration
    VoluntaryExit,          // exit
    AggregatorCommittee,    // batched aggregation (Boole+)
}
```

Note the `DutyExecutor` split: **committee** roles (`Committee`, `AggregatorCommittee`)
are keyed by `CommitteeId`; everything else is keyed by the individual validator
pubkey. This mirrors the batching design — committee-level duties identify themselves by
the operator set, per-validator duties by the validator.

### The QBFT message and the partial-signature message

A consensus payload is a `QbftMessage` (`consensus.rs:104`):

```rust
pub struct QbftMessage {
    pub qbft_message_type: QbftMessageType,  // Proposal/Prepare/Commit/RoundChange
    pub height: u64,                         // ≈ the slot number
    pub round: u64,
    pub identifier: VariableList<u8, U56>,   // the MessageId again
    pub root: Hash256,                       // tree-hash of the data being agreed
    pub data_round: u64,                     // round where data was last prepared
    pub round_change_justification: ...,     // signed justifications (≤13)
    pub prepare_justification: ...,
}
```

A partial-signature payload is `PartialSignatureMessages` (`partial_sig.rs:120`):

```rust
pub struct PartialSignatureMessages {
    pub kind: PartialSignatureKind,  // PostConsensus / Randao / SelectionProof / ...
    pub slot: Slot,
    pub messages: VariableList<PartialSignatureMessage, PartialSignatureMessagesLen>,
}

pub struct PartialSignatureMessage {   // partial_sig.rs:128
    pub partial_signature: Signature,  // a BLS signature share (96 bytes)
    pub signing_root: Hash256,         // what was signed
    pub signer: OperatorId,            // who produced this share
    pub validator_index: ValidatorIndex,
}
```

`PartialSignatureKind` (`partial_sig.rs:20`) distinguishes the *pre-consensus* shares
(randao reveal, selection proofs — these don't need QBFT) from the *post-consensus*
shares (the signature over the agreed attestation/block). Keep this distinction in mind
for §12.

---

## 5. The processor: Anchor's central nervous system

Before any duty logic makes sense, you need the **processor**. It plays the role
Lighthouse's `beacon_processor` plays: a bounded, priority-ranked work scheduler that
keeps the node from overloading itself. *Almost every CPU-bound or fan-out task in
Anchor is submitted to the processor rather than spawned ad hoc.*

It's started first thing in `Client::run` (`client/src/lib.rs:143`):

```rust
let processor_senders = processor::spawn(config.processor, executor.clone());
```

`processor::spawn` (`processor/src/lib.rs:106`) creates the queues and returns
`Senders`, which is cloned into every component that needs to schedule work.

### Two queues, one semaphore

`anchor/processor/src/senders.rs:11`:

```rust
pub struct Senders {
    /// Catch-all queue for quick or well-behaved async tasks. Runs immediately,
    /// does not require a permit.
    pub permitless: Sender,
    pub urgent_consensus: Sender,
}
```

The receive side (`processor/src/receivers.rs:49`) implements a **two-level priority
scheduler** using Tokio's `select!` macro with the `biased;` directive:

```rust
pub async fn next_work_item(&mut self, semaphore: &Arc<Semaphore>) -> Option<ReceivedWork> {
    select! {
        biased;
        Ok(permit) = semaphore.clone().acquire_owned() => {
            self.next_work_item_with_permit(permit).await
        },
        Some(work_item) = self.permitless.recv() => Some(work_item),
        else => None,
    }
}
```

#### What is a Semaphore?

A **semaphore** is a concurrency primitive that controls how many tasks can proceed
simultaneously. It holds N "permits" (tokens). A task must *acquire* a permit before
running — if none are free it waits. When the task finishes, the permit is released
(dropped) and another waiting task can proceed. This prevents the system from spawning
unbounded concurrent work that would overwhelm CPU or memory.

Anchor creates the semaphore with `config.max_workers` permits (defaults to
`num_cpus::get()` — the number of logical CPU cores on the machine). This means at most
N heavy tasks run concurrently, regardless of how fast work items arrive.

#### How the `select!` works line by line

Tokio's `select!` polls multiple async branches simultaneously and resolves the first
one that becomes ready. The `biased;` directive disables the random fairness that
`select!` normally applies — instead it always tries branches **top to bottom**. Here
that means:

1. **First branch — acquire a permit:** `semaphore.clone().acquire_owned()` attempts to
   take a permit. If a permit is available (worker capacity exists), this branch wins
   immediately and calls `next_work_item_with_permit(permit)`. That inner method
   (`receivers.rs:73`) then does *another* biased select between the two queues:

   ```rust
   pub async fn next_work_item_with_permit(&mut self, permit: OwnedSemaphorePermit) -> Option<ReceivedWork> {
       Some(select! {
           biased;
           Some(work_item) = self.urgent_consensus.recv() => work_item.with_permit(permit),
           Some(work_item) = self.permitless.recv() => work_item, // permit dropped — not consumed
           else => return None,
       })
   }
   ```

   Inside, `urgent_consensus` is tried first. If urgent work is waiting, it gets the
   permit (the permit stays alive for the duration of that task, occupying one worker
   slot). If only permitless work is pending, the item is returned without attaching the
   permit — the permit is simply dropped, immediately freeing the slot.

2. **Second branch — permitless fallback:** If no permit is available (all worker slots
   are busy with urgent work), the outer select falls through to the `permitless` branch
   and returns whatever is waiting there — with no permit attached. This guarantees
   permitless work is **never starved** even when the system is at maximum load.

3. **`else` branch:** If both channels are closed (all senders dropped), returns `None`,
   which signals the processor loop to exit.

#### The effect: bounded concurrency with starvation-free lightweight work

The net result is:

- **`UrgentConsensus` work** (QBFT consensus, partial-signature signing) gets
  first-class access to worker permits. It runs with bounded concurrency — at most
  `max_workers` such tasks execute simultaneously.
- **`Permitless` work** (fast, non-blocking tasks like routing an inbound network
  message to the correct QBFT instance, or emitting a heartbeat) runs without consuming
  a permit. It never has to wait for worker capacity, so it cannot be blocked behind a
  pile of heavy consensus tasks. The tradeoff is that permitless work has no concurrency
  cap — it must therefore be genuinely lightweight (a few microseconds, no blocking I/O).
- **Priority:** when both queues have work and a permit exists, urgent consensus always
  wins (the `biased;` directive guarantees top-to-bottom evaluation). Permitless work
  only gets dequeued when either (a) no urgent work is waiting, or (b) no permits are
  available.

The permit is an `OwnedSemaphorePermit` — it lives inside the `ReceivedWork` struct and
is held for the full lifetime of the spawned task (`processor/src/lib.rs:164`). When the
task finishes, `DropOnFinish` drops the permit, releasing the slot for the next item.

### Three work kinds

`anchor/processor/src/work.rs:11`:

```rust
pub type AsyncFn = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type BlockingFn = Box<dyn FnOnce() + Send>;
pub type ImmediateFn = Box<dyn FnOnce(DropOnFinish) + Send>;

pub(crate) enum WorkKind {
    Async(AsyncFn),       // spawned on the Tokio runtime
    Blocking(BlockingFn), // spawned via spawn_blocking (CPU work: signing, verifying)
    Immediate(ImmediateFn), // run inline in the processor loop — must NEVER block
}
```

The dispatch loop (`processor/src/lib.rs:124`) pulls the highest-priority ready item,
checks expiry, then routes it. The `DropOnFinish` guard handed to immediate/async/
blocking work is what releases the permit and stops the timing metric — *holding it
longer keeps the permit*, which is how a synchronous "kick a QBFT instance" immediate
task can carry its accounting into the spawned instance.

**Why this matters to you as a maintainer.** When you see code like
`self.processor.urgent_consensus.send_blocking(move || { ... share.sign(root) ... })`
that's a deliberate choice: BLS signing is CPU-bound, so it goes to `spawn_blocking` on
the urgent queue. When you see `permitless.send_immediate(...)` it's a fast,
non-blocking hand-off (e.g. forwarding a partial signature to its collector). If a duty
is mysteriously delayed under load, the processor queue depth metrics
(`ANCHOR_PROCESSOR_QUEUE_LENGTH`, `ANCHOR_PROCESSOR_WORK_EVENTS_*`) are your first
instrument.

---

## 6. Learning your responsibilities: the Eth event syncer

An operator doesn't choose its validators in a config file; it **learns** them by
reading events from the SSV smart contract on the Ethereum **execution layer (EL)**.
This is the job of `eth::SsvEventSyncer`, started at `client/src/lib.rs:461`.

### The syncer struct and the two endpoints

`anchor/eth/src/sync.rs:127`:

```rust
pub struct SsvEventSyncer {
    rpc_client: RootProvider,    // HTTP: historical log fetching, block metadata
    ws_client: RootProvider,     // WebSocket: live block stream
    ws_url: String,
    event_processor: EventProcessor,
    network: SsvNetworkConfig,
    is_synced: watch::Sender<bool>,
}
```

`sync()` (`sync.rs:250`) loops forever: a **historical** pass over HTTP from the
contract's deployment block up to `HEAD − FOLLOW_DISTANCE` (8 blocks, for reorg
safety), batched 10k blocks at a time and fetched up to 50 batches concurrently; then a
**live** pass driven by the WebSocket block stream. Between the two it flips the
`is_synced` watch to `true` (`sync.rs:354`) — the very signal `Client::run` blocks on
before starting duty services.

### The contract events Anchor cares about

`anchor/eth/src/generated.rs:4` declares the Solidity ABI via `alloy::sol!`:

```rust
event OperatorAdded(uint64 indexed operatorId, address indexed owner, bytes publicKey, uint256 fee);
event OperatorRemoved(uint64 indexed operatorId);
event ValidatorAdded(address indexed owner, uint64[] operatorIds, bytes publicKey, bytes shares, Cluster cluster);
event ValidatorExited(address indexed owner, uint64[] operatorIds, bytes publicKey);
event ValidatorRemoved(address indexed owner, uint64[] operatorIds, bytes publicKey, Cluster cluster);
event FeeRecipientAddressUpdated(address indexed owner, address recipientAddress);
event ClusterLiquidated(address indexed owner, uint64[] operatorIds, Cluster cluster);
event ClusterReactivated(address indexed owner, uint64[] operatorIds, Cluster cluster);
```

The dispatch on `topic0` is at `event_processor.rs:192` — each event maps to a
`process_*` handler.

### The hot path: `ValidatorAdded` → a row in your database

This is the single most important event for an operator. `event_processor.rs:459`,
`process_validator_added`, in essence:

1. Bump the owner's nonce (`bump_and_get_nonce_tx`).
2. Parse the validator pubkey and compute the `ClusterId` from `owner + operatorIds`.
3. **Parse the shares blob** (`util.rs:28`, `parse_shares`) — the `shares` bytes are
   laid out as `[BLS sig (96)] [share pubkey × N (48 each)] [encrypted key × N (256
   each)]`. One `Share` is produced per operator in the set.
4. Verify the registration signature against the bumped nonce.
5. Build the `Cluster` + `ValidatorMetadata`, register the validator in the **slashing
   protection** DB, then insert everything in one transaction.

The "am I in this cluster?" determination happens during insertion,
`cluster_operations.rs:42`:

```rust
let own_id = self.get_own_operator_id_tx(tx)?;
shares.iter().try_for_each(|share| {
    // Check if any of these shares belong to us, meaning we are a member in the cluster
    if own_id == Some(OperatorId(*share.operator_id)) {
        our_share = Some(share.to_owned());
    }
    tx.prepare_cached(sql_operations::INSERT_CLUSTER_MEMBER)?
        .execute(params![*share.cluster_id, share.operator_id])?;
    self.insert_share(tx, share, &validator.public_key)
})?;
state_updates.insert_validator(cluster, validator.to_owned(), our_share);
```

> **Key invariant** (called out in `database/src/lib.rs`): the in-memory `shares` map
> holds **only your own** shares. So "do I co-run validator X?" is answered by a single
> `state.shares().get_by(&validator_pubkey)` lookup returning `Some` — you'll see
> exactly this check in the message receiver (§9) to decide whether an inbound message
> is even relevant to you.

After the transaction commits, the validator pubkey is queued for index resolution
(`PostCommitAction::IndexSync`).

### `OwnOperatorId`: who am I?

You start the node before you necessarily know your own `OperatorId` (it's assigned
on-chain by `OperatorAdded`). `OwnOperatorId` (`database/src/lib.rs:354`) handles the
"maybe not known yet, cache once known" pattern:

```rust
pub enum OwnOperatorId {
    Known(OperatorId),
    FromState { receiver: Receiver<NetworkState>, id: OnceCell<OperatorId> },
}
```

It's created at `client/src/lib.rs:492` from `database.watch()` and threaded into the
message sender, QBFT manager, and signature collector. Each calls `.get()` only when it
actually needs the ID.

### Index sync and voluntary exits

- **Index sync** (`eth/src/index_sync.rs:33`, `start_validator_index_syncer`) batches
  up to 512 pubkeys (queued from `ValidatorAdded`, plus a rolling sweep of any DB rows
  with `index: None`), calls the beacon node's
  `POST /eth/v1/beacon/states/head/validators`, and writes the resolved indices back.
- **Voluntary exits** (`eth/src/voluntary_exit_processor.rs:34`,
  `start_exit_processor`): a `ValidatorExited` event (live only) becomes an
  `ExitRequest`, scheduled a few slots out, then at the target slot the exit is signed
  via threshold signatures (`collect_voluntary_exit_partial_signatures`, §12) and POSTed
  to the beacon node's voluntary-exit pool.

---

## 7. Discovering duties: the duties tracker

Knowing *which* validators you co-run (from §6) is separate from knowing *when* they
have duties. Two sources cooperate:

1. **Lighthouse's `DutiesService`** (`validator_services` crate, built at
   `client/src/lib.rs:668`) handles attester and sync-committee *subscription* duties
   the way a normal validator client does — it's the same machinery Lighthouse uses.
2. **Anchor's `DutiesTracker`** (`duties_tracker/src/duties_tracker.rs:30`) tracks the
   pieces Anchor itself needs to reason about: **block proposers** and **sync-committee
   membership** for the validators we run.

```rust
pub struct DutiesTracker<T: SlotClock + 'static> {
    duties: Duties,
    voluntary_exit_tracker: Arc<VoluntaryExitTracker>,
    beacon_nodes: Arc<BeaconNodeFallback<T>>,
    spec: Arc<ChainSpec>,
    slots_per_epoch: u64,
    slot_clock: T,
    network_state_rx: watch::Receiver<NetworkState>,   // ← the DB view from §6
}
```

It runs two polling loops (`duties_tracker.rs:259`, `start`), each waking once per slot:
`poll_beacon_proposers` and `poll_sync_committee_duties`. The pattern in both is the
same and worth noting: fetch from the beacon node, then **filter to validators we
actually run** using the DB view. From `poll_beacon_proposers` (`duties_tracker.rs:206`):

```rust
let validator_indices = {
    let network_state = self.network_state_rx.borrow();
    network_state.validator_indices()        // our validators only
};
let relevant_duties = response.data.into_iter()
    .filter(|proposer_duty| validator_indices.contains(&proposer_duty.validator_index))
    .collect::<Vec<_>>();
self.duties.proposers.write().insert(current_epoch, relevant_duties);
```

The tracked duties (`duties_tracker/src/lib.rs:89`) feed the **message validator**
(§9) — it consults `DutiesProvider` to decide whether an inbound consensus message
corresponds to a real duty (otherwise it's spam to ignore).

`VoluntaryExitTracker` (`voluntary_exit_tracker.rs:16`) is a small companion that
schedules our validators' exits per target slot and tracks per-slot duty counts (used
for rate-limiting hooks).

---

## 8. Network topology: subnets and gossipsub topics

### The problem this section solves

SSV operators talk to each other over a peer-to-peer network. They use **libp2p
gossipsub**, a protocol where a message you publish is forwarded peer-to-peer until
everyone interested has seen it (like spreading a rumor through a crowd). This is a
*separate* network from Ethereum's own beacon-chain gossip network — it carries only
SSV consensus traffic (QBFT votes, partial signatures).

The challenge: there are thousands of clusters on the network, all gossiping at once. If
every operator had to listen to *all* of that traffic, each node would be flooded with
messages it doesn't care about — most of it is consensus for validators that operator
has nothing to do with. We need a way for an operator to hear **only** the traffic
relevant to its own clusters.

The solution is **topic-based filtering**. Gossipsub lets you split traffic into named
channels called **topics**. You only receive messages on topics you've *subscribed* to.
So if each cluster's traffic goes onto a predictable topic, an operator can subscribe to
just the handful of topics its clusters use and ignore the rest.

### Subnets: a fixed number of "buckets"

If every cluster had its own topic, there could be tens of thousands of topics — too
many to manage, and most would carry almost no traffic. Instead, SSV defines a fixed
number of **subnets** — 128 of them — and deterministically assigns each cluster to one.
A subnet is just a bucket; many clusters share the same subnet, and each subnet maps to
exactly one gossipsub topic.

```rust
// anchor/subnet_service/src/subnet.rs:16
pub const SUBNET_COUNT: usize = 128;
```

Because the assignment is deterministic (a pure function of the cluster's operator set),
**every** operator computes the same subnet for a given cluster without coordinating.
The sender knows which subnet to publish on, and the interested receivers know which
subnets to listen on — they arrive at the same answer independently. The trade-off of
having only 128 buckets is that you also receive traffic from *other* clusters that
happen to land in the same bucket, but that's a small, bounded amount of noise compared
to listening to everything.

### Committee → subnet: how a cluster is assigned to a bucket

Recall from §4 that a **committee** is identified by its operator set (the `CommitteeId`
is the hash of the sorted operator IDs — it does **not** depend on the owner, so every
cluster run by the same set of operators shares one committee and therefore one subnet).
The subnet calculation turns that operator set into a number in `0..128`. There are two
algorithms, and which one is used depends on the active network fork (§14):

**Alan (the legacy algorithm)** — `from_committee_alan` at `subnet.rs:61`:

```rust
// Interpret the 32-byte committee id as one big number, then take it modulo 128.
let id = U256::from_be_bytes(*committee_id);
SubnetId((id % U256::from(subnet_count)) ...)
```

Simply: `subnet = committee_id % 128`. Spreads committees roughly evenly across the 128
buckets, but pays no attention to *which operator* is in which committee.

**Boole (the newer algorithm)** — `from_operators` at `subnet.rs:89`. This uses a
technique called **MinHash**:

```rust
// Hash each operator id on its own, then keep the single smallest hash.
let min_hash = operator_ids
    .iter()
    .map(|operator_id| Sha256::digest(operator_id.to_le_bytes()))
    .min()
    .ok_or(SubnetCalculationError::EmptyOperatorList)?;
// The subnet is that minimum hash, modulo 128.
let subnet_id = U256::from_be_bytes(min_hash) % subnet_count;
```

Step by step: hash every operator ID in the committee separately, find the **smallest**
of those hashes, and use it (mod 128) as the subnet.

Why bother? The goal is to **reduce the number of distinct subnets a busy operator has
to monitor.** Consider an operator that participates in many different committees (a
common situation — see §4). Under Alan, each of those committees has a different
`CommitteeId`, so each lands in a more-or-less random bucket; the operator ends up having
to subscribe to many subnets. Under Boole's MinHash, whenever that operator happens to
own the smallest hash in a committee, *all* such committees collapse onto the **same**
subnet (because they share that same minimum). So a heavily-used operator tends to
concentrate its committees into fewer subnets, which means fewer subscriptions and less
peer-management overhead. The result is order-independent (sorting the operators doesn't
change the minimum) and stable (the same operator set always yields the same subnet).

### Subnet → topic string: naming the channel

A subnet is just a number; the gossipsub topic is a **string**. The mapping is a
prefix plus the subnet number (`topic.rs:16`, `create_topic`):

- **Alan** topic: `ssv.v2.<subnet>`           e.g. `ssv.v2.42`
- **Boole** topic: `/ssv/<network>/boole/<subnet>`  e.g. `/ssv/mainnet/boole/42`

The fork determines the *prefix*, so the same subnet number is a different topic string
before and after a fork — which is exactly what keeps pre-fork and post-fork traffic on
separate channels during a transition.

### The subnet service: keeping subscriptions in sync

Knowing *how* to compute a subnet is only half the job. Something has to continuously
answer the question **"given the clusters I'm currently part of, which topics should I be
subscribed to right now?"** — and update those subscriptions whenever the answer changes.
That is the **subnet service**.

`start_subnet_service` (`service.rs:149`) spawns a long-lived background task. Its core
is a loop that waits on three independent triggers (`subscriptions.rs:99`):

```rust
tokio::select! {
    // 1. Our cluster membership changed (a ValidatorAdded/Removed event hit the DB).
    _ = db.changed(), if !self.subscribe_all_subnets => {
        self.handle_subnet_changes::<E>(&mut service_state).await;
    }
    // 2. An epoch boundary passed — refresh gossipsub scoring rates.
    _ = sleep(delay), if !self.disable_gossipsub_topic_scoring => {
        self.send_scoring_rate_updates::<E>(&service_state).await;
    }
    // 3. The network is transitioning between forks (e.g. Alan → Boole).
    Ok(()) = lifecycle_rx.changed() => {
        self.on_lifecycle_transition(new_lifecycle, &mut service_state).await;
        self.handle_subnet_changes::<E>(&mut service_state).await;
    }
}
```

**Trigger 1 — cluster membership changed.** When the on-chain event syncer (§6) records
that we've joined or left a cluster, the in-memory `NetworkState` changes and the
`db.changed()` branch fires. The service recomputes the full set of subnets it *should*
be subscribed to — by taking every cluster it belongs to, extracting each one's operator
set, and running the fork's subnet algorithm (`subscriptions.rs:175`,
`handle_subnet_changes`). It then **diffs** that target set against what it's currently
subscribed to:

```rust
// subscriptions.rs:202 — only act on the difference, not the whole set.
let to_leave = currently_subscribed.difference(&target_subnets);
let to_join  = target_subnets.difference(&currently_subscribed);
self.send_unsubscribes(...to_leave...).await?;
self.send_subscribes(...to_join...).await?;
```

This diff is why the service only emits an event when something *actually* changed —
joining one new cluster typically produces a single `Subscribe`, not 128 of them.

**Trigger 2 — epoch boundary (scoring).** Gossipsub defends itself against misbehaving
peers by *scoring* them: a peer that floods a topic, or one that goes suspiciously quiet,
loses score and is eventually ignored. To judge "too much" or "too little," gossipsub
needs to know the **expected** message rate for each topic. That rate depends on how many
validators are active on the subnet, which changes as committees and sync-committee
memberships rotate. So once per epoch the service recalculates each subscribed topic's
expected rate (`scoring.rs:21`, `send_scoring_rate_updates`) and pushes it down as a
`RateUpdate`. (Both this and the per-subscription rate can be turned off with
`disable_gossipsub_topic_scoring`.)

**Trigger 3 — fork transition.** When the network is approaching a fork, the topic
*prefix* is about to change (`ssv.v2.*` → `/ssv/<net>/boole/*`), so for a window of time
the node must listen on **both** the old and new topics to avoid missing messages. The
service tracks a small state machine of fork "lifecycle" phases (`subscriptions.rs:123`,
`on_lifecycle_transition`):

- **WarmUp** (fork is coming): subscribe to the upcoming fork's topics *in addition to*
  the current ones.
- **GracePeriod** (fork just happened): keep the previous fork's topics subscribed a
  while longer so late/in-flight messages still arrive.
- **Normal** (settled): unsubscribe from the now-defunct fork's topics.

This dual-subscription dance is what lets a node cross a fork boundary without dropping
consensus messages that were published just before or after the switch.

### What the service emits, and who acts on it

The subnet service never touches libp2p directly. It only computes *decisions* and sends
them as `TopicEvent`s (`subnet.rs:148`) over a channel to the network layer:

- `Subscribe { topic, subnet, message_rate }`
- `Unsubscribe { topic, subnet }`
- `RateUpdate { topic, message_rate }`

Note the events carry the **full topic string** — the network layer never has to know
about forks or naming schemes; it just subscribes to whatever string it's handed. The
network's `on_topic_event` (`network/src/network.rs:575`) is the consumer: on
`Subscribe` it calls `gossipsub().subscribe(topic)`, applies the scoring parameters, and
(on the *first* subscription to a subnet) tells the peer manager to go find peers on that
subnet; on `Unsubscribe` it does the reverse, leaving the subnet only once the last
subscription referencing it is gone. This split keeps the **policy** (which subnets, fork
rules, scoring math) in the subnet service and the **mechanism** (libp2p calls, peer
connections) in the network layer.

### Slot-based routing (SIP-43)

One subtlety ties this together. During a fork transition, which prefix should a given
message use — old or new? The rule (SIP-43) is that the **message's own slot** decides,
not the wall-clock moment of sending. A consensus message *for a pre-fork slot* is
published on the pre-fork topic even if it goes out slightly after the fork has
technically begun. The subnet service exposes slot-aware lookups for exactly this
(`service.rs:95`, `subnet_for_committee_with_operators_at_slot`), and the message sender
uses them so routing always matches the fork rules that were in effect for the slot the
message belongs to.

The complete end-to-end chain
`validator pubkey → cluster → committee → subnet → topic` is walked through in §13.

---

## 9. The message pipeline: ingress and egress

### The Network run loop

`anchor/network/src/network.rs:70` defines `Network`, and `:179` is the loop you'll
return to often:

```rust
pub async fn run<E: EthSpec>(mut self) {
    loop {
        tokio::select! {
            swarm_message = self.swarm.select_next_some() => self.handle_swarm_event(swarm_message),
            Some(event) = self.topic_event_receiver.recv() => self.on_topic_event::<E>(event),
            event = self.message_rx.recv() => { /* outbound: (topic, bytes) → publish */ }
            event = self.outcome_rx.recv() => { /* report gossip validation verdict */ }
            Ok(()) = self.lifecycle_rx.changed() => { /* fork transition → ENR domain */ }
        }
    }
}
```

Four channels connect it to the rest of the client (all created in `client/src/lib.rs`):
`topic_event_receiver` from the subnet service; `message_rx` carrying outbound
`(topic_string, message_bytes)`; `outcome_rx` carrying the **gossip validation verdict**
that this node must report back to gossipsub; and `lifecycle_rx` for fork transitions.

### Ingress: from gossip bytes to a QBFT instance

An inbound gossip message lands in `handle_gossipsub_message` (`network.rs:308`), which
parses the topic (rejecting unparseable ones) and forwards to the `MessageReceiver`.
The receiver trait (`message_receiver/src/lib.rs`) and its `Outcome` are small:

```rust
pub trait MessageReceiver {
    fn receive(&self, propagation_source: PeerId, message_id: MessageId,
               message: Message, topic_context: TopicContext) -> Result<(), Error>;
}

pub struct Outcome {           // manager.rs:24
    pub message_id: MessageId,
    pub propagation_source: PeerId,
    pub action: MessageAcceptance,  // gossipsub Accept / Reject / Ignore
}
```

The real work is `NetworkMessageReceiver::receive` (`message_receiver/src/manager.rs:70`).
This is the **ingress hot path** — read it in full once; here is its spine:

```rust
self.processor.urgent_consensus.send_blocking(move || {
    // 1. Full validation (signature, topic/domain, duty, dedup, rate, round) — §below
    let result = receiver.validator.validate(&message.data, &topic_context);
    let mut action = MessageAcceptance::from(&result);

    // 2. Don't punish peers while we're still syncing history.
    if let MessageAcceptance::Reject = action && !*receiver.is_synced.borrow() {
        action = MessageAcceptance::Ignore;
    }
    // 3. Report the verdict back to gossipsub via outcome_tx → network run loop.
    receiver.outcome_tx.try_send(Outcome { message_id, propagation_source, action })?;

    // 4. If valid, is it even ours? (committee membership / share ownership)
    let ValidatedMessage { signed_ssv_message, ssv_message } = match result { ... };
    match msg_id.duty_executor() {
        Some(DutyExecutor::Validator(validator)) =>
            if state.shares().get_by(&validator).is_none() { return; }  // not our validator
        Some(DutyExecutor::Committee(committee_id)) =>
            if !committee_contains_our_operator_id { return; }          // not our committee
        None => return,
    }

    // 5. Doppelgänger guard during the startup monitoring window.
    // 6. Route by payload type:
    match ssv_message {
        ValidatedSSVMessage::QbftMessage(m) =>
            receiver.qbft_manager.receive_data(signed_ssv_message, m),
        ValidatedSSVMessage::PartialSignatureMessages(m) =>
            receiver.signature_collector.receive_partial_signatures(m),
    }
}, RECEIVER_NAME);
```

Note the two-stage relevance check: **validation decides the gossip verdict** (do we
propagate this to peers?), then a cheap **ownership check** decides whether *we* process
it. A message can be perfectly valid (so we Accept and relay it) yet irrelevant to us
(so we don't spin up an instance).

### The validation rules

`message_validator/src/lib.rs:410`, `validate`, first tries to SSZ-decode the
`SignedSSVMessage`; failure → `PreDecodeFailure`. Then `validate_decoded_message`
(`:427`) runs, in order:

1. **Structural** (`signed_ssv_message.validate()`): RSA signature count/size, signers
   present, sorted, non-duplicated, non-zero.
2. **Role & committee resolution**: get the `Role` from the `MessageId`; resolve the
   committee info (by `CommitteeId` for committee roles, by validator pubkey otherwise).
   Unknown validator/committee → fail.
3. **Topic & domain** (`validate_topic_and_domain`, `:591`): the message must be on the
   correct subnet for its committee, its domain must match the fork active at the
   message's slot, and the topic's fork must match (SIP-43 consistency).
4. **Per-type semantics**: `validate_ssv_message` (`:681`) dispatches to consensus or
   partial-signature validation.

The consensus rules (`message_validator/src/consensus_message.rs`) include the QBFT
sanity checks: RSA signatures verify against the operators' keys (`:58`); a *decided*
message (multi-signer Commit) needs ≥ quorum signers (`:94`); `round != 0` (`:117`);
`round ≤ max_round` (`:140`); the proposer of a Proposal must be the **round-robin
leader** (`validate_qbft_logic`, `:223`, `:247`); and dedup/anti-regression: a signer
may not *decrease* its round (`RoundAlreadyAdvanced`, `:272`) nor send conflicting
proposal data for the same round (`DifferentProposalData`, `:287`).

The verdict mapping is deliberate (`message_validator/src/lib.rs:227`): "this message
is stale/irrelevant" failures (wrong domain, late slot, round already advanced, unknown
validator, …) map to **`Ignore`** (don't penalize the peer), while malformed/forged
messages fall through to **`Reject`** (penalize). Combined with the
"`Reject`→`Ignore` while not synced" rule in the receiver, this is how Anchor avoids
banning honest peers during cold start.

### Egress: signing and publishing an outbound SSV message

The `MessageSender` trait (`message_sender/src/lib.rs:15`):

```rust
pub trait MessageSender: Send + Sync {
    fn sign_and_send(&self, message: UnsignedSSVMessage, committee_id: CommitteeId,
                     additional_message_callback: Option<Box<MessageCallback>>) -> Result<(), Error>;
    fn send(&self, message: SignedSSVMessage, committee_id: CommitteeId) -> Result<(), Error>;
}
```

`NetworkMessageSender::sign_and_send` (`message_sender/src/network.rs:49`) gates on
"network open / operator id known / synced," then does the RSA signing on the urgent
queue (CPU work) and hands the signed message to `do_send`:

```rust
self.processor.urgent_consensus.send_blocking(move || {
    let signature = sender.sign(&message)?;             // RSA sign with operator key
    let message = SignedSSVMessage::new(vec![signature], vec![operator_id],
                                        message.ssv_message, message.full_data)?;
    if let Some(callback) = additional_message_callback { callback(&message); }
    sender.do_send(message, committee_id);              // → subnet/topic → network_tx
}, SIGNER_NAME);
```

`do_send` extracts the **slot** from the message, asks the subnet service for the
subnet/topic *at that slot* (SIP-43), and pushes `(topic, bytes)` onto `network_tx` —
the same channel the Network run loop drains. (`ImpostorMessageSender`,
`message_sender/src/impostor.rs`, is the test/observer variant that skips real signing.)

---

## 10. QBFT: agreeing on what to sign

QBFT (a variant of IBFT/PBFT used by go-ssv) is how a committee agrees on the exact
bytes everyone will sign. The pure protocol engine lives in `anchor/common/qbft/`; the
orchestration that maps duties to instances lives in `anchor/qbft_manager/`.

### The state machine

`anchor/common/qbft/src/qbft_types.rs:126`:

```rust
pub enum InstanceState {
    AwaitingProposal,                 // waiting for the round leader to propose
    Prepare { proposal_root: Hash256 },
    Commit { proposal_root: Hash256 },
    SentRoundChange,                  // we gave up on this round
    Complete,
    RoundChangeConsensus,
}
```

Four message types drive transitions (`consensus.rs:146`): **Proposal → Prepare →
Commit**, with **RoundChange** as the liveness escape hatch. The instance itself is
`Qbft<F, D, S>` (`qbft/src/lib.rs:103`), holding the config, the `MessageId`, the
`InstanceHeight` (≈ slot), the four message containers, the current round/state, and a
`completed: Option<Completed<D::Hash>>`.

### The receive hot path

`qbft/src/lib.rs:539`, `receive` — validate, then dispatch by type:

```rust
pub fn receive(&mut self, wrapped_msg: WrappedQbftMessage) -> Result<(), QbftError> {
    let (validated_msg, signer) = self.validate_message(&wrapped_msg)?;
    let msg_round: Round = wrapped_msg.qbft_message.round.into();
    match wrapped_msg.qbft_message.qbft_message_type {
        QbftMessageType::Proposal => { /* received_propose */ }
        QbftMessageType::Prepare  => self.received_prepare(signer, msg_round, wrapped_msg)?,
        QbftMessageType::Commit => {
            if wrapped_msg.signed_message.operator_ids().len() == 1 {
                self.received_commit(signer, msg_round, wrapped_msg)?   // single-signer commit
            } else {
                self.received_decided(wrapped_msg)?                     // aggregated/decided
            }
        }
        QbftMessageType::RoundChange => self.received_round_change(signer, msg_round, wrapped_msg)?,
    }
    Ok(())
}
```

**Quorum** is computed per-value: count signed messages per `root` and check against
`quorum_size` (`msg_container.rs:56`). On a Prepare quorum the instance moves to
`Commit` and broadcasts its own Commit (`received_prepare`, `qbft/src/lib.rs:852`); on a
Commit quorum it aggregates the commits and marks `Completed::Success(root)`
(`received_commit`, `:939`). A `Completed` is (`qbft_types.rs:200`):

```rust
pub enum Completed<D> {
    TimedOut,
    Success(D),  // the agreed value's hash; the data itself is looked up in `self.data`
}
```

### Leader selection (round-robin, deterministic)

`qbft_types.rs:55`:

```rust
let index = (round.get() - Round::default().get() + height + eth_epoch) % sorted_committee.len();
*operator_id == *sorted_committee.get_index(index)...
```

The leader for a round is `(round−1 + height + epoch_shift) % N` over the *sorted*
committee. The `epoch_shift` term is enabled post-Boole so leadership also rotates by
epoch. This is exactly the rule the message validator enforces when it checks
`SignerNotLeader`.

### Timeouts and round change

`qbft_manager/src/timeout.rs:6`:

```rust
const QUICK_TIMEOUT_THRESHOLD: u64 = 8; // Round 8
const QUICK_TIMEOUT: u64 = 2;           // 2 Seconds
const SLOW_TIMEOUT: u64 = 120;          // 2 Minutes
```

Rounds 1–8 use 2s timeouts; 9+ use 2 minutes. `calculate_round_timeout` (`:24`) applies
them either **cumulatively from `instance_start_time`** (`SlotTime`, attestations) or
**per-round from the round's start** (`Relative`, proposals). When a timer fires the
instance calls `end_round` (`qbft/src/lib.rs:1224`): bump the round (capped by
`max_rounds`), set `SentRoundChange`, broadcast a RoundChange. Receiving **f+1**
RoundChanges for a higher round makes you jump to it; a **quorum** of RoundChanges lets
the new round begin (`received_round_change`, `:1079`).

### How a duty becomes an instance

`QbftManager` (`qbft_manager/src/lib.rs:132`) keeps three maps of running instances,
keyed by duty type:

```rust
proposer_consensus_data_instances: Map<ProposerInstanceId, ProposerConsensusData>,
beacon_vote_instances:             Map<CommitteeInstanceId, BeaconVote>,
aggregator_committee_instances:    Map<AggregatorCommitteeInstanceId, AggregatorCommitteeConsensusData<E>>,
```

The instance identifiers (`:64`) are `(committee, instance_height)` for committee duties
and `(validator, duty_kind, instance_height)` for per-validator duties, where
`instance_height` is the slot. Instances are **spawned lazily on first message**
(`get_or_spawn_instance`, `:425`): the manager opens an mpsc channel, stores the sender,
and submits the instance's async loop to the processor's `permitless` queue. The loop
itself (`qbft_manager/src/instance.rs:280`, `qbft_instance`) owns a `QbftInstance` state
(`Uninitialized → Initialized → Decided`), drives the timer, and feeds network messages
into `Qbft::receive`. When the duty side calls `decide_instance` (used throughout the
validator store), it both *starts* the instance with the local proposal value and
*awaits* the `Completed`.

> **Why lazy spawn matters:** a Proposal can arrive from a peer *before* your local duty
> logic asks to decide that slot (clock skew, you're not the leader, etc.). Spawning on
> first message — local *or* remote — means the instance exists to receive that early
> Proposal instead of dropping it.

---

## 11. Threshold signatures: the signature collector

Once QBFT has fixed *what* to sign, every operator signs the agreed `signing_root` with
its BLS key share, broadcasts a **partial signature**, and someone reconstructs the full
signature once `2f+1` partials arrive. That's `SignatureCollectorManager`
(`anchor/signature_collector/src/lib.rs:75`):

```rust
pub struct SignatureCollectorManager<S: SlotClock> {
    processor: Senders,
    operator_id: OwnOperatorId,
    fork_schedule: Arc<ForkSchedule>,
    slot_clock: S,
    slots_per_epoch: u64,
    message_sender: Arc<dyn MessageSender>,
    // per (signing_root, validator) collection in progress:
    signature_collectors: DashMap<(Hash256, ValidatorIndex), SignatureCollector>,
    // committee-batched local partials before broadcast:
    committee_partial_signature_batches: DashMap<(Hash256, CommitteeId), CommitteePartialSignatureBatch>,
}
```

### Producing your own partial and broadcasting it

`sign_and_collect` (`lib.rs:136`) is the public entry the validator store calls. It (a)
registers a notifier so the caller's future resolves when the full signature is ready,
then (b) signs locally on the urgent queue (`lib.rs:178`):

```rust
self.processor.urgent_consensus.send_blocking(move || {
    let partial_signature = if let Some(share) = &validator_signing_data.share {
        share.sign(validator_signing_data.root)   // BLS sign with our key share
    } else { Signature::empty() };                  // impostor mode
    let message = PartialSignatureMessage {
        partial_signature, signing_root: validator_signing_data.root,
        signer, validator_index: validator_signing_data.index,
    };
    // broadcast (single-validator → send immediately; committee → batch then send)
    // and feed our own partial back into the local collector
    manager.receive_partial_signature(message, metadata.slot);
}, SIGNER_NAME);
```

For **single-validator** duties the partial is published immediately
(`DutyExecutor::Validator`). For **committee** duties the partials are buffered per
`(base_hash, committee_id)` until one-per-validator is collected, then sent as a single
committee message (`lib.rs:221`) — fewer, fatter messages on the wire.

### Ingesting partials and reconstructing

Inbound partials (yours and peers') funnel into a per-collection actor
(`signature_collector`, `lib.rs:576`), which stores one signature per operator
(`add_partial_signature`, `:651`, with a loud `error!` if an operator ever sends two
*different* signatures for the same root — that's misbehavior) and then attempts
reconstruction (`try_reconstruct`, `:679`):

```rust
fn try_reconstruct(&mut self) -> ControlFlow<()> {
    let Some(threshold) = self.threshold else { return ControlFlow::Continue(()); };
    if (self.signature_share.len() as u64) < threshold { return ControlFlow::Continue(()); }
    let signature = match combine_signatures(mem::take(&mut self.signature_share)) {
        Ok(signature) => Arc::new(signature),
        Err(err) => { error!(?err, "Failed to recover signature"); return ControlFlow::Break(()); }
    };
    for notifier in mem::take(&mut self.notifiers) { let _ = notifier.send(Arc::clone(&signature)); }
    self.full_signature = Some(signature);
    ControlFlow::Continue(())
}
```

`combine_signatures` (`lib.rs:707`) maps each `OperatorId` to a `KeyId` and calls
`bls_lagrange::combine_signatures` — Lagrange interpolation over the BLS signature group
(`common/bls_lagrange/src/blst.rs:121`). The math: each operator's share is a point on a
secret polynomial; the full signature is the value at x=0, recovered as
`S = Σ λ_i · S_i` where `λ_i = Π_{j≠i} x_j/(x_j − x_i)`. The blst implementation does
this efficiently by precomputing a shared numerator. The threshold is the same `2f+1`
from §3.

**Maintainer note:** if signatures "never complete," the suspects are: not enough
operators online (`< threshold` partials), a `BeaconVote` mismatch (operators signed
*different* roots because QBFT didn't actually converge), or wrong `KeyId`/`OperatorId`
mapping. The `error!("Failed to recover signature")` and the conflicting-signature
`error!` are the log lines to grep for.

---

## 12. The validator store: every duty, walked through

`AnchorValidatorStore` (`anchor/validator_store/src/lib.rs:161`) is the **bridge**
between Lighthouse's validator-duty services and Anchor's SSV machinery. Lighthouse's
services call its `ValidatorStore` trait methods exactly as they would call a normal
local-keystore signer; Anchor's implementation transparently runs QBFT + threshold
signing underneath.

```rust
pub struct AnchorValidatorStore<T: SlotClock, E: EthSpec, C: ConsensusDecider<E> = QbftManager<E, T>> {
    database: Arc<NetworkDatabase>,
    decrypted_keys: Mutex<LruCache<[u8; ENCRYPTED_KEY_LENGTH], SecretKey>>,
    signature_collector: Box<dyn SignatureCollecting>,
    consensus: Arc<C>,                       // the QBFT manager
    slashing_protection: Arc<SlashingDatabase>,
    slot_clock: T,
    spec: Arc<ChainSpec>,
    genesis_validators_root: Hash256,
    private_key: Option<Rsa<Private>>,
    fork_schedule: Arc<ForkSchedule>,
    // watch channels feeding per-slot context to duty signing:
    voting_context_tx: watch::Sender<Option<Arc<VotingContext>>>,
    voting_assignments_tx: watch::Sender<Option<Arc<VotingAssignments>>>,
    aggregation_assignments_tx: watch::Sender<Option<Arc<AggregationAssignments<E>>>>,
    // ... gas_limit, builder prefs, is_synced, task_executor
}
```

### The fundamental split: consensus duties vs. signature-only duties

This is the single most important distinction in the validator store:

| Needs QBFT consensus first (everyone must sign the **same** data) | Signature-only (threshold sign a locally-known value) |
|---|---|
| **Attestation** (`sign_attestations` → `sign_committee_attestations`) | **Randao reveal** (`randao_reveal`) |
| **Block proposal** (`sign_block` → `decide_abstract_block`) | **Selection proof** (`produce_selection_proof`) |
| **Sync committee message** (`sign_committee_sync_committee_signatures`) | **Sync selection proof** (`produce_sync_selection_proof`) |
| **Aggregate & proof** (`sign_committee_aggregate_and_proofs` / `sign_single_aggregate_and_proof`) | **Validator registration** (`sign_validator_registration_data`) |
| **Sync contribution** (`sign_committee_sync_committee_contributions` / single) | **Voluntary exit** (`collect_voluntary_exit_partial_signatures`) |

Why the split? Anything that votes on chain state — attestations, blocks, aggregations —
must be **byte-identical across operators** or the partial signatures won't combine and
slashing risk appears. So those go through QBFT first. Things like a randao reveal or a
selection proof are deterministic functions of (epoch/slot, key) — every operator
already knows the exact root to sign, so they skip consensus and go straight to
threshold signing.

### Attestation (the canonical consensus duty)

`sign_attestations` (`lib.rs:2997`) groups incoming attestations by SSV committee, then
for each committee runs `sign_committee_attestations` (`lib.rs:1464`). The heart of it:

```rust
let completed = self.consensus.decide_instance(
    CommitteeInstanceId { committee: committee_id, instance_height: slot.as_usize().into() },
    BeaconVote {                              // ← the data consensus is reached over
        block_root: first_att_data.beacon_block_root,
        source: first_att_data.source,
        target: first_att_data.target,
    },
    self.create_beacon_vote_validator(slot, validator_attestation_committees),
    timeout_mode,                             // SlotTime, instance_start = slot_start + slot/3
    &cluster.cluster_members,
).await?;
let data = match completed { Completed::Success(d) => d, Completed::TimedOut => return Err(Timeout) };
```

One QBFT instance decides one `BeaconVote` for **all** validators that committee runs
this slot. The decided vote is then stamped onto each validator's `AttestationData`, and
all the post-consensus partial signatures are collected in one batch
(`collect_prepared_signatures`, `lib.rs:329`). **Slashing protection** is checked *after*
signing, in a batch, on a blocking thread (`slashing_protection_attestations`,
`lib.rs:1744`) — slashable or already-signed attestations are dropped, the safe ones
returned to Lighthouse.

The single-validator collection path (`collect_signature`, `lib.rs:449`) is where the
key share is decrypted (LRU-cached) and `signature_collector.sign_and_collect` is
invoked:

```rust
let decrypted_key_share = self.decrypted_keys.lock()
    .try_get_or_insert(encrypted_private_key, || decrypt_key_share(...))?.clone();
let metadata = SignatureMetadata { kind: PostConsensus, role: Role::Committee,
                                   threshold: cluster.get_f()*2 + 1, slot, committee_id };
let collector = self.signature_collector.sign_and_collect(metadata, requester,
    ValidatorSigningData { root: signing_root, index: validator.index?, share: decrypted_key_share });
let signature = (*collector.await?).clone();
```

### Block proposal

`sign_block` (`lib.rs:2363`) is structurally similar but: it checks `is_synced` first,
runs consensus over `ProposerConsensusData` (the full or blinded block SSZ) via
`decide_abstract_block` (`lib.rs:524`) using `TimeoutMode::Relative`, checks **block**
slashing protection *before* signing (`check_and_insert_block_proposal`), then collects
the post-consensus signature. The randao reveal needed to build the block is a separate
signature-only call (`randao_reveal`, `lib.rs:2244`).

### Per-slot context: the metadata service

The validator store doesn't fetch attestation data or aggregates inline on the hot path
— that would be too slow and would risk operators fetching at slightly different times
and disagreeing. Instead `MetadataService` (`validator_store/src/metadata_service.rs`)
pre-stages everything on a 3-phase per-slot schedule and publishes it on the watch
channels the store reads:

- **Phase 1** (slot start): `VotingAssignments` — which of our validators attest / are
  in which sync subnets this slot.
- **Phase 2** (1/3 slot, or on a beacon **head event**): `VotingContext` — the
  `BeaconVote` (head block root, source, target) all attestation/sync QBFT instances
  will agree on. With `--with-weighted-attestation-data` it polls *all* beacon nodes and
  scores their heads to pick the best vote.
- **Phase 3** (2/3 slot): `AggregationAssignments` — which validators are aggregators,
  plus the pre-built `AggregatorCommitteeConsensusData` (post-Boole) that the aggregate
  QBFT instances decide over.

This is why the slot timeline in §2 lines up with the consensus instance start times:
the data is *ready* at exactly the offset the instances start.

### Registration & exits

`RegistrationService` (`validator_store/src/registration_service.rs`) periodically signs
builder/MEV `ValidatorRegistrationData`. A DVT subtlety lives here: registration
payloads contain a timestamp, and if operators signed at slightly different wall-clock
times their signatures would diverge — so Anchor **normalizes the timestamp to the epoch
start** (`lib.rs:2519`) so all operators sign identical bytes. Voluntary exits are
threshold-signed via `collect_voluntary_exit_partial_signatures` (`lib.rs:826`) and
submitted by the exit processor (§6).

---

## 13. End-to-end: an attestation through one slot

Putting it all together, here's the life of an attestation duty for a committee that
your node co-runs, slot `N`:

```
t=0   (slot N start)
  • MetadataService Phase 1 computes VotingAssignments: "validators V1,V2,V3 of
    committee C attest this slot" (metadata_service.rs).
  • Lighthouse's DutiesService already knows V1..V3 must attest at slot N.

t=1/3 slot (≈4s)  ── attestation deadline
  • MetadataService Phase 2 fixes the BeaconVote (head/source/target) from the beacon
    node (or weighted across nodes). Published on voting_context_tx.
  • Lighthouse's AttestationService calls validator_store.sign_attestations([V1,V2,V3]).
  • sign_committee_attestations groups them under CommitteeId(C) and calls
    qbft_manager.decide_instance(CommitteeInstanceId{C, height=N}, BeaconVote, ...).

  ── QBFT round 1 (qbft/src/lib.rs) ──
  • The round-1 leader (round-robin over sorted operators) broadcasts a Proposal of the
    BeaconVote. Message is RSA-signed (message_sender) and published to committee C's
    gossip topic (subnet = MinHash(ops)%128 post-Boole).
  • Peers' Network run loops receive it → MessageReceiver validates (leader correct?
    domain correct? duty real?) → routes to their QbftManager → Qbft::receive.
  • Each honest operator broadcasts Prepare. On a 2f+1 Prepare quorum, state→Commit and
    each broadcasts Commit. On a 2f+1 Commit quorum: Completed::Success(root).

  ── threshold signing (signature_collector) ──
  • Each operator signs the decided BeaconVote-derived signing_root with its BLS key
    share, batches one partial per validator, and broadcasts a committee
    PartialSignatureMessages on the same subnet.
  • Inbound partials route (MessageReceiver → signature_collector.receive_partial_
    signatures). When 2f+1 partials for a (root, validator) arrive, combine_signatures
    (Lagrange) reconstructs the full BLS signature.

  ── back to Lighthouse ──
  • collect_prepared_signatures resolves; slashing_protection_attestations batch-checks
    (drop slashable/duplicate); safe Attestations returned to AttestationService, which
    publishes them to the beacon node, which gossips them on the beacon network.

t=2/3 slot (≈8s)  ── aggregation deadline
  • If any of V1..V3 were selected aggregators (selection proof, signature-only), the
    aggregate runs its own QBFT instance (AggregatorCommittee post-Boole) + threshold
    signing, same shape.
```

Every arrow above is a function you can now find: the topic math (§8), the RSA sign +
publish (§9 egress), the validation gate (§9 ingress), `Qbft::receive` (§10), the
collector (§11), and the store methods (§12).

---

## 14. Forks: how behavior changes over time

SSV upgrades the protocol at **forks** (distinct from Ethereum's own forks). The
`Fork` enum (`anchor/common/fork/src/fork.rs:20`) is ordered chronologically:

```rust
pub enum Fork {
    Alan,  // subnet = committee_id % 128;          topic "ssv.v2.<subnet>"
    Boole, // subnet = min(SHA256(op)) % 128;        topic "/ssv/<network>/boole/<subnet>"
}
```

`ForkSchedule` (`fork/src/schedule.rs:86`) maps each fork to a `ForkConfig {fork, epoch,
domain_type}`. `active_fork(epoch)` (`:194`) returns the latest fork whose activation
epoch has passed. This one function gates behavior across the whole codebase:

- **Subnet & topic** computation (§8).
- **Message domain** in the `MessageId` and in validation (`validate_topic_and_domain`).
- **QBFT leader rotation** (the epoch-shift term, §10.3).
- **Aggregation strategy** in the validator store: pre-Boole aggregations/contributions
  are per-validator (`ProposerInstanceId`, `Role::Aggregator`); Boole+ batches them at
  the committee level (`AggregatorCommitteeInstanceId`, `Role::AggregatorCommittee`).
  You can see this branch repeatedly, e.g. `sign_aggregate_and_proofs` (`lib.rs:2563`):
  `if self.fork_schedule.active_fork(epoch) >= Fork::Boole { committee path } else { single path }`.

Around a fork there's a **preparation window** (1 epoch before — dual-subscribe to new
topics) and a **grace period** (32 slots after — keep old topics for late messages),
managed by the fork monitor (`fork::monitor::spawn`, started at `client/src/lib.rs:443`)
which feeds a `watch::Receiver<ForkLifecycle>` into the subnet service and network.

**Maintainer takeaway:** when adding a new fork, the touch points are: a new `Fork`
variant, its `ForkConfig` in the built-in network configs, any new subnet/topic rule,
and the `>= Fork::X` branches in the validator store. The `active_fork`/`active_fork_at_slot`
calls are your search anchors.

> Ethereum's **ePBS / Gloas** upgrade is being built on the `epbs` branch. Notably it does
> *not* follow this "add an SSV fork" recipe: an early attempt to add a `Fork::CStar` ordinal
> was reverted, and ePBS now gates directly on Ethereum's own `gloas_enabled()` boundary
> instead. §15 covers that decision and the duty-level changes ePBS forces.

---

## 15. ePBS / Gloas: Anchor's EIP-7732 support (in progress)

> **Branch note.** Everything in this section lives on the **`epbs`** branch, *not*
> `unstable`. It is **work in progress**: some is merged, some is open issues, and several
> design points are deliberately unresolved because the SSV-side spec (currently an *open*
> pull request, [ssvlabs/SIPs #94][sip-94], `sips/epbs_support.md`) and the upstream
> Ethereum spec ([EIP-7732][eip-7732], the **Gloas** CL fork) are both still moving. Line
> numbers reference `epbs` at HEAD `a7c0b84`; treat the quoted **symbol names** as the
> durable references. Where something is not yet decided it is flagged as an **open
> question** rather than stated as fact.

> **History note — `Fork::CStar` is gone.** An earlier draft of this section described an
> SSV-invented `Fork::CStar` ordinal as the ePBS gate. **PR #1090 removed it entirely.**
> ePBS is an *Ethereum* fork, not an SSV network fork, so Anchor now gates directly on
> Ethereum's own boundary — `spec.fork_name_at_slot(slot).gloas_enabled()` (and the
> equivalent `ForkName::Gloas` comparison), read from the eth2 `ChainSpec`'s
> `gloas_fork_epoch`. The SSV `Fork` enum is back to just `Alan` and `Boole`
> (`fork.rs:22`), and the `cstar` domain-type / schedule entries are deleted. If you find a
> `>= Fork::CStar` reference anywhere, it is stale — the live check is `gloas_enabled()`.

[sip-94]: https://github.com/ssvlabs/SIPs/pull/94
[eip-7732]: https://eips.ethereum.org/EIPS/eip-7732

### 15.1 What ePBS changes, in one breath

EIP-7732 **enshrines proposer–builder separation** in the protocol. Today a proposer
outsources its execution payload to a builder through an off-protocol **MEV-Boost relay**:
it signs a *blinded* block (committing to a payload header) and trusts the relay to swap
its signature for the full payload. ePBS removes the relay. A block is split in two:

- the **consensus block** (the beacon block, proposed at `t = 0`), which now commits to a
  builder **bid** — a `SignedExecutionPayloadBid` — rather than to a full payload; and
- the **execution payload**, revealed *later in the slot* by the builder as a
  `SignedExecutionPayloadEnvelope` directly on the p2p network.

Three consequences ripple into validator duties, and therefore into Anchor:

1. **The attestation deadline moves earlier** — from `1/3` of the slot (≈4000 ms) to `1/4`
   (3000 ms on a 12 s slot) — because attesters now vote only on the consensus block, not
   on a validated payload. (In spec terms this is the new basis-point timing model:
   `ATTESTATION_DUE_BPS` drops from `3333` to `ATTESTATION_DUE_BPS_GLOAS = 2500`; the old
   `INTERVALS_PER_SLOT` constant is retired.)
2. **`AttestationData.index` is repurposed** to carry the attester's fork-choice view of
   payload status (`0` = payload `EMPTY`, `1` = payload `FULL`), so it must now be agreed
   via QBFT instead of being a locally-derived constant. (Spec carve-out: for a *same-slot*
   attestation the index is always `0`.)
3. **New duties appear**: the **Payload Timeliness Committee** (PTC) attestation, and the
   proposer's **proposer-preferences** broadcast. The **blinded block** path effectively
   disappears (blinded blocks are no longer a concept under Gloas).

On the SSV side this is gated on Ethereum's Gloas activation (`gloas_enabled()`); there is
no separate SSV fork for it.

### 15.2 The fork wiring: gating on `ForkName::Gloas`

Anchor does **not** invent an SSV fork for ePBS. The SSV `Fork` enum stays at two variants
(`anchor/common/fork/src/fork.rs:22`):

```rust
pub enum Fork {
    Alan,  // subnet = committee_id % 128;     topic "ssv.v2.<subnet>"
    Boole, // subnet = min(SHA256(op)) % 128;   topic "/ssv/<network>/boole/<subnet>"
}
```

Every ePBS behavior keys off Ethereum's fork instead, via the eth2 `ChainSpec` carried
through Anchor. Two equivalent spellings appear in the code:

- **slot-scoped:** `self.spec.fork_name_at_slot::<E>(slot).gloas_enabled()` — used wherever
  a slot is in hand (QBFT dispatch, committee signing).
- **epoch/version-scoped:** `ForkName::from(version) >= ForkName::Gloas` — used where only a
  decoded `version` or epoch is available (block decode, message validation).

The payoff of this approach is that **no new SSV domain type, schedule entry, or subnet
topology is needed** — ePBS messages ride the *existing* Boole subnet/topic surface, and
the fork boundary is automatically the same one Ethereum and Lighthouse already agree on.
The cost is that the gate is now spread across two crates' worth of `gloas_enabled()` /
`ForkName::Gloas` call sites rather than one `Fork` ordinal; the **`gloas`** substring is
your search anchor throughout this section.

### 15.3 Slot timing tightens (merged)

Anchor previously hard-coded the attestation start at `slot/3` and aggregation at
`slot*2/3`. Under Gloas the unaggregated-attestation deadline tightens to `slot/4`. Rather
than branch on the fork by hand, Anchor now asks **Lighthouse's `ChainSpec`** for the
deadline, which itself flips at `gloas_fork_epoch` (`validator_store/src/lib.rs:1542`):

```rust
let timeout_mode = TimeoutMode::SlotTime {
    instance_start_time: self
        .get_instant_in_slot(slot, self.spec.get_attestation_due::<E>(slot))?,
};
```

`get_attestation_due` returns the pre-Gloas deadline (≈`slot/3`) before the fork and the
tighter Gloas deadline (`slot/4`) at/after it — a property pinned by a test
(`lib.rs:3713`, `attestation_due_switches_at_gloas_boundary`). The metadata service's
Phase 2 wake uses the **next** slot's deadline so the tighter budget applies on the correct
side of the boundary (`metadata_service.rs:261`):

```rust
// We sleep into the next slot, so look up the deadline for that slot, not the current
// one. Otherwise the tighter Gloas deadline would apply one slot late at the fork boundary.
let fallback = self_clone_phase2.spec.get_attestation_due::<E>(next_slot);
```

A broad, mechanical cleanup rides along: every `Duration::from_secs(spec.seconds_per_slot)`
was replaced with `spec.get_slot_duration()` (client, notifier, subnet message-rate,
validator store, metadata service). This is groundwork for **basis-point slot timing**: the
Gloas spec drops the old `INTERVALS_PER_SLOT` model in favor of deadlines expressed in
basis points of `SLOT_DURATION_MS` (e.g. `ATTESTATION_DUE_BPS_GLOAS = 2500` → 25 % = 3000
ms on a 12 s slot), so slot duration is no longer assumed to be a whole number of seconds.

> **Open research (#1028, #1092).** The tighter 3000 ms attestation deadline eats into the
> consensus budget. With weighted-attestation-data (WAD) enabled, the WAD hard timeout now
> races QBFT round 1 with near-zero slack; whether to lower it, fork-gate it, or document
> the zero-slack behavior is **undecided pending production latency data** (#1028).
> Separately, the committee QBFT instance still *hard-sleeps to the attestation deadline*
> before starting round 1, so even with the head-event monitor delivering the vote early,
> consensus doesn't start until ~3.0 s; decoupling an `earliest_start` from the
> round-timeout anchor is a proposed-but-unsettled design (#1092).

### 15.4 The attestation vote grows a field: `GloasBeaconVote` (merged)

Pre-Gloas, the committee agrees on a `BeaconVote { block_root, source, target }` (§12).
Under Gloas the attester's `AttestationData.index` is **no longer zero** — it encodes the
attester's fork-choice view of payload status (`0` = empty, `1` = full, for non-same-slot
attestations). Because that value is part of the **signed attestation root**, every operator
must sign the *same* index, which means it has to travel through QBFT. Hence a new consensus
value, `GloasBeaconVote` (`common/ssv_types/src/consensus.rs:943`):

```rust
pub struct GloasBeaconVote {
    pub block_root: Hash256,
    pub source: Checkpoint,
    pub target: Checkpoint,
    /// BN-supplied `AttestationData.index`. Under Gloas this encodes the attester's
    /// fork-choice view of payload status (`0` = EMPTY, `1` = FULL for non-same-slot
    /// attestations) and participates in the signed attestation root, so it must travel
    /// through QBFT rather than being reconstructed locally.
    pub attestation_data_index: u64,
}
```

`GloasBeaconVote` and `BeaconVote` are deliberately **separate types**: their SSZ
encodings differ in length (120 vs 112 bytes), so a pre-Gloas binary mutually *rejects*
Gloas-shaped wire bytes instead of silently misreading them — a fork-safety property pinned
by `test_beacon_vote_rejects_gloas_bytes` (`consensus.rs:2969`).

The committee attestation path branches on the Ethereum fork to pick the consensus value
(`validator_store/src/lib.rs:1550`, in `sign_committee_attestations`):

```rust
let (block_root, source, target, decided_hash) = if self
    .spec
    .fork_name_at_slot::<E>(slot)
    .gloas_enabled()
{
    // decide a GloasBeaconVote { ..., attestation_data_index: first_att_data.index }
    // TODO(#1027): apply `decided.attestation_data_index` to each validator's
    // `attestation.data.index` before signing.
} else {
    // decide the existing BeaconVote { block_root, source, target }
};
```

> **Open gap (#1027).** Note the `TODO(#1027)` at `lib.rs:1576`: the Gloas branch reaches
> *consensus* on the index but does not yet **apply** the decided index back onto each
> validator's `AttestationData` before signing — the signing root is still built from
> `block_root/source/target` only. Until #1027 lands, the index is agreed but not actually
> stamped onto the attestation.

#### The Gloas validator: `GloasBeaconVoteValidator`

A parallel validator (`consensus.rs:1193`) carries over every pre-Gloas check (far-future
target, source < target, majority-fork protection, slashing) and adds the two Gloas rules:

1. **Range-check** the index to `{0, 1}` (`consensus.rs:1267`) — anything `>= 2` is
   `IndexOutOfRange`. The same-slot "`index = 0`" rule (per the Ethereum spec) is *not*
   enforced here — it would need a BN lookup — and is left to gossip/BN validation.
2. **Slashing-DB reconstruction with the decided index** (`check_attestation_slashing`,
   `consensus.rs:1326`): the protection check rebuilds `AttestationData` using the single
   QBFT-decided `attestation_data_index`, so an operator can't be walked into signing both
   `index=0` and `index=1` for the same `(slot, source, target)` — a cross-index double
   vote trips protection. (Pinned by `test_gloas_slashing_trips_on_cross_index_equivocation`,
   `consensus.rs:2690`.)

> **Accepted trust model.** The SSV spec (PR #94 §2) makes the index **trusted from the
> QBFT leader** — it is never compared against each operator's own BN view, exactly as the
> existing `BeaconVote.block_root` already trusts the leader. A malicious leader can
> therefore push a payload-status value contrary to the cluster's BN-majority observation;
> the validator's job is to keep the value *well-formed* and *non-self-slashing*, not to
> arbitrate fork choice. (Caveat: the spec text frames this only as parallel to
> `block_root` leader-trust; the "worst case is a missed attestation" characterization is
> this document's own inference, not spec wording.)

#### Routing in the QBFT manager

`QbftManager` gains a fourth instance map, `gloas_beacon_vote_instances`
(`qbft_manager/src/lib.rs:144`), and the `Role::Committee` ingress arm forks on the
Ethereum fork to spawn the right instance type (`lib.rs:321`):

```rust
if self.spec.fork_name_at_slot::<E>(slot).gloas_enabled() {
    self.pass_to_instance::<GloasBeaconVote>(id, wrapped)
} else {
    self.pass_to_instance::<BeaconVote>(id, wrapped)
}
```

Both map to the **same** `Role::Committee` / `DutyExecutor::Committee` `MessageId`
(`QbftDecidable for GloasBeaconVote`, `lib.rs:511`), so the message-ID/topic surface is
unchanged across the fork — only the *decoded value type* differs. The
`gloas_dispatch_tests` (`qbft_manager/src/tests/gloas_dispatch_tests.rs`) assert that a
committee message spawns a `GloasBeaconVote` instance at/after Gloas and a `BeaconVote`
instance before it.

> **Open gap (#1061) — the sync-committee path was *not* migrated.**
> `sign_committee_sync_committee_signatures` (`validator_store/src/lib.rs:1410`) still
> decides over a plain `BeaconVote` at `slot/3` timing (`get_slot_duration() / 3`,
> `lib.rs:1431`), while the *ingress* router (above) sends post-Gloas committee messages to
> `GloasBeaconVote` instances. The result: after Gloas, the sync-message committee instance
> receives no matching peer input and **times out every slot**. #1061 tracks aligning both
> call sites on `GloasBeaconVote`. This is a concrete, known regression on the branch — not
> a hypothetical.

### 15.5 The block loses its blinded form (merged)

Under Gloas the execution payload is decoupled from the block body, so the proposer signs a
plain `BeaconBlock` and the **blinded-block concept becomes redundant** — blinded blocks are
no longer a Gloas concept at all, so for this variant the would-be blinded and full
projections decode to the same thing. `ProposerConsensusData` gains a direct
`decode_block::<E>()` (`consensus.rs:267`), and the two consumers fork-gate on
`ForkName::Gloas` accordingly:

- **Block signing** — `decode_decided_block` (`validator_store/src/lib.rs:1934`, branch at
  `:1939`): Gloas decodes `DataSSZ` directly as a `BeaconBlock`; pre-Gloas keeps the existing
  try-blinded-then-full fallback. This removes a guaranteed-to-fail blinded decode on every
  Gloas proposal.
- **Block validation** — `ProposerConsensusDataValidator::validate_block_proposal`
  (`consensus.rs:387`, branch at `:399`): same fork gate, and `decode_blinded_block` now
  *rejects* Gloas input with `NoMatchingVariant` (`consensus.rs:255`) so the blinded path can
  never be taken post-fork (pinned by `decode_blinded_block_rejects_gloas`, `consensus.rs:3038`).

> **Open gap (#1017), blocked upstream.** `sign_block`'s destructure of Lighthouse's
> `FullBlockContents` will need a Gloas arm that extracts the `.block` and explicitly drops
> the builder envelope (the cluster signs and publishes only the `BeaconBlock`, never the
> payload). This is **blocked on Lighthouse / beacon-APIs PR #580** finalizing the
> `produceBlockV4` self-build body shape — LH itself carries a TODO that its current Gloas
> SSZ encoding diverges from the in-flight spec.

> **Envelope signing — stubbed in code, but the spec is in flux.** In the *code*,
> distributed signing of the builder's `SignedExecutionPayloadEnvelope` is a deliberate
> `Unsupported` stub (`sign_execution_payload_envelope`, `lib.rs:3315`, `TODO(gloas)`).
> An earlier SSV-spec draft (the basis for that stub) treated envelope signing as **out of
> scope** — not an SSV duty, with the accepted consequence that on a self-build slot the
> cluster does not republish the envelope and PTC members may observe
> `payload_present = FALSE`. **However, the current SIP PR #94 has since reversed this**: at
> its present HEAD it defines envelope signing as a *new QBFT-based SSV duty* (a second QBFT
> round producing the signature, which operators then publish). This is exactly the kind of
> moving target this section warns about — the code's stub reflects the *older* design, and
> aligning it with the current spec is open, unscheduled work.

### 15.6 The PTC attestation: a new validator-scoped, *leaderless* duty

The Payload Timeliness Committee is a 512-validator subset of a slot's attesters
(`PTC_SIZE = 512` in the spec). Each member independently observes whether the builder
revealed the payload on time and broadcasts a `PayloadAttestationMessage` (whose
`PayloadAttestationData` carries `payload_present` and `blob_data_available`). Crucially this
is **not a consensus duty**: there is no value to negotiate (each member reports its own
observation) and there is no slashing for PTC equivocation. This shape drove a mid-effort
**redesign** on the branch.

The wire surface is `Role::PTCAttester` (`common/ssv_types/src/msgid.rs:24`, wire byte
`[7,0,0,0]` at `:37`) with a matching `PartialSignatureKind::PTCAttester`. Its defining
property is that it is **validator-scoped and non-QBFT**:

- It uses `DutyExecutor::Validator` (keyed by validator pubkey), not `Committee`
  (`msgid.rs:164`).
- It has **no max QBFT round** (`max_round() == None`, `msgid.rs:78`), and a new helper
  `Role::is_qbft_role()` (`msgid.rs:85`) returns `false` for it.

That single classification is load-bearing across three call sites, so the role-partition is
pinned by a test (`role_qbft_classification_is_pinned`, `msgid.rs`):

1. **Consensus-message rejection** — `validate_consensus_message_semantics` now rejects a
   consensus message for *any* non-QBFT role generically (`consensus_message.rs:124`):

   ```rust
   if matches!(msg_id.role(), Some(role) if !role.is_qbft_role()) {
       return Err(ValidationFailure::UnexpectedConsensusMessage);
   }
   ```

   (This replaced the hand-maintained `ValidatorRegistration | VoluntaryExit` list and
   retired the now-dead `FailedToGetMaxRound` failure.)
2. **QBFT manager routing** — `PTCAttester` is added to the "these roles don't use QBFT"
   arms in `receive_data` (`qbft_manager/src/lib.rs:286`, `:355`), so a PTC message can never
   spawn a consensus instance.
3. **Partial-signature validation** — `PTCAttester` binds to its own
   `PartialSignatureKind::PTCAttester` (`partial_signature.rs:148`), counts as a
   pre-consensus message (`message_counts.rs:70`, `:114`), gets a flat per-validator duty
   limit of `2` (`lib.rs:1034`), and uses the **short TTL** (`1 + LATE_SLOT_ALLOWANCE`,
   `lib.rs:948`) because a PTC vote is only useful for its own slot.

A fork safety-net rejects `PTCAttester` messages before Gloas. Note this is a **distinct
failure variant** from the SSV-fork gate: PTC uses `RoleNotActiveBeforeEthFork`
(`message_validator/src/lib.rs:879`), keyed on the *Ethereum* fork
(`spec.fork_name_at_epoch(epoch).gloas_enabled()`, `minimum_fork: ForkName::Gloas`), whereas
the older Boole-gated roles use `RoleNotActiveBeforeFork` keyed on the SSV `Fork` ordinal
(`lib.rs:859`). The two variants sit side by side in `validate_role_for_fork` (`lib.rs:849`).

> **This area was redesigned mid-flight — read git history carefully.** The SSV spec was
> rewritten from a *committee-scoped, QBFT-based* PTC to the *validator-scoped, leaderless*
> model above. Two merged-then-reworked PRs are the fossils: a committee-scoped
> `Role::PTCCommittee` (#1033) was retargeted to `Role::PTCAttester` (#1080), and a
> `PayloadAttestationVote` QBFT value object (#1047) was **reverted entirely** (#1076) as
> dead code. If you find references to "PTC QBFT" or a payload-attestation consensus value,
> they are obsolete.

> **Signing path now implemented (#1082); client wiring still open.** `sign_payload_attestation`
> (`validator_store/src/lib.rs:3324`) is **no longer a stub** — it collects a single-validator
> partial signature over the `PayloadAttestationData` signing root (`Domain::PTCAttester`),
> with explicitly **no beacon-node fetch and no slashing protection** (Lighthouse already
> fetched the data and abstained on no-block at the 75 % cutoff). The HTTP timeout quotients
> for PTC duties are wired in the client (`HTTP_PTC_DUTIES_TIMEOUT_QUOTIENT`,
> `HTTP_PAYLOAD_ATTESTATION_TIMEOUT_QUOTIENT`, `client/src/lib.rs:80–81`). What remains open
> is the client-side `PayloadAttestationService` that drives Lighthouse's PTC duty loop
> against the Anchor backend (#1038). Because PTC is honest-convergence rather than
> consensus, a minority operator near the observation cutoff can silently miss the threshold;
> surfacing that as a distinguishable divergence metric (#1079) is **not** a normative spec
> requirement and is open research.

> **Spec note — `payload_present` is not the whole story.** The Ethereum
> `PayloadAttestationData` carries **two** booleans: `payload_present` *and*
> `blob_data_available`. A PTC member attests to both — that the payload was revealed on
> time *and* that its blob data is available — not just payload presence.

### 15.7 Proposer preferences (planned, not yet started)

Under ePBS a proposer broadcasts `SignedProposerPreferences` — fee recipient and
`target_gas_limit` — to the `proposer_preferences` gossip topic, so builders can construct
payloads to its liking; *not* broadcasting them implicitly declines all external-builder
bids for that slot. (Note: the per-slot builder commitment is a separate object,
`ExecutionPayloadBid`; proposer preferences are the proposer's standing intent.) In Anchor
this is a future **signature-only** duty (no QBFT — every operator already knows the bytes),
modeled on `sign_validator_registration_data`. Like registration it is expected to **not
touch slashing protection** (Lighthouse bypasses the slashing DB and doppelganger for
registration, so Anchor would match) — though note the SSV spec is currently *silent* on
slashing protection for preferences, so this is an inference from the registration analogy,
not a stated requirement. As with registration, operators must agree on byte-identical
contents despite signing independently.

None of this exists in code yet: there is **no `Role::ProposerPreferences`** in `msgid.rs`,
and `sign_proposer_preferences` (`validator_store/src/lib.rs:3367`) is an `Unsupported` /
`TODO(gloas)` stub. The work is tracked across four issues: the role + message-validator
fork-gate (#1062), the signing path (#1063), the client-side `ProposerPreferencesService`
(#1064), and fork-gating the existing `RegistrationService` at the Gloas epoch (#1065).

### 15.8 Status summary

State is as of `epbs` HEAD `a7c0b84`. **Merged** rows cite the **PR** that landed the work
(verifiable in `git log unstable..epbs`); **Open** rows cite the **issue** tracking the gap.

**Merged (on `epbs`):**

| Area | PR(s) |
|---|---|
| Lighthouse bump to a Gloas-aware `unstable` | #1013 |
| Fork-aware attestation deadline (`get_attestation_due`) + `get_slot_duration` cleanup | #1024 |
| `GloasBeaconVote` consensus value | #1059 |
| `GloasBeaconVoteValidator` (index range-check + cross-index slashability) | #1071 |
| Gloas decode branch on `ProposerConsensusData` (`decode_block`) | #1070 |
| Fork-gate block decode / validate, drop the blinded path | #1072 |
| `Role::PTCAttester` wire surface (validator-scoped, non-QBFT) | #1033 (orig `PTCCommittee`) → #1080 (retarget) |
| `PayloadAttestationVote` QBFT value object — **added then reverted** as dead code | #1047 ↩ #1076 |
| `sign_payload_attestation` (SingleValidator partial-sig, no slashing protection) | #1082 |
| **Remove `Fork::CStar`; gate ePBS on Ethereum `gloas_enabled()`** | #1090 |
| (`Fork::CStar` + `cstar` schedule were added by #1016/#1030, then **deleted by #1090**) | #1016, #1030 → #1090 |

**Open (gaps and follow-ups):**

| Area | State | Issue(s) |
|---|---|---|
| Apply decided `attestation_data_index` onto each `AttestationData` before signing | `TODO(#1027)` in code | #1027 |
| Migrate sync-committee committee vote to `GloasBeaconVote` | known per-slot timeout after Gloas | #1061 |
| `sign_block` Gloas `FullBlockContents` destructure | blocked on Lighthouse beacon-APIs #580 | #1017 |
| `produceBlockV4` / proposer block-decode spec tests | open | #1073 |
| PTC client service (`PayloadAttestationService`) + metadata voting-context phase | open (signing path itself merged via #1082) | #1038, #1036 |
| Proposer preferences: role, sign path, service, registration fork-gate | unstarted; no `Role::ProposerPreferences` yet | #1062, #1063, #1064, #1065 |
| `sign_execution_payload_envelope` | `Unsupported` stub; spec (PR #94) now wants a QBFT duty | *(no issue; spec in flux)* |
| WAD timeout under tighter budget / eager-start decoupling | open research | #1028, #1092 |
| PTC no-threshold divergence metric | open research, non-normative | #1079 |
| Wire PTC *committee-scoped* QBFT instances | **obsolete** — superseded by the validator-scoped redesign | #1035 |

> **Stale-issue caveat.** Issue hygiene on the upstream tracker lags the code: #1037
> ("implement `sign_payload_attestation`") and #1038 duplicate the now-closed #1077/#1078,
> and #1035 belongs to the abandoned committee-scoped PTC design. Trust the **code** and the
> merged-PR list over open-issue titles where they disagree.

**Maintainer takeaway.** The merged work is the *plumbing*: Ethereum-fork gating (no SSV
fork), a wider attestation vote, fork-gated block decoding, a new role classification, and
the PTC signing path. The behavior is **not yet end-to-end** — the decided index isn't
applied (#1027), the sync-committee path times out every slot after Gloas (#1061), block
signing is blocked on Lighthouse (#1017), and proposer preferences are unstarted
(#1062–#1065). When reading this code, **`gloas_enabled()` / `ForkName::Gloas`** branches and
`TODO(gloas)` / `TODO(#…)` markers are your search anchors. Two specs are still moving — the
Ethereum Gloas spec and SSV's unmerged SIP PR #94 — so re-check both before treating any
"open question" here as settled.

---

## 16. A maintainer's debugging map

A quick index of "symptom → where to look," distilled from the hot paths above.

| Symptom | First place to look | Why |
|---|---|---|
| Node "does nothing" at startup | `is_synced` watch + `SsvEventSyncer` (`eth/src/sync.rs:250`) | Duty services block on historical sync (`client/src/lib.rs:698`) |
| "I'm not signing for validator X" | `ValidatorAdded` handling (`event_processor.rs:459`); `state.shares().get_by(pubkey)` | If we hold no share, we're not a co-signer; check the share parsed & matched our `OperatorId` |
| Duty present but `validator_index` never set | index syncer (`eth/src/index_sync.rs:47`) | Most duties need the beacon-chain index |
| Consensus never decides | `Qbft::receive` (`qbft/src/lib.rs:539`), quorum (`msg_container.rs:56`), leader check | Wrong leader/round, or < quorum honest operators online |
| Rounds keep timing out | `timeout.rs:24`, `instance_start_time` in validator_store (`lib.rs:1375`/`408`) | Misaligned slot-offset start or `Relative` vs `SlotTime` mismatch |
| Full signature never reconstructs | `try_reconstruct` (`signature_collector/src/lib.rs:679`) | < threshold partials, or operators signed different roots (QBFT didn't converge) |
| Messages rejected / peers getting banned | `message_validator` (`lib.rs:427`), verdict map (`:227`) | Check which `ValidationFailure` → `Reject` vs `Ignore`; remember `Reject`→`Ignore` while unsynced |
| Not receiving a committee's traffic | subnet calc (`subnet.rs:54`/`71`), subnet service (`subscriptions.rs:86`) | Wrong subnet for the active fork, or subscription not updated after a DB change |
| Slashable / double-sign worry | `slashing_protection_attestations` (`lib.rs:1744`), `check_and_insert_block_proposal` | Anchor checks Lighthouse's slashing DB before returning signed attestations/blocks |
| Everything is slow under load | processor metrics `ANCHOR_PROCESSOR_QUEUE_LENGTH`, `_WORK_EVENTS_*` | The processor is the global throttle; a saturated `urgent_consensus` queue delays consensus |
| Behavior changed at an epoch boundary | `ForkSchedule::active_fork` (`schedule.rs:194`) and `>= Fork::Boole` branches | A fork activated and switched subnet/topic/aggregation rules |

### Reading order for a new maintainer

1. `client/src/lib.rs:92` (`Client::run`) — the wiring of everything.
2. `common/ssv_types/` — the vocabulary (operator, share, cluster, committee, message).
3. `eth/src/sync.rs` + `event_processor.rs` — how the DB gets populated.
4. `processor/src/lib.rs` — the scheduler every other component uses.
5. `validator_store/src/lib.rs` — the duty surface (start with `sign_committee_attestations`).
6. `common/qbft/src/lib.rs` (`receive`) + `qbft_manager/src/instance.rs` — consensus.
7. `signature_collector/src/lib.rs` — threshold reconstruction.
8. `message_receiver/src/manager.rs` + `message_validator/src/lib.rs` + `message_sender/src/network.rs` — the wire.
9. `subnet_service/` + `common/fork/` — topology and forks.

### Verification note

Code excerpts and `file:line` anchors in §§1–14 and §16 were taken from the `unstable`
branch by reading the sources directly; the **§15 ePBS / Gloas** anchors were taken from
the `epbs` branch at HEAD `a7c0b84`, which is ahead of `unstable` and still in active
development. They are accurate as a *map*, but line numbers drift — treat the quoted
**function and type names** as the durable references and re-locate by symbol when a line
number no longer matches. The §15 "open" items reflect the issue/PR state at the time of
writing and will go stale fastest; re-check the linked issues — and both moving specs (the
Ethereum Gloas spec and SSV's unmerged SIP PR #94) — before relying on them. This document describes
structure and data flow; it does not replace running `make test` / `make lint` when you
change any of these paths (see `.claude/rules/verification.md`).
