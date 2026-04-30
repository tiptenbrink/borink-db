# Append-Only Log Contention Plan

The old single shared counter log worked well with low latency, but
high-latency shared storage turned the global counter race into a retry
amplifier. With a 500 ms visibility delay, many writers chose from stale state,
appended records for the same next counter, and then most of those physical
records became lost attempts.

## Goals

- Atomic transactions involving multiple keys.
- Less than 50% wasted physical space due to lost blocks with:
  - 8 writer processes
  - 500 ms storage latency
  - 100,000 keys
  - hot-key distribution:
    - 1,000 hot keys receive 30% of writes
    - next 9,000 keys bring cumulative writes to 80%
    - remaining 90,000 cold keys receive 20%
- Bounded retries with high probability, not a hard guarantee.
- Append-only storage; multiple files are allowed.

## Benchmark Target

The final benchmark should model:

- 8 writer processes.
- 100,000 total keys.
- Three hotness levels:
  - 1,000 keys: 30% of writes
  - next 9,000 keys: next 50% of writes
  - remaining 90,000 keys: remaining 20% of writes
- Writers issue batches of 25 updates.
- Each writer produces around 1 batch per second.
- Total target throughput: 200 updates/second despite 500 ms latency.
- A single update is an atomic transaction and may touch multiple keys.
- Transaction size is Pareto-like:
  - 80% of transactions touch 1 key
  - maximum transaction size is 20 key updates

## Design Direction

The main change is to stop making every writer compete for one global append
counter. Instead, use append-only records whose transaction identity is globally
unique and whose visibility can be determined during merge/indexing.

Resolved logical block model:

- Physical blocks are append-only storage containers. They are not logical
  records and are not versioned.
- Physical blocks all carry group index/count metadata. The common single-block
  case is just a group of one: `{index=0, count=1}`. There is no separate block
  kind.
- A logical block is the commit/visibility unit. It may fit in one physical
  block or span multiple grouped physical blocks.
- Only logical blocks have IDs. Logical block IDs are globally unique random
  128-bit values.
- Duplicate logical blocks are allowed in the append stream. Indexing
  deduplicates by logical block ID, which gives idempotency for retried appends
  whose first attempt actually made it to storage.
- Logical blocks are ordered by the file position of their final physical block.
- The index only observes complete logical blocks, never partial physical
  groups.
- A given index has a version equal to the last logical block it has consumed.
- Key state maps each key to the latest committed logical block ID for that key.
- ID `0`/null is the special absent value. A create-style `put_if` against ID
  `0` succeeds only if the key has not been written yet.
- Success/retry is a logical transaction result, not a physical block result.

Public API scope:

- The intended public API is `get(key)` plus `tx(callback)`.
- `get(key)` reads whatever this LogFile has in its current single-version
  index/cache.
- `tx(callback)` queues side-effect-free transaction code onto one worker thread
  owned by the LogFile and returns a handle that resolves after the worker has
  written the batch and knows this transaction's individual result.
- Inside a transaction, `get(key)` also reads the current single-version index.
- `put_if(key, value)` does not take an expected ID from the caller. It uses the
  last logical block ID observed by the transaction context for that key. If the
  key has not been observed yet, `put_if` first observes the current index state
  for that key and uses that ID.
- A `put_if` transaction returns retry if the key no longer maps to its observed
  logical block ID when the implementation decides which transactions enter the
  next logical block.
- `overwrite(key, value)` buffers a write without a version condition.
- Failed `put_if` transactions are never written into the log.
- One logical block can contain multiple successful transactions.
- If a later transaction in the same candidate logical block conflicts with an
  earlier transaction's buffered write, the later transaction returns retry
  before anything is written.
- Only point-key conditions are tracked. Range/index read sets need a
  higher-level index API before they can be represented precisely.
- Transaction callbacks must be deterministic and free of external side effects
  because retry means the callback may need to be submitted again by the caller.
- `tx().wait()` should return the committed logical block ID on success.
- The API should support returning user data computed by the callback, so
  callers can read multiple keys, compute a result, and receive it together with
  the success/retry outcome.

Example: same-key conflict inside one implementation batch:

```text
Initial index:
  x -> x1

Candidate queue:
  T1: put_if(x, value=x2)   // observes x1
  T2: put_if(x, value=x3)   // observes x1
  T3: put_if(x, value=x4)   // observes x2 via batch overlay

Resolution:
  T1 success, overlay x -> block_id_for_T1
  T2 retry, because x no longer maps to x1
  T3 success if it observed T1's overlay ID

Written logical block:
  contains T1 and T3 only
```

Example: multi-key atomic transaction:

```text
Initial index:
  alice -> a1
  bob   -> b1

T1:
  get(alice) -> a1
  get(bob)   -> b1
  put_if(alice, balance=7)
  put_if(bob,   balance=8)

If no earlier accepted transaction changes alice or bob:
  T1 success, both key updates are written in the same logical block.

If an earlier accepted transaction changes bob:
  T1 retry, neither alice nor bob is written.
```

Example: overwrite plus conditional write:

```text
Initial index:
  x -> x1
  y -> y1

Candidate queue:
  T1: overwrite(x, x2)
  T2: put_if(x, x3), put_if(y, y2)  // observes x1 and y1
  T3: put_if(x, x4)                 // observes T1's overlay ID

Resolution:
  T1 success, overlay x -> block_id_for_T1
  T2 retry, because x no longer maps to x1; y is not written
  T3 success, because its observed x ID matches the overlay
```

Longer-term contention control:

- Prefer per-writer or per-shard logs instead of one shared append point.
- Add small intent/commit records so a lost attempt wastes metadata rather than
  full payload bytes.
- Shard keys so unrelated keys do not contend.
- Use hot-key mitigation: after repeated retries, route work through a
  per-shard/per-key owner queue instead of continuing a stampede.

## Why This Helps

Multi-key transaction records give us the semantic unit needed for benchmark
updates. They do not by themselves solve high-latency contention, but they make
the next steps possible:

- one logical update can atomically affect many keys
- batches can contain many independent transactions
- later sharding or writer-local logs can preserve the same transaction model
- verification can check transaction completeness rather than only single-key
  records

The current implementation writes random logical block IDs into one append-only
file. Reducing wasted space below 50% under the target workload likely requires
the longer-term intent/sharding changes above.
