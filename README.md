# java-event-sourcing-ledger

A small, real, event-sourced ledger for a **fictional internal-credits system** (accounts hold "credits";
there is no money, no cards, no personal data). Plain Java 17, no frameworks, JUnit 5 only.

It exists to show the mechanics that usually get hand-waved: an append-only log with optimistic
concurrency, crash recovery of a half-written file, a two-step transfer saga with compensation,
idempotent commands, projections that must equal a from-scratch rebuild, snapshots, and schema upcasting.

## What is in it

| Piece | Where | What it does |
|---|---|---|
| Event store (in-memory) | `AbstractEventStore`, `InMemoryEventStore` | Append-only; per-stream `expectedVersion` (optimistic concurrency); gap-free global `seq` |
| Event store (file) | `FileEventStore` | JSONL, one event per line; optional `fsync` on append; torn-last-line recovery on reopen |
| Aggregate | `Account`, `LedgerEvent`, `Command` | Immutable state; `decide*` (command to event, may refuse) and `apply` (event to state); no overdraft |
| Transfer saga | `Ledger` | Debit source, credit destination, compensate the source if the credit is refused; `recoverSagas()` after a crash |
| Idempotency | `Ledger` + `Projections.SagaIndex` | Every event stores its `commandId`; a repeated id is answered `DUPLICATE` (survives restart) |
| Projections | `Projections` | Balance view, per-UTC-day statement, saga/command index; pure folds over the log |
| Snapshots | `AccountRepository` | Every N events per stream; `load = snapshot + tail` |
| Schema versioning | `EventCodec` | v1 to v2 upcaster (adds `currency`, default `CRD`); old lines are upcast on read, never rewritten |
| JSON | `Json` | ~200-line encoder/decoder for flat objects with string/integer values; rejects everything else |
| Demo / Bench | `Demo`, `Bench` | Crash-mid-saga scenario with invariant checks; crude throughput/replay numbers |

## Design notes

- **One stream per account** (`acct-<id>`). A transfer touches two streams, so it cannot be one atomic append;
  hence the saga: `TransferDebited` (source stream), then `TransferCredited` (destination stream), or
  `TransferCompensated` (source stream) if the credit is refused.
- **Crash recovery of a saga**: if the process dies after the debit, the log holds a `TransferDebited` with no
  matching credit/compensation. The saga index projection exposes it as *pending*; `recoverSagas()` retries the
  credit (or compensates). While a transfer is in flight, `sum(balances) + in-flight == credits issued - withdrawn`;
  after recovery, `sum(balances) == credits issued - withdrawn`.
- **The destination is validated only at the credit step**, on purpose, so the compensation path is real and testable.
- **Torn-last-line rule** (`FileEventStore`): an entry exists only if its line is newline-terminated and parses.
  Bytes after the last `\n` are a torn tail (the append was never acknowledged) and are truncated. A
  newline-terminated line that does not parse, a blank line, or a seq/version gap is corruption and throws
  `CorruptLogException` rather than being silently dropped. A complete JSON object missing only its `\n` is also
  dropped (it was not acknowledged either).
- **Optimistic concurrency in the command path**: load aggregate, decide, append with `expectedVersion`; on a
  version conflict, reload and decide again (so a stale decision cannot overdraw).
- **Projections are pure folds**, so "rebuild from scratch == incrementally maintained" is a checkable property.
  The `Ledger` catches its projections up from the log before every command (read-your-writes).
- **`apply()` uses `instanceof` pattern matching over the sealed `LedgerEvent`, not a `switch` with type
  patterns**: pattern-matching `switch` is a preview feature on JDK 17 (final in 21) and this project targets
  `--release 17` and CI runs 17 and 21. The cost is losing the compiler's exhaustiveness check, so a test enumerates
  `LedgerEvent.class.getPermittedSubclasses()` and requires the codec samples (and `apply`/`delta`) to cover all of them.

## Run it

```bash
mvn -q -B verify                                            # build + all tests
java -cp target/classes com.github.sup3rmar10.ledger.Demo   # crash-mid-saga scenario + invariants
java -cp target/classes com.github.sup3rmar10.ledger.Bench  # crude throughput/replay numbers
```

The demo (all synthetic) opens three accounts, deposits and withdraws, sends a duplicate command, a rejected
overdraft, and a transfer to an unopened account (compensated). It then simulates a process crash after the debit
of a transfer (a `SimulatedCrash` exception, not `kill -9`), appends a partial line to the log file to simulate a
torn write, reopens, recovers, and prints invariant checks (exit code 1 if any fails). Sample tail of the output:

```
  torn bytes discarded on reopen: 27
  before recovery: sum(balances)=1575 in-flight=75 (sum + in-flight = 1650)
  recoverSagas() completed 1 pending transfer(s)
  [PASS] sum(balances) == credits issued minus withdrawn (1650) after recovery
  [PASS] no negative balance
  [PASS] rebuild-from-scratch projections == incrementally maintained
RESULT: all invariants hold
```

## Tests

68 test invocations (JUnit 5), all passing locally on Windows 11 with JDK 17 (`mvn -q -B verify` run twice).
The `EventStore` contract (7 tests) runs against both the in-memory and the file store.

- **Property-style tests** (`PropertyTest`): seeded `java.util.Random`; 3,000 random 30-command sequences
  (90,000 commands) checked against an independent model (a map of balances) for outcome, balances and credit
  conservation, plus `rebuild == incremental`, `snapshot + tail == full replay`, and statement closing balances;
  800 more sequences with random crashes between saga steps followed by restart + recovery; 40 more through the
  file store including reopen. Failure messages include the seed (same seed = same run).
- **Torn-tail test** cuts a 3-event file at every byte offset of its last line and checks the reopen result and
  the physical truncation.
- **Upcasting**: a literal v1 line is read as v2 with `currency=CRD`; a file with a v1 line is read, appended to,
  and the old line is verified byte-for-byte untouched.
- **Idempotency**: duplicates are ignored in-process and after a restart from the file.

## Mutation check (by hand)

I applied each mutation below to the main source, ran `mvn -q -B test`, recorded whether any test failed, and
restored the file (the source tree hash was verified identical before and after). The edits were applied by a
throwaway script that is not in this repo; each row names the exact change so it is easy to reproduce.

| # | Mutation (file) | Result | Killed by (first tests my log parser listed) |
|---|---|---|---|
| 1 | Skip the `expectedVersion` check (`AbstractEventStore`) | killed | `LedgerTest` stale-decision and conflict tests; the contract tests for both stores also assert it |
| 2 | Allow overdraft, no funds check (`Account`) | killed | `AccountTest.overdraftIsRejected...`, `LedgerTest.rejectedCommand...` |
| 3 | Drop the compensation append (`Ledger`) | killed | `LedgerTest.failedCreditCompensatesTheDebit`, crash-then-compensate test |
| 4 | Snapshot off-by-one: snapshot when `version % N == 1` (`AccountRepository`) | killed | `SnapshotTest.snapshotIsTakenAtEveryNthEvent...` |
| 5 | Snapshot tail off-by-one: replay from `snapshot.version - 1` (`AccountRepository`) | killed | `ProjectionsTest.balanceViewMatches...`, many others (double-applied event) |
| 6 | Upcaster default currency wrong, `USD` (`EventCodec`) | killed | `EventCodecTest.upcastsV1AccountOpened...` |
| 7 | Drop the idempotency (`commandId` seen) check (`Ledger`) | killed | `LedgerTest.duplicateCommandIdIsAppliedOnlyOnce` |
| 8 | Do not truncate the torn tail; append after the garbage (`FileEventStore`) | killed | `FileEventStoreTest.tornTail...`, `appendAfterTornRecovery...` |
| 9 | Drop `fsync` (`force`) on append (`FileEventStore`) | **SURVIVED** | nothing can observe it without killing the OS or pulling power |
| 10 | `recoverSagas` does not finish pending transfers (`Ledger`) | killed | `LedgerTest.crashAfterDebit...` (both) |
| 11 | Statement counts credits as debits (`Projections`) | killed | `ProjectionsTest.statementGroupsByUtcDay...` |
| 12 | JSON encoder does not escape backslash (`Json`) | killed | `JsonTest.roundTripsStrings...` |
| 13 | No seq-gap check when indexing (`AbstractEventStore`) | killed, **but only after I added a test** | first run: SURVIVED; the original gap test removed a line inside one stream, so the version check masked it. Added `sequenceGapAcrossDifferentStreamsIsCorruptionToo` |
| 14 | `readAll(afterSeq)` off by one (`AbstractEventStore`) | killed | 20 tests |
| 15 | Concurrency retries disabled, `MAX_RETRIES = 0` (`Ledger`) | killed | `LedgerTest.conflictingConcurrentDeposit...` |
| 16 | Non-positive amounts accepted (`Account`) | killed | `AccountTest.nonPositiveAmountsAreRejected` |
| 17 | Operations allowed on never-opened accounts (`Account`) | killed | `AccountTest.operationsOnAnUnopenedAccount...`, compensation tests |
| 18 | Saga index never clears pending on `TransferCredited` (`Projections`) | killed | `LedgerTest.crashAfterDebit...`, `ProjectionsTest.sagaIndexTracks...` (my first version of this mutation did not compile and was redone) |
| 19 | `Account.apply` does not advance `version` | killed | 26 tests |
| 20 | Codec writes schema 1 instead of the current one | killed | `EventCodecTest.writesCurrentSchemaAndOneLine` |
| 21 | Deposit overflow guard removed (`Account`) | killed | `AccountTest.depositThatWouldOverflowIsRejected` |
| 22 | Upcaster overwrites an existing currency (`put` instead of `putIfAbsent`) | **SURVIVED** | v1 lines never carry a currency, so no realistic input distinguishes the two; arguably an equivalent mutant |
| 23 | Skip `force(true)` after truncating a torn tail (`FileEventStore`) | **SURVIVED** | same as 9: durability of the truncation is not observable in-process |

**23 mutations: 20 killed, 3 survived** (2 are durability calls that need a real crash to observe, 1 is
near-equivalent). Test counts in the last column are lower bounds: failures of tests inherited from the shared
contract class are printed in a format my log parser skipped. Mutation 13 is the useful lesson: a test that "looked
right" was protected by a second, redundant check, and only the mutation exposed it.

## Measured numbers (laptop, single run each, high variance)

Windows 11 laptop, 8 logical cores, Temurin JDK 17.0.20, local disk (model not recorded), `Bench` run 5 times
(the earlier two runs used a replay account of 200,000 events, the last three 200,050). Crude `System.nanoTime`
loops with a short warm-up, **not JMH**: treat as order-of-magnitude, not a benchmark result.

| Measurement | Range over 5 runs |
|---|---|
| Append, in-memory (500k events, 100 streams) | 0.3M to 1.6M events/sec (typically ~1.2M) |
| Append, file, no fsync (100k events) | 32k to 100k events/sec |
| Append, file, **fsync per append** (2k events) | 540 to 755 events/sec |
| Load one account, 200k events, **full replay** (in-memory store) | 17 to 47 ms |
| Load the same account, **snapshot every 100** (50-event tail) | 0.001 to 0.024 ms |
| Reopen the file store with 100k events (parse + index everything) | 1.4 to 2.8 s |

What these do and do not say: fsync costs roughly two orders of magnitude of append throughput on this machine.
Snapshots make a single-aggregate load nearly free, but they do **not** help process start-up here: the file store
re-parses the whole log on open, and the snapshots live only in memory.

## Limits / not verified

- **Single process, single writer.** `Ledger` methods are `synchronized`; there is no file locking, so two
  processes opening the same log file would corrupt it. Optimistic concurrency is exercised by simulating a
  second writer on the same store in tests, not by real concurrent processes.
- **Durability is not proven.** `fsync` is requested via `FileChannel.force(false)`; I did not test power loss or
  `kill -9`, and the demo's "crash" is a thrown exception plus a hand-appended partial line. The torn-line rule
  assumes one `write` per line batch and that a crash tears only the final line; multi-event batches are not atomic
  across a crash (the `Ledger` only ever appends one event at a time).
- **Not a distributed log**: no replication, no consensus, no retention, no compaction, whole log held in memory.
- **Saga scope**: recovery is retry-or-compensate driven by the log; the compensation trigger is a domain refusal
  of the credit only (destination not open). There are no timeouts, no poison-transfer handling, and recovery
  relies on the single-writer assumption to avoid double-crediting.
- **Rejected commands are not recorded**, so a rejected command id may be retried (a deliberate choice).
- **Snapshots are in-memory only** and are an optimisation, not a persistence format.
- **Idempotency index** is the set of all command ids in the log, kept in memory (unbounded growth).
- **JSON** supports only flat string/integer objects; there is no support for evolving to nested payloads.
- **Tested locally only on Windows 11 + JDK 17.** The CI workflow (`.github/workflows/ci.yml`, JDK 17 and 21 on
  ubuntu-latest) ran green on its first GitHub Actions run (2026-10-20), so JDK 21 and Linux are covered by CI, though I have not run them by hand.
- The benchmark numbers are from one laptop and noisy; mutation testing was manual and covers 23 hand-picked
  mutations, not a systematic tool run.

## License

MIT, see `LICENSE`.
