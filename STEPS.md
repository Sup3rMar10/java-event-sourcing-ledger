# STEPS

## Part A - Steps taken (in order, including the bugs hit)

1. **Checked the toolchain**: JDK 17.0.20 (Temurin) on PATH, Maven 3.9.9. Created the project skeleton and a
   `pom.xml` with `maven.compiler.release=17`, junit-jupiter 5.11.4, surefire 3.5.2 (no other dependencies).
2. **Chose hand-written JSON over Jackson.** The events are flat (strings and integers only), so a ~200-line
   encoder/decoder that rejects everything else was smaller than a dependency plus its configuration, and it
   makes "this line is torn/corrupt" an explicit parse failure.
3. **Designed the model** before coding: one stream per account; sealed `LedgerEvent` (6 records) and sealed
   `Command` (4 records); the transfer is a saga across two streams with `commandId == transferId`.
4. **Hit a language constraint:** a `switch` with type patterns (what I first wanted for `apply()`) is a *preview*
   feature in JDK 17, and `--enable-preview` class files would not run on JDK 21 (the CI matrix). Used `instanceof`
   pattern matching instead and added a test that enumerates `getPermittedSubclasses()` so a new event type cannot
   be forgotten in the codec/`apply`.
5. **Wrote the stores**: `AbstractEventStore` holds the in-memory index and the version/seq rules and calls an
   overridable `persist()` *before* events become visible; `FileEventStore` implements `persist()` with a
   `FileChannel` write, optional `force(false)`, and the torn-tail truncation on open.
6. **Wrote the aggregate, repository (snapshots), projections, ledger (saga + idempotency + retry), demo, bench.**
7. **Wrote the tests** (contract tests shared by both stores, file-store corruption tests, codec/upcast tests,
   aggregate tests, ledger/saga tests with a conflict-injecting store, projections tests, snapshot tests, and
   seeded property tests against an independent model).
8. **First `mvn -q -B verify`: 67 tests, 1 failure.** Real bug found by my demo smoke test: `Demo` read
   `projections().balances().total()` on a freshly constructed `Ledger` *before* the projections had caught up
   with the log, so it printed `sum(balances)=0` and reported an invariant violation. Fixed by adding
   `Ledger.totalBalance()` which catches up first. (The invariant checker caught a bug in the demo, not the ledger.)
9. **Second failure was my own arithmetic:** the test hard-coded 28 torn bytes; the partial line I append is 27
   bytes. Fixed the expectation.
10. **Ran `mvn -q -B verify` twice: both green** (67 tests at that point).
11. **Benchmarked** (`Bench`). First version used a 200,000-event account with snapshots every 100, so the
    snapshot "tail" was empty and the load time was meaningless; changed to 200,050 events so a 50-event tail is
    replayed. Ran the bench five times in total because the numbers swing a lot between runs; the README reports ranges.
12. **Mutation check by hand** (23 mutations, scripted application with automatic restore and a source-tree hash
    comparison). Findings:
    - Mutation 18 first failed to *compile* (a pattern-binding variable used inside an `if (false)` branch); redone
      as `... instanceof TransferCredited c && c.amount() < 0`, then killed.
    - **Mutation 13 (no seq-gap check) survived.** My gap test deleted a line inside a single stream, so the
      per-stream version check caught it and the global-seq check was never needed. Added
      `sequenceGapAcrossDifferentStreamsIsCorruptionToo` (three streams, drop the middle line); mutation 13 is now killed.
    - Survivors that stay: dropping `fsync` (9), skipping `force(true)` after truncation (23), and
      `put` vs `putIfAbsent` in the upcaster (22, near-equivalent). Documented in the README.
13. **Final: `mvn -q -B verify` run twice again, both green, 68 tests**; demo exits 0 and prints
    `RESULT: all invariants hold`. No git commands were run and nothing was pushed from this working session.

## Part B - How to run it yourself

Prerequisites: JDK 17 or newer and Maven 3.9.

```bash
# 1. build and test (runs 68 tests, ~25 s, the property tests take ~9 s)
mvn -q -B verify

# 2. the crash-mid-saga demo (exit code 1 if an invariant fails)
java -cp target/classes com.github.sup3rmar10.ledger.Demo

# 3. crude benchmark numbers (takes a minute; numbers vary run to run)
java -cp target/classes com.github.sup3rmar10.ledger.Bench
```

To repeat a mutation by hand, make one edit from the README table (for example change `if (amount > balance) {` in
`Account.requireFunds` to `if (false) {`), run `mvn -q -B test`, expect failures, then `git checkout` the file.

To reproduce a property-test failure, take the seed from the assertion message and call
`PropertyTest.run(seed, new InMemoryEventStore(), crashes)` (same seed gives the same command sequence).

CI (`.github/workflows/ci.yml`) runs `mvn -q -B verify` and then the demo on JDK 17 and 21.
