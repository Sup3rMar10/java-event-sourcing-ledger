package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.Command.Deposit;
import com.github.sup3rmar10.ledger.Command.OpenAccount;
import com.github.sup3rmar10.ledger.Command.Transfer;
import com.github.sup3rmar10.ledger.Command.Withdraw;
import com.github.sup3rmar10.ledger.EventStore.NewEvent;
import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import com.github.sup3rmar10.ledger.Fixtures.MutableClock;
import com.github.sup3rmar10.ledger.Ledger.Outcome;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCompensated;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCredited;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferDebited;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

import static com.github.sup3rmar10.ledger.Fixtures.START;
import static com.github.sup3rmar10.ledger.Fixtures.dep;
import static com.github.sup3rmar10.ledger.Fixtures.one;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LedgerTest {
    @TempDir
    Path dir;

    private final MutableClock clock = new MutableClock(START);

    private Ledger ledger(EventStore store) {
        return new Ledger(store, clock, Ledger.SagaHooks.NONE, 5);
    }

    private static Ledger funded(EventStore store, Clock c) {
        Ledger l = new Ledger(store, c, Ledger.SagaHooks.NONE, 5);
        l.handle(new OpenAccount("o-a", "a", "alice"));
        l.handle(new OpenAccount("o-b", "b", "bob"));
        l.handle(new Deposit("d-a", "a", 100));
        l.handle(new Deposit("d-b", "b", 20));
        return l;
    }

    private static List<String> types(EventStore store, String stream) {
        return store.readStream(stream, 0).stream().map(e -> e.event().type()).toList();
    }

    @Test
    void happyPathUpdatesBalances() {
        InMemoryEventStore store = new InMemoryEventStore();
        Ledger l = funded(store, clock);
        assertEquals(Outcome.APPLIED, l.handle(new Withdraw("w1", "a", 30)).outcome());
        assertEquals(70, l.balance("a"));
        assertEquals(20, l.balance("b"));
        assertEquals(70, l.account("a").balance());
    }

    @Test
    void duplicateCommandIdIsAppliedOnlyOnce() {
        InMemoryEventStore store = new InMemoryEventStore();
        Ledger l = funded(store, clock);
        long seqBefore = store.lastSeq();
        assertEquals(Outcome.DUPLICATE, l.handle(new Deposit("d-a", "a", 100)).outcome());
        assertEquals(Outcome.DUPLICATE, l.handle(new Deposit("d-a", "a", 999)).outcome());
        assertEquals(100, l.balance("a"));
        assertEquals(seqBefore, store.lastSeq());
    }

    @Test
    void idempotencySurvivesARestartBecauseTheIndexIsRebuiltFromTheLog() throws IOException {
        Path file = dir.resolve("idem.jsonl");
        try (FileEventStore s = new FileEventStore(file, false)) {
            funded(s, clock);
        }
        try (FileEventStore s = new FileEventStore(file, false)) {
            Ledger restarted = ledger(s);
            assertEquals(Outcome.DUPLICATE, restarted.handle(new Deposit("d-a", "a", 100)).outcome());
            assertEquals(100, restarted.balance("a"));
        }
    }

    @Test
    void rejectedCommandWritesNothingAndTheSameIdMayBeRetried() {
        InMemoryEventStore store = new InMemoryEventStore();
        Ledger l = ledger(store);
        l.handle(new OpenAccount("o", "a", "alice"));
        long seq = store.lastSeq();
        Ledger.Result r = l.handle(new Withdraw("w", "a", 10));
        assertEquals(Outcome.REJECTED, r.outcome());
        assertEquals(seq, store.lastSeq());
        l.handle(new Deposit("d", "a", 10));
        assertEquals(Outcome.APPLIED, l.handle(new Withdraw("w", "a", 10)).outcome());
        assertEquals(0, l.balance("a"));
    }

    @Test
    void transferMovesCreditsAndWritesDebitThenCreditOnTheTwoStreams() {
        InMemoryEventStore store = new InMemoryEventStore();
        Ledger l = funded(store, clock);
        assertEquals(Outcome.APPLIED, l.handle(new Transfer("t1", "a", "b", 40)).outcome());
        assertEquals(60, l.balance("a"));
        assertEquals(60, l.balance("b"));
        assertTrue(types(store, "acct-a").contains("TransferDebited"));
        assertTrue(types(store, "acct-b").contains("TransferCredited"));
        // resubmitting the same transfer command does nothing
        assertEquals(Outcome.DUPLICATE, l.handle(new Transfer("t1", "a", "b", 40)).outcome());
        assertEquals(60, l.balance("a"));
        assertEquals(60, l.balance("b"));
    }

    @Test
    void transferWithInsufficientFundsIsRejectedAndWritesNoEvents() {
        InMemoryEventStore store = new InMemoryEventStore();
        Ledger l = funded(store, clock);
        long seq = store.lastSeq();
        assertEquals(Outcome.REJECTED, l.handle(new Transfer("t1", "b", "a", 21)).outcome());
        assertEquals(seq, store.lastSeq());
        assertEquals(Outcome.REJECTED, l.handle(new Transfer("t2", "a", "a", 1)).outcome());
        assertEquals(seq, store.lastSeq());
    }

    @Test
    void failedCreditCompensatesTheDebit() {
        InMemoryEventStore store = new InMemoryEventStore();
        Ledger l = funded(store, clock);
        Ledger.Result r = l.handle(new Transfer("t1", "a", "ghost", 40));
        assertEquals(Outcome.COMPENSATED, r.outcome());
        assertEquals(100, l.balance("a"));
        assertEquals(0, l.inFlightCredits());
        List<String> aTypes = types(store, "acct-a");
        assertEquals(List.of("AccountOpened", "CreditsDeposited", "TransferDebited", "TransferCompensated"), aTypes);
        assertEquals(0, store.streamVersion("acct-ghost"), "nothing may be created on the failed destination");
        TransferCompensated c = (TransferCompensated) store.readStream("acct-a", 3).get(0).event();
        assertTrue(c.reason().contains("not open"), c.reason());
    }

    @Test
    void crashAfterDebitIsFinishedByRecoveryAndRecoveryIsIdempotent() {
        InMemoryEventStore store = new InMemoryEventStore();
        funded(store, clock);
        Ledger crashing = new Ledger(store, clock, id -> {
            throw new Ledger.SimulatedCrash("boom " + id);
        }, 5);
        assertThrows(Ledger.SimulatedCrash.class, () -> crashing.handle(new Transfer("t1", "a", "b", 40)));

        Ledger restarted = ledger(store); // new process, same log
        assertEquals(60, restarted.balance("a"));
        assertEquals(20, restarted.balance("b"));
        assertEquals(40, restarted.inFlightCredits(), "debited but not yet credited");

        assertEquals(1, restarted.recoverSagas());
        assertEquals(60, restarted.balance("a"));
        assertEquals(60, restarted.balance("b"));
        assertEquals(0, restarted.inFlightCredits());
        assertEquals(0, restarted.recoverSagas(), "second recovery must find nothing to do");
        assertEquals(60, restarted.balance("b"), "no double credit");
        assertEquals(Outcome.DUPLICATE, restarted.handle(new Transfer("t1", "a", "b", 40)).outcome());
    }

    @Test
    void crashAfterDebitToAnUnknownAccountIsCompensatedByRecovery() {
        InMemoryEventStore store = new InMemoryEventStore();
        funded(store, clock);
        Ledger crashing = new Ledger(store, clock, id -> {
            throw new Ledger.SimulatedCrash("boom " + id);
        }, 5);
        assertThrows(Ledger.SimulatedCrash.class, () -> crashing.handle(new Transfer("t1", "a", "nobody", 40)));
        Ledger restarted = ledger(store);
        assertEquals(1, restarted.recoverSagas());
        assertEquals(100, restarted.balance("a"));
        assertEquals(0, restarted.inFlightCredits());
        assertEquals(List.of("AccountOpened", "CreditsDeposited", "TransferDebited", "TransferCompensated"),
                types(store, "acct-a"));
    }

    @Test
    void staleDecisionIsRetriedAgainstFreshStateSoNoOverdraftSneaksIn() {
        InMemoryEventStore inner = new InMemoryEventStore();
        ConflictingStore store = new ConflictingStore(inner);
        Ledger l = funded(store, clock); // a has 100
        // another writer withdraws 80 between our load and our append
        store.beforeNextAppend = () -> inner.append("acct-a", inner.streamVersion("acct-a"),
                one(new LedgerEvent.CreditsWithdrawn("a", 80), "other-writer"));
        Ledger.Result r = l.handle(new Withdraw("w", "a", 30));
        assertEquals(Outcome.REJECTED, r.outcome(), "20 left, 30 requested, after reload");
        assertEquals(20, l.balance("a"));
    }

    @Test
    void conflictingConcurrentDepositIsRetriedAndBothLand() {
        InMemoryEventStore inner = new InMemoryEventStore();
        ConflictingStore store = new ConflictingStore(inner);
        Ledger l = funded(store, clock);
        store.beforeNextAppend = () -> inner.append("acct-a", inner.streamVersion("acct-a"),
                one(dep("a", 50), "other-writer"));
        assertEquals(Outcome.APPLIED, l.handle(new Deposit("d2", "a", 7)).outcome());
        assertEquals(157, l.balance("a"));
        assertEquals(1, store.conflictsSeen);
    }

    @Test
    void demoScenarioUpholdsAllInvariants() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        boolean ok = Demo.run(dir, new PrintStream(buf, true, StandardCharsets.UTF_8));
        String out = buf.toString(StandardCharsets.UTF_8);
        assertTrue(ok, out);
        assertTrue(out.contains("RESULT: all invariants hold"), out);
        assertTrue(out.contains("torn bytes discarded on reopen: 27"), out);
    }

    /** Lets a test sneak another writer's append in just before the ledger's next append. */
    static final class ConflictingStore implements EventStore {
        private final EventStore delegate;
        Runnable beforeNextAppend;
        int conflictsSeen;

        ConflictingStore(EventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<StoredEvent> append(String stream, long expectedVersion, List<NewEvent> events) {
            Runnable r = beforeNextAppend;
            beforeNextAppend = null;
            if (r != null) {
                r.run();
            }
            try {
                return delegate.append(stream, expectedVersion, events);
            } catch (ConcurrencyException e) {
                conflictsSeen++;
                throw e;
            }
        }

        @Override
        public List<StoredEvent> readStream(String stream, long afterVersion) {
            return delegate.readStream(stream, afterVersion);
        }

        @Override
        public List<StoredEvent> readAll(long afterSeq) {
            return delegate.readAll(afterSeq);
        }

        @Override
        public long streamVersion(String stream) {
            return delegate.streamVersion(stream);
        }

        @Override
        public long lastSeq() {
            return delegate.lastSeq();
        }
    }
}
