package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.Command.Deposit;
import com.github.sup3rmar10.ledger.Command.OpenAccount;
import com.github.sup3rmar10.ledger.Command.Transfer;
import com.github.sup3rmar10.ledger.Command.Withdraw;
import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import com.github.sup3rmar10.ledger.Fixtures.MutableClock;
import com.github.sup3rmar10.ledger.Projections.StatementRow;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static com.github.sup3rmar10.ledger.Fixtures.DAY;
import static com.github.sup3rmar10.ledger.Fixtures.START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProjectionsTest {
    private final MutableClock clock = new MutableClock(START);
    private final InMemoryEventStore store = new InMemoryEventStore();
    private final Ledger ledger = new Ledger(store, clock, Ledger.SagaHooks.NONE, 3);

    private void scriptedScenario() {
        ledger.handle(new OpenAccount("o1", "a", "alice"));
        ledger.handle(new OpenAccount("o2", "b", "bob"));
        ledger.handle(new Deposit("d1", "a", 100));
        ledger.handle(new Withdraw("w1", "a", 30));
        clock.advance(DAY);
        ledger.handle(new Deposit("d2", "a", 5));
        ledger.handle(new Transfer("t1", "a", "b", 25));
        clock.advance(DAY + 1);
        ledger.handle(new Transfer("t2", "a", "ghost", 10));
        ledger.handle(new Deposit("d3", "b", 1));
    }

    @Test
    void balanceViewMatchesTheAggregates() {
        scriptedScenario();
        for (String id : List.of("a", "b", "ghost")) {
            assertEquals(ledger.account(id).balance(), ledger.projections().balances().balance(id), id);
        }
        assertEquals(50, ledger.balance("a"));
        assertEquals(26, ledger.balance("b"));
        assertEquals(76, ledger.projections().balances().total());
    }

    @Test
    void statementGroupsByUtcDayWithRunningClosingBalance() {
        clock.advance(10_000); // not midnight, to check day bucketing rather than exact ms
        ledger.handle(new OpenAccount("o", "a", "alice"));
        ledger.handle(new Deposit("d1", "a", 100));
        ledger.handle(new Withdraw("w1", "a", 30));
        clock.advance(DAY);
        ledger.handle(new Deposit("d2", "a", 5));
        LocalDate d1 = LocalDate.of(2026, 1, 1);
        assertEquals(List.of(
                new StatementRow(d1, 100, 30, 2, 70),
                new StatementRow(d1.plusDays(1), 5, 0, 1, 75)), ledger.statement("a"));
        assertEquals(List.of(), ledger.statement("nobody"));
    }

    @Test
    void transferDebitsAndCompensationsAppearInTheStatement() {
        scriptedScenario();
        List<StatementRow> a = ledger.statement("a");
        // day 1: +100 -30 ; day 2: +5 -25 ; day 3: -10 (debit) +10 (compensation)
        assertEquals(3, a.size());
        assertEquals(new StatementRow(LocalDate.of(2026, 1, 3), 10, 10, 2, 50), a.get(2));
        assertEquals(50, a.get(2).closingBalance());
    }

    @Test
    void rebuildFromScratchEqualsIncrementallyMaintainedProjections() {
        scriptedScenario();
        assertEquals(Projections.rebuild(store).state(), ledger.projections().state());
    }

    @Test
    void foldingEventsOneAtATimeEqualsFoldingInOneGo() {
        scriptedScenario();
        Projections stepwise = new Projections();
        for (StoredEvent e : store.readAll(0)) {
            stepwise.on(e);
            if (e.seq() % 3 == 0) {
                stepwise.catchUp(store); // catching up mid-way must not double count
                break;
            }
        }
        stepwise.catchUp(store);
        stepwise.catchUp(store); // idempotent when nothing is new
        assertEquals(Projections.rebuild(store).state(), stepwise.state());
        assertEquals(store.lastSeq(), stepwise.lastSeq());
    }

    @Test
    void sagaIndexTracksPendingTransfersAndSeenCommands() {
        InMemoryEventStore s = new InMemoryEventStore();
        Projections p = new Projections();
        s.append("acct-a", 0, Fixtures.one(new LedgerEvent.TransferDebited("a", "t1", "b", 9), "t1"));
        p.catchUp(s);
        assertTrue(p.saga().seen("t1"));
        assertFalse(p.saga().seen("other"));
        assertEquals(1, p.saga().pending().size());
        assertEquals(9, p.saga().inFlightCredits());
        s.append("acct-b", 0, Fixtures.one(new LedgerEvent.TransferCredited("b", "t1", "a", 9), "t1"));
        p.catchUp(s);
        assertEquals(0, p.saga().pending().size());
        assertEquals(0, p.saga().inFlightCredits());
    }
}
