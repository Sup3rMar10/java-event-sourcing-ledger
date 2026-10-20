package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.Command.Deposit;
import com.github.sup3rmar10.ledger.Command.OpenAccount;
import com.github.sup3rmar10.ledger.Command.Transfer;
import com.github.sup3rmar10.ledger.Command.Withdraw;
import com.github.sup3rmar10.ledger.Fixtures.MutableClock;
import com.github.sup3rmar10.ledger.Ledger.Outcome;
import com.github.sup3rmar10.ledger.Projections.StatementRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.github.sup3rmar10.ledger.Fixtures.DAY;
import static com.github.sup3rmar10.ledger.Fixtures.START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-style tests: a seeded RNG generates random command sequences; each is run against the real
 * Ledger and against a tiny independent model (a map of balances). Same seed => same run, so a
 * failure message with the seed is a reproducer.
 */
class PropertyTest {
    @TempDir
    Path dir;

    private static final String[] IDS = {"a0", "a1", "a2", "a3", "a4", "ghost"};

    private record Run(Ledger ledger, Map<String, Long> model, long netIssued) {
    }

    private static Run run(long seed, EventStore store, boolean crashes) {
        Random rnd = new Random(seed);
        MutableClock clock = new MutableClock(START);
        int every = rnd.nextInt(9); // 0 disables snapshots
        Ledger.SagaHooks hooks = id -> {
            if (crashes && rnd.nextInt(10) < 3) {
                throw new Ledger.SimulatedCrash("crash " + id);
            }
        };
        Ledger ledger = new Ledger(store, clock, hooks, every);
        Map<String, Long> model = new HashMap<>(); // key present == account opened
        List<Command> accepted = new ArrayList<>();
        long net = 0;
        for (int i = 0; i < 30; i++) {
            clock.advance(rnd.nextInt(3) * (DAY / 2));
            String cmd = "s" + seed + "-" + i;
            String id = IDS[rnd.nextInt(IDS.length)];
            long amt = 1 + rnd.nextInt(120);
            int kind = rnd.nextInt(10);
            Command c;
            Outcome expected;
            if (kind == 0 && !accepted.isEmpty()) {
                c = accepted.get(rnd.nextInt(accepted.size()));
                expected = Outcome.DUPLICATE;
            } else if (kind <= 2) {
                c = new OpenAccount(cmd, id, "owner");
                expected = model.containsKey(id) ? Outcome.REJECTED : Outcome.APPLIED;
                if (expected == Outcome.APPLIED) {
                    model.put(id, 0L);
                }
            } else if (kind <= 5) {
                c = new Deposit(cmd, id, amt);
                expected = model.containsKey(id) ? Outcome.APPLIED : Outcome.REJECTED;
                if (expected == Outcome.APPLIED) {
                    model.merge(id, amt, Long::sum);
                    net += amt;
                }
            } else if (kind <= 7) {
                c = new Withdraw(cmd, id, amt);
                expected = model.containsKey(id) && amt <= model.get(id) ? Outcome.APPLIED : Outcome.REJECTED;
                if (expected == Outcome.APPLIED) {
                    model.merge(id, -amt, Long::sum);
                    net -= amt;
                }
            } else {
                String to = IDS[rnd.nextInt(IDS.length)];
                c = new Transfer(cmd, id, to, amt);
                if (id.equals(to) || !model.containsKey(id) || model.get(id) < amt) {
                    expected = Outcome.REJECTED;
                } else if (model.containsKey(to)) {
                    expected = Outcome.APPLIED;
                    model.merge(id, -amt, Long::sum);
                    model.merge(to, amt, Long::sum);
                } else {
                    expected = Outcome.COMPENSATED; // credit refused, debit given back: no net change
                }
            }
            String where = "seed=" + seed + " step=" + i + " cmd=" + c;
            Ledger.Result r = null;
            try {
                r = ledger.handle(c);
            } catch (Ledger.SimulatedCrash e) {
                ledger = new Ledger(store, clock, hooks, every); // "restart": fresh process, same log, no snapshots
                ledger.recoverSagas();
                assertEquals(0, ledger.projections().saga().pending().size(), where);
            }
            if (r != null) {
                assertEquals(expected, r.outcome(), where + " detail=" + r.detail());
            }
            if (expected != Outcome.REJECTED && expected != Outcome.DUPLICATE) {
                accepted.add(c);
            }
            if (i == 15) {
                assertEquals(Projections.rebuild(store).state(), ledger.projections().state(), "mid-run " + where);
            }
        }
        ledger.recoverSagas();
        return new Run(ledger, model, net);
    }

    private static void verify(long seed, Run r, EventStore store) {
        Ledger l = r.ledger();
        String where = "seed=" + seed;
        assertEquals(Projections.rebuild(store).state(), l.projections().state(), "rebuild != incremental, " + where);
        long total = 0;
        for (String id : IDS) {
            long expected = r.model().getOrDefault(id, 0L);
            assertEquals(expected, l.balance(id), where + " balance " + id);
            assertEquals(l.accountFullReplay(id), l.account(id), where + " snapshot+tail != replay " + id);
            assertEquals(r.model().containsKey(id), l.account(id).open(), where + " open " + id);
            assertTrue(expected >= 0, where);
            total += expected;
            List<StatementRow> st = l.statement(id);
            if (!st.isEmpty()) {
                assertEquals(expected, st.get(st.size() - 1).closingBalance(), where + " statement " + id);
            }
        }
        assertEquals(r.netIssued(), total, "credits not conserved, " + where);
        assertEquals(r.netIssued(), l.projections().balances().total(), where);
        assertEquals(0, l.inFlightCredits(), where);
    }

    @Test
    void threeThousandRandomSequencesMatchTheModelAndRebuildEqualsIncremental() {
        for (long seed = 0; seed < 3000; seed++) {
            InMemoryEventStore store = new InMemoryEventStore();
            verify(seed, run(seed, store, false), store);
        }
    }

    @Test
    void randomCrashesBetweenSagaStepsNeverLoseOrCreateCreditsAfterRecovery() {
        for (long seed = 10_000; seed < 10_800; seed++) {
            InMemoryEventStore store = new InMemoryEventStore();
            verify(seed, run(seed, store, true), store);
        }
    }

    @Test
    void sameThroughTheFileStoreIncludingReopen() {
        for (long seed = 20_000; seed < 20_040; seed++) {
            Path file = dir.resolve("p" + seed + ".jsonl");
            Map<String, Object> liveState;
            try (FileEventStore store = new FileEventStore(file, false)) {
                Run r = run(seed, store, seed % 2 == 0);
                verify(seed, r, store);
                liveState = r.ledger().projections().state();
            }
            try (FileEventStore reopened = new FileEventStore(file, false)) {
                assertNotNull(liveState);
                assertEquals(liveState, Projections.rebuild(reopened).state(), "reopen changed state, seed=" + seed);
            }
        }
    }
}
