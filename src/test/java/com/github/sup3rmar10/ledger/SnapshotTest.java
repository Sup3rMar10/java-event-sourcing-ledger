package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.NewEvent;
import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotTest {

    /** Builds an account with `events` total events (1 open + deposits/withdrawals). */
    private static Account build(AccountRepository repo, int events, Random rnd) {
        Account a = repo.save(Account.empty("x"), new LedgerEvent.AccountOpened("x", "o", "CRD"), "c0", 1L);
        for (int i = 1; i < events; i++) {
            LedgerEvent ev = (a.balance() > 5 && rnd.nextBoolean())
                    ? a.decideWithdraw(1 + rnd.nextInt((int) Math.min(5, a.balance())))
                    : a.decideDeposit(1 + rnd.nextInt(9));
            a = repo.save(a, ev, "c" + i, 1L);
        }
        return a;
    }

    @Test
    void snapshotIsTakenAtEveryNthEventWithMatchingVersion() {
        AccountRepository repo = new AccountRepository(new InMemoryEventStore(), 4);
        Account a = repo.save(Account.empty("x"), new LedgerEvent.AccountOpened("x", "o", "CRD"), "c0", 1L);
        for (int i = 1; i <= 9; i++) {
            a = repo.save(a, a.decideDeposit(1), "c" + i, 1L); // versions 2..10
            long expected = (a.version() / 4) * 4;
            if (expected == 0) {
                assertTrue(repo.snapshot("x").isEmpty(), "no snapshot before event 4, version " + a.version());
            } else {
                assertEquals(expected, repo.snapshot("x").orElseThrow().version(), "at version " + a.version());
            }
        }
        // the snapshot's state must describe exactly the events up to its own version
        Account snap = repo.snapshot("x").orElseThrow();
        assertEquals(8, snap.version());
        assertEquals(7, snap.balance(), "1 open + 7 deposits of 1 credit");
    }

    @Test
    void snapshotsAreDisabledWhenEveryIsZeroOrNegative() {
        for (int every : new int[] {0, -3}) {
            AccountRepository repo = new AccountRepository(new InMemoryEventStore(), every);
            build(repo, 30, new Random(1));
            assertTrue(repo.snapshot("x").isEmpty());
        }
    }

    @Test
    void snapshotPlusTailEqualsFullReplayForManyLengthsAndIntervals() {
        for (int every = 1; every <= 12; every++) {
            for (int events = 1; events <= 40; events++) {
                AccountRepository repo = new AccountRepository(new InMemoryEventStore(), every);
                Account built = build(repo, events, new Random(every * 1000L + events));
                assertEquals(repo.loadFullReplay("x"), repo.load("x"), "every=" + every + " events=" + events);
                assertEquals(built, repo.load("x"));
            }
        }
    }

    @Test
    void loadReadsOnlyTheTailAfterTheSnapshot() {
        List<Long> afterVersions = new ArrayList<>();
        InMemoryEventStore inner = new InMemoryEventStore();
        EventStore recording = new EventStore() {
            @Override
            public List<StoredEvent> append(String s, long v, List<NewEvent> e) {
                return inner.append(s, v, e);
            }

            @Override
            public List<StoredEvent> readStream(String s, long afterVersion) {
                afterVersions.add(afterVersion);
                return inner.readStream(s, afterVersion);
            }

            @Override
            public List<StoredEvent> readAll(long a) {
                return inner.readAll(a);
            }

            @Override
            public long streamVersion(String s) {
                return inner.streamVersion(s);
            }

            @Override
            public long lastSeq() {
                return inner.lastSeq();
            }
        };
        AccountRepository repo = new AccountRepository(recording, 10);
        build(repo, 25, new Random(7)); // snapshot at version 20, tail of 5
        afterVersions.clear();
        Account loaded = repo.load("x");
        assertEquals(List.of(20L), afterVersions);
        assertEquals(25, loaded.version());
    }
}
