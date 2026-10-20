package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.NewEvent;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

final class Fixtures {
    static final long DAY = 86_400_000L;
    static final long START = LocalDate.of(2026, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

    private Fixtures() {
    }

    static final class MutableClock extends Clock {
        private long millis;

        MutableClock(long millis) {
            this.millis = millis;
        }

        void advance(long ms) {
            millis += ms;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }

    static List<NewEvent> one(LedgerEvent e, String cmd) {
        return List.of(new NewEvent(e, cmd, START));
    }

    static LedgerEvent.CreditsDeposited dep(String acct, long amount) {
        return new LedgerEvent.CreditsDeposited(acct, amount);
    }

    static Command.OpenAccount open(String cmd, String acct) {
        return new Command.OpenAccount(cmd, acct, "owner-" + acct);
    }

    static Command.Deposit deposit(String cmd, String acct, long amt) {
        return new Command.Deposit(cmd, acct, amt);
    }
}
