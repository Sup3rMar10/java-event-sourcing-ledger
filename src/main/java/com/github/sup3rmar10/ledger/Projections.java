package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCompensated;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCredited;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferDebited;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Read models, each a pure fold over the global event log: balance view, per-day statement, and a
 * saga/idempotency index (which commandIds were seen, which transfers are debited but unsettled).
 * Being pure folds is what makes "rebuild from scratch == incrementally maintained" a testable property.
 */
public final class Projections {

    public interface Projection {
        void on(StoredEvent e);
    }

    /** A transfer whose debit is stored but whose credit/compensation is not. */
    public record Pending(String transferId, String from, String to, long amount) {
    }

    public record StatementRow(LocalDate day, long credited, long debited, int entries, long closingBalance) {
    }

    public static final class BalanceView implements Projection {
        private final Map<String, Long> balances = new TreeMap<>();

        @Override
        public void on(StoredEvent e) {
            balances.merge(e.event().accountId(), Account.delta(e.event()), Long::sum);
        }

        public long balance(String accountId) {
            return balances.getOrDefault(accountId, 0L);
        }

        public long total() {
            return balances.values().stream().mapToLong(Long::longValue).sum();
        }

        public Map<String, Long> state() {
            return new TreeMap<>(balances);
        }
    }

    /** Per account, per UTC day: credits in, credits out, number of entries, closing balance. */
    public static final class StatementView implements Projection {
        // account -> day -> [credited, debited, entries]
        private final Map<String, TreeMap<LocalDate, long[]>> days = new TreeMap<>();

        @Override
        public void on(StoredEvent e) {
            long delta = Account.delta(e.event());
            if (delta == 0) {
                return;
            }
            LocalDate day = Instant.ofEpochMilli(e.timestampMillis()).atZone(ZoneOffset.UTC).toLocalDate();
            long[] t = days.computeIfAbsent(e.event().accountId(), k -> new TreeMap<>())
                    .computeIfAbsent(day, k -> new long[3]);
            if (delta > 0) {
                t[0] += delta;
            } else {
                t[1] -= delta;
            }
            t[2]++;
        }

        public List<StatementRow> statement(String accountId) {
            List<StatementRow> rows = new ArrayList<>();
            long running = 0;
            for (Map.Entry<LocalDate, long[]> d : days.getOrDefault(accountId, new TreeMap<>()).entrySet()) {
                long[] t = d.getValue();
                running += t[0] - t[1];
                rows.add(new StatementRow(d.getKey(), t[0], t[1], (int) t[2], running));
            }
            return rows;
        }

        public Map<String, List<StatementRow>> state() {
            Map<String, List<StatementRow>> m = new TreeMap<>();
            for (String id : days.keySet()) {
                m.put(id, statement(id));
            }
            return m;
        }
    }

    public static final class SagaIndex implements Projection {
        private final Set<String> commands = new HashSet<>();
        private final Map<String, Pending> pending = new LinkedHashMap<>();

        @Override
        public void on(StoredEvent e) {
            commands.add(e.commandId());
            if (e.event() instanceof TransferDebited d) {
                pending.put(d.transferId(), new Pending(d.transferId(), d.accountId(), d.to(), d.amount()));
            } else if (e.event() instanceof TransferCredited c) {
                pending.remove(c.transferId());
            } else if (e.event() instanceof TransferCompensated c) {
                pending.remove(c.transferId());
            }
        }

        public boolean seen(String commandId) {
            return commands.contains(commandId);
        }

        public List<Pending> pending() {
            return new ArrayList<>(pending.values());
        }

        public long inFlightCredits() {
            return pending.values().stream().mapToLong(Pending::amount).sum();
        }
    }

    private final BalanceView balances = new BalanceView();
    private final StatementView statements = new StatementView();
    private final SagaIndex saga = new SagaIndex();
    private long lastSeq;

    public BalanceView balances() {
        return balances;
    }

    public StatementView statements() {
        return statements;
    }

    public SagaIndex saga() {
        return saga;
    }

    public long lastSeq() {
        return lastSeq;
    }

    public void on(StoredEvent e) {
        balances.on(e);
        statements.on(e);
        saga.on(e);
        lastSeq = e.seq();
    }

    /** Incremental maintenance: fold in only the events appended since the last call. */
    public void catchUp(EventStore store) {
        for (StoredEvent e : store.readAll(lastSeq)) {
            on(e);
        }
    }

    /** From-scratch rebuild: fold the whole log into a fresh set of projections. */
    public static Projections rebuild(EventStore store) {
        Projections p = new Projections();
        p.catchUp(store);
        return p;
    }

    /** Comparable snapshot of everything, for equality checks in tests. */
    public Map<String, Object> state() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("lastSeq", lastSeq);
        m.put("balances", balances.state());
        m.put("statements", statements.state());
        m.put("commands", new TreeSet<>(saga.commands));
        m.put("pending", new LinkedHashMap<>(saga.pending));
        return m;
    }
}
