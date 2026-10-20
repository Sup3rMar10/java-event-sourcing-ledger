package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.NewEvent;
import com.github.sup3rmar10.ledger.EventStore.StoredEvent;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads and saves Account aggregates. Every snapshotEvery-th event of a stream triggers an
 * in-memory snapshot of the aggregate state; load() = latest snapshot + the tail after its version.
 * snapshotEvery <= 0 disables snapshots. Snapshots are an optimisation only: dropping them never
 * changes any result (tests assert snapshot + tail == full replay).
 */
public final class AccountRepository {
    private final EventStore store;
    private final int snapshotEvery;
    private final Map<String, Account> snapshots = new ConcurrentHashMap<>();

    public AccountRepository(EventStore store, int snapshotEvery) {
        this.store = store;
        this.snapshotEvery = snapshotEvery;
    }

    public static String stream(String accountId) {
        return "acct-" + accountId;
    }

    public Account load(String id) {
        Account a = snapshots.getOrDefault(id, Account.empty(id));
        for (StoredEvent e : store.readStream(stream(id), a.version())) {
            a = a.apply(e.event());
        }
        return a;
    }

    /** Ignores snapshots: the reference answer used to check them. */
    public Account loadFullReplay(String id) {
        Account a = Account.empty(id);
        for (StoredEvent e : store.readStream(stream(id), 0)) {
            a = a.apply(e.event());
        }
        return a;
    }

    public Optional<Account> snapshot(String id) {
        return Optional.ofNullable(snapshots.get(id));
    }

    /**
     * Appends one event with expectedVersion = current.version() (optimistic concurrency) and returns
     * the next state. Throws ConcurrencyException if someone else appended in between.
     */
    public Account save(Account current, LedgerEvent event, String commandId, long timestampMillis) {
        store.append(stream(current.id()), current.version(),
                List.of(new NewEvent(event, commandId, timestampMillis)));
        Account next = current.apply(event);
        if (snapshotEvery > 0 && next.version() % snapshotEvery == 0) {
            snapshots.put(next.id(), next);
        }
        return next;
    }
}
