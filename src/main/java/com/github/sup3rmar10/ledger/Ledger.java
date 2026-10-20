package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.ConcurrencyException;
import com.github.sup3rmar10.ledger.Projections.Pending;
import com.github.sup3rmar10.ledger.Projections.StatementRow;

import java.time.Clock;
import java.util.List;
import java.util.function.Function;

/**
 * Command side + read side wired together for one process (single writer).
 *
 * Idempotency: every stored event carries its commandId; a command whose id is already in the
 * log is answered DUPLICATE and not executed again (survives restarts because the index is a
 * projection of the log). Rejected commands write nothing, so they may be retried.
 *
 * Transfer saga: (1) debit source, (2) credit destination; if (2) is refused, (3) compensate the
 * source. A crash between (1) and (2) leaves a pending transfer in the log; recoverSagas() finishes it.
 * The destination is only checked at step 2, on purpose, so the compensation path is exercised.
 */
public final class Ledger {

    public enum Outcome { APPLIED, DUPLICATE, REJECTED, COMPENSATED }

    public record Result(Outcome outcome, String detail) {
    }

    /** Test/demo seam to inject a crash between saga steps. */
    @FunctionalInterface
    public interface SagaHooks {
        SagaHooks NONE = id -> {
        };

        void afterDebit(String transferId);
    }

    /** Thrown by hooks to model a process dying; the Ledger deliberately does not catch it. */
    public static final class SimulatedCrash extends RuntimeException {
        public SimulatedCrash(String message) {
            super(message);
        }
    }

    static final int MAX_RETRIES = 5;

    private final EventStore store;
    private final java.time.Clock clock;
    private final SagaHooks hooks;
    private final AccountRepository repo;
    private final Projections projections = new Projections();

    public Ledger(EventStore store) {
        this(store, Clock.systemUTC(), SagaHooks.NONE, 50);
    }

    public Ledger(EventStore store, Clock clock, SagaHooks hooks, int snapshotEvery) {
        this.store = store;
        this.clock = clock;
        this.hooks = hooks;
        this.repo = new AccountRepository(store, snapshotEvery);
    }

    public synchronized Result handle(Command c) {
        projections.catchUp(store);
        if (projections.saga().seen(c.commandId())) {
            return new Result(Outcome.DUPLICATE, "command " + c.commandId() + " already processed");
        }
        try {
            Result r = dispatch(c);
            projections.catchUp(store);
            return r;
        } catch (DomainException e) {
            projections.catchUp(store);
            return new Result(Outcome.REJECTED, e.getMessage());
        }
    }

    private Result dispatch(Command c) {
        if (c instanceof Command.OpenAccount o) {
            step(o.accountId(), o.commandId(), a -> a.decideOpen(o.owner()));
            return new Result(Outcome.APPLIED, "account opened");
        } else if (c instanceof Command.Deposit d) {
            step(d.accountId(), d.commandId(), a -> a.decideDeposit(d.amount()));
            return new Result(Outcome.APPLIED, "deposited");
        } else if (c instanceof Command.Withdraw w) {
            step(w.accountId(), w.commandId(), a -> a.decideWithdraw(w.amount()));
            return new Result(Outcome.APPLIED, "withdrawn");
        } else if (c instanceof Command.Transfer t) {
            return transfer(t);
        }
        throw new IllegalStateException("unhandled command " + c);
    }

    private Result transfer(Command.Transfer t) {
        if (t.from().equals(t.to())) {
            throw new DomainException("cannot transfer to the same account");
        }
        step(t.from(), t.commandId(), a -> a.decideDebit(t.commandId(), t.to(), t.amount()));
        hooks.afterDebit(t.commandId());
        return completeTransfer(new Pending(t.commandId(), t.from(), t.to(), t.amount()));
    }

    private Result completeTransfer(Pending p) {
        try {
            step(p.to(), p.transferId(), a -> a.decideCredit(p.transferId(), p.from(), p.amount()));
            return new Result(Outcome.APPLIED, "transfer completed");
        } catch (DomainException e) {
            String reason = e.getMessage();
            step(p.from(), p.transferId(), a -> a.decideCompensate(p.transferId(), p.amount(), reason));
            return new Result(Outcome.COMPENSATED, "credit failed (" + reason + "); debit compensated");
        }
    }

    /** Load aggregate, decide, append with expectedVersion; on a version conflict reload and decide again. */
    private void step(String accountId, String commandId, Function<Account, LedgerEvent> decide) {
        for (int attempt = 0; ; attempt++) {
            Account a = repo.load(accountId);
            LedgerEvent event = decide.apply(a);
            try {
                repo.save(a, event, commandId, clock.millis());
                return;
            } catch (ConcurrencyException e) {
                if (attempt >= MAX_RETRIES) {
                    throw e;
                }
            }
        }
    }

    /** Finishes every transfer that was debited but never credited/compensated. Returns how many. */
    public synchronized int recoverSagas() {
        projections.catchUp(store);
        List<Pending> todo = projections.saga().pending();
        for (Pending p : todo) {
            completeTransfer(p);
        }
        projections.catchUp(store);
        return todo.size();
    }

    public synchronized long balance(String accountId) {
        projections.catchUp(store);
        return projections.balances().balance(accountId);
    }

    public synchronized List<StatementRow> statement(String accountId) {
        projections.catchUp(store);
        return projections.statements().statement(accountId);
    }

    public synchronized long totalBalance() {
        projections.catchUp(store);
        return projections.balances().total();
    }

    public synchronized long inFlightCredits() {
        projections.catchUp(store);
        return projections.saga().inFlightCredits();
    }

    /** Aggregate state loaded through snapshots. */
    public Account account(String accountId) {
        return repo.load(accountId);
    }

    /** Aggregate state by full replay (reference for checking snapshots). */
    public Account accountFullReplay(String accountId) {
        return repo.loadFullReplay(accountId);
    }

    public Projections projections() {
        return projections;
    }
}
