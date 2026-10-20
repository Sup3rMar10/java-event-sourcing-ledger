package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.LedgerEvent.AccountOpened;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsDeposited;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsWithdrawn;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCompensated;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCredited;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferDebited;

/**
 * The Account aggregate: immutable state plus decide* (command -> event, may refuse) and
 * apply (event -> next state, never refuses). version = number of events applied.
 *
 * apply() uses instanceof pattern matching over the sealed LedgerEvent. A switch with type
 * patterns would give compile-time exhaustiveness but is only a preview feature on JDK 17
 * (final in 21), and this project targets --release 17 and runs on 17 and 21.
 */
public record Account(String id, boolean open, long balance, long version) {

    public static Account empty(String id) {
        return new Account(id, false, 0, 0);
    }

    public Account apply(LedgerEvent e) {
        long next = version + 1;
        if (e instanceof AccountOpened o) {
            return new Account(o.accountId(), true, 0, next);
        }
        return new Account(id, open, balance + delta(e), next);
    }

    /** Signed effect of an event on its account's balance. Shared with the projections. */
    public static long delta(LedgerEvent e) {
        if (e instanceof AccountOpened) {
            return 0;
        } else if (e instanceof CreditsDeposited d) {
            return d.amount();
        } else if (e instanceof CreditsWithdrawn w) {
            return -w.amount();
        } else if (e instanceof TransferDebited t) {
            return -t.amount();
        } else if (e instanceof TransferCredited t) {
            return t.amount();
        } else if (e instanceof TransferCompensated t) {
            return t.amount();
        }
        throw new IllegalStateException("unhandled event " + e.type());
    }

    public LedgerEvent decideOpen(String owner) {
        if (id == null || id.isBlank()) {
            throw new DomainException("account id must not be blank");
        }
        if (open) {
            throw new DomainException("account " + id + " is already open");
        }
        return new AccountOpened(id, owner, EventCodec.DEFAULT_CURRENCY);
    }

    public LedgerEvent decideDeposit(long amount) {
        requireOpen();
        requirePositive(amount);
        if (balance > Long.MAX_VALUE - amount) {
            throw new DomainException("balance would overflow");
        }
        return new CreditsDeposited(id, amount);
    }

    public LedgerEvent decideWithdraw(long amount) {
        requireOpen();
        requirePositive(amount);
        requireFunds(amount);
        return new CreditsWithdrawn(id, amount);
    }

    public LedgerEvent decideDebit(String transferId, String to, long amount) {
        requireOpen();
        requirePositive(amount);
        requireFunds(amount);
        return new TransferDebited(id, transferId, to, amount);
    }

    public LedgerEvent decideCredit(String transferId, String from, long amount) {
        requireOpen();
        requirePositive(amount);
        return new TransferCredited(id, transferId, from, amount);
    }

    public LedgerEvent decideCompensate(String transferId, long amount, String reason) {
        requireOpen();
        return new TransferCompensated(id, transferId, amount, reason);
    }

    private void requireOpen() {
        if (!open) {
            throw new DomainException("account " + id + " is not open");
        }
    }

    private static void requirePositive(long amount) {
        if (amount <= 0) {
            throw new DomainException("amount must be positive");
        }
    }

    private void requireFunds(long amount) {
        if (amount > balance) {
            throw new DomainException("insufficient credits in " + id + ": balance " + balance + ", requested " + amount);
        }
    }
}
