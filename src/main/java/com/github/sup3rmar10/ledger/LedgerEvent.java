package com.github.sup3rmar10.ledger;

/**
 * Facts that happened to an account. All amounts are whole "credits" of a fictional
 * internal-credits system (not money). Nested records are the only permitted subtypes
 * (same compilation unit), so the hierarchy is closed.
 */
public sealed interface LedgerEvent {

    String accountId();

    default String type() {
        return getClass().getSimpleName();
    }

    record AccountOpened(String accountId, String owner, String currency) implements LedgerEvent {
    }

    record CreditsDeposited(String accountId, long amount) implements LedgerEvent {
    }

    record CreditsWithdrawn(String accountId, long amount) implements LedgerEvent {
    }

    /** Saga step 1, on the source account's stream. */
    record TransferDebited(String accountId, String transferId, String to, long amount) implements LedgerEvent {
    }

    /** Saga step 2, on the destination account's stream. */
    record TransferCredited(String accountId, String transferId, String from, long amount) implements LedgerEvent {
    }

    /** Saga compensation, on the source account's stream: gives the debited credits back. */
    record TransferCompensated(String accountId, String transferId, long amount, String reason)
            implements LedgerEvent {
    }
}
