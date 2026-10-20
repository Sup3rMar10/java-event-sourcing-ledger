package com.github.sup3rmar10.ledger;

/** Requests to change state. Every command carries a client-chosen commandId used for idempotency. */
public sealed interface Command {

    String commandId();

    record OpenAccount(String commandId, String accountId, String owner) implements Command {
    }

    record Deposit(String commandId, String accountId, long amount) implements Command {
    }

    record Withdraw(String commandId, String accountId, long amount) implements Command {
    }

    /** The commandId doubles as the transfer (saga) id. */
    record Transfer(String commandId, String from, String to, long amount) implements Command {
    }
}
