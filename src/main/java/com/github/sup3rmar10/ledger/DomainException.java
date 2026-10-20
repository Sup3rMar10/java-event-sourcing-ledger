package com.github.sup3rmar10.ledger;

/** A business rule refused a command (for example overdraft). Nothing is written when this is thrown. */
public final class DomainException extends RuntimeException {
    public DomainException(String message) {
        super(message);
    }
}
