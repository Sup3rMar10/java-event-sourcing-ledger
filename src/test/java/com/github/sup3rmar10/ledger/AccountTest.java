package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.LedgerEvent.AccountOpened;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsDeposited;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsWithdrawn;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCompensated;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCredited;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferDebited;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccountTest {

    private static Account opened() {
        Account a = Account.empty("a");
        return a.apply(a.decideOpen("alice"));
    }

    private static Account withBalance(long credits) {
        Account a = opened();
        return a.apply(a.decideDeposit(credits));
    }

    @Test
    void openThenDepositAndWithdrawFoldsToTheRightState() {
        Account a = Account.empty("a");
        a = a.apply(new AccountOpened("a", "alice", "CRD"));
        a = a.apply(new CreditsDeposited("a", 100));
        a = a.apply(new CreditsWithdrawn("a", 30));
        assertEquals(new Account("a", true, 70, 3), a);
    }

    @Test
    void applyAdvancesVersionAndHandlesEverySagaEvent() {
        Account a = withBalance(100); // version 2
        a = a.apply(new TransferDebited("a", "t", "b", 40));
        assertEquals(60, a.balance());
        a = a.apply(new TransferCompensated("a", "t", 40, "x"));
        assertEquals(100, a.balance());
        a = a.apply(new TransferCredited("a", "t2", "b", 5));
        assertEquals(105, a.balance());
        assertEquals(5, a.version());
    }

    @Test
    void openingTwiceIsRejected() {
        assertThrows(DomainException.class, () -> opened().decideOpen("again"));
    }

    @Test
    void blankAccountIdIsRejected() {
        assertThrows(DomainException.class, () -> Account.empty(" ").decideOpen("x"));
    }

    @Test
    void operationsOnAnUnopenedAccountAreRejected() {
        Account a = Account.empty("ghost");
        assertFalse(a.open());
        assertThrows(DomainException.class, () -> a.decideDeposit(1));
        assertThrows(DomainException.class, () -> a.decideWithdraw(1));
        assertThrows(DomainException.class, () -> a.decideCredit("t", "x", 1));
        assertThrows(DomainException.class, () -> a.decideDebit("t", "x", 1));
    }

    @Test
    void nonPositiveAmountsAreRejected() {
        Account a = withBalance(10);
        for (long bad : new long[] {0, -1, Long.MIN_VALUE}) {
            assertThrows(DomainException.class, () -> a.decideDeposit(bad), "deposit " + bad);
            assertThrows(DomainException.class, () -> a.decideWithdraw(bad), "withdraw " + bad);
            assertThrows(DomainException.class, () -> a.decideCredit("t", "x", bad), "credit " + bad);
        }
    }

    @Test
    void overdraftIsRejectedForWithdrawAndForTransferDebit() {
        Account a = withBalance(50);
        DomainException e = assertThrows(DomainException.class, () -> a.decideWithdraw(51));
        assertTrue(e.getMessage().contains("insufficient"), e.getMessage());
        assertThrows(DomainException.class, () -> a.decideDebit("t", "b", 51));
    }

    @Test
    void withdrawingTheExactBalanceIsAllowedAndLeavesZero() {
        Account a = withBalance(50);
        a = a.apply(a.decideWithdraw(50));
        assertEquals(0, a.balance());
        Account b = withBalance(50);
        b = b.apply(b.decideDebit("t", "x", 50));
        assertEquals(0, b.balance());
    }

    @Test
    void depositThatWouldOverflowIsRejected() {
        Account a = new Account("a", true, Long.MAX_VALUE - 5, 2);
        assertThrows(DomainException.class, () -> a.decideDeposit(6));
        assertEquals(new CreditsDeposited("a", 5), a.decideDeposit(5));
    }
}
