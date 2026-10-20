package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.Command.Deposit;
import com.github.sup3rmar10.ledger.Command.OpenAccount;
import com.github.sup3rmar10.ledger.Command.Transfer;
import com.github.sup3rmar10.ledger.Command.Withdraw;
import com.github.sup3rmar10.ledger.Projections.StatementRow;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.Map;

/**
 * Scenario: normal traffic, a duplicate command, a rejected overdraft, a transfer that is compensated,
 * then a (simulated) process crash in the middle of a transfer plus a torn final line, a restart,
 * saga recovery, and invariant checks. All data is synthetic; credits are fictional.
 *
 * Run: java -cp target/classes com.github.sup3rmar10.ledger.Demo
 */
public final class Demo {
    private Demo() {
    }

    public static void main(String[] args) throws IOException {
        Path dir = Files.createTempDirectory("ledger-demo");
        boolean ok;
        try {
            ok = run(dir, System.out);
        } finally {
            Files.deleteIfExists(dir.resolve("ledger.jsonl"));
            Files.deleteIfExists(dir);
        }
        if (!ok) {
            System.exit(1);
        }
    }

    public static boolean run(Path dir, PrintStream out) throws IOException {
        Path file = dir.resolve("ledger.jsonl");
        Clock clock = Clock.systemUTC();
        EventStore store = new FileEventStore(file, true);
        try {
            Ledger ledger = new Ledger(store, clock, Ledger.SagaHooks.NONE, 10);
            out.println("== phase 1: normal operation (file store, fsync on append) ==");
            send(out, ledger, "open alice", new OpenAccount("c01", "alice", "Alice (synthetic)"));
            send(out, ledger, "open bob  ", new OpenAccount("c02", "bob", "Bob (synthetic)"));
            send(out, ledger, "open carol", new OpenAccount("c03", "carol", "Carol (synthetic)"));
            send(out, ledger, "deposit alice 1000", new Deposit("c04", "alice", 1000));
            send(out, ledger, "deposit bob 500   ", new Deposit("c05", "bob", 500));
            send(out, ledger, "deposit carol 250 ", new Deposit("c06", "carol", 250));
            send(out, ledger, "withdraw bob 100  ", new Withdraw("c07", "bob", 100));
            long netIssued = 1000 + 500 + 250 - 100;
            send(out, ledger, "transfer alice->bob 200", new Transfer("t01", "alice", "bob", 200));
            send(out, ledger, "transfer carol->alice 999 (overdraft)", new Transfer("t02", "carol", "alice", 999));
            send(out, ledger, "deposit alice 1000 AGAIN, same commandId c04", new Deposit("c04", "alice", 1000));
            send(out, ledger, "transfer alice->ghost 50 (ghost never opened)", new Transfer("t03", "alice", "ghost", 50));

            out.println("== phase 2: crash in the middle of a transfer ==");
            Ledger crashing = new Ledger(store, clock, id -> {
                throw new Ledger.SimulatedCrash("process killed after debit of transfer " + id);
            }, 10);
            try {
                crashing.handle(new Transfer("t-crash", "bob", "carol", 75));
            } catch (Ledger.SimulatedCrash e) {
                out.println("  CRASH (simulated): " + e.getMessage());
            }
            store.close();
            // A crash can also tear the line being written; simulate that by appending a partial line.
            Files.write(file, "{\"seq\":99,\"stream\":\"acct-bo".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);
            out.println("  (simulated) torn partial line appended to the log file");

            out.println("== phase 3: restart and recover ==");
            store = new FileEventStore(file, true);
            FileEventStore reopened = (FileEventStore) store;
            out.println("  torn bytes discarded on reopen: " + reopened.recoveredTornBytes());
            ledger = new Ledger(store, clock, Ledger.SagaHooks.NONE, 10);
            long sumBefore = sum(ledger);
            long inFlight = ledger.inFlightCredits();
            out.println("  before recovery: sum(balances)=" + sumBefore + " in-flight=" + inFlight
                    + " (sum + in-flight = " + (sumBefore + inFlight) + ")");
            int recovered = ledger.recoverSagas();
            out.println("  recoverSagas() completed " + recovered + " pending transfer(s)");

            out.println("== invariants ==");
            long sumAfter = sum(ledger);
            boolean conservedMidSaga = sumBefore + inFlight == netIssued;
            boolean conserved = sumAfter == netIssued;
            boolean noneNegative = ledger.projections().balances().state().values().stream().noneMatch(v -> v < 0);
            boolean settled = ledger.inFlightCredits() == 0 && ledger.projections().saga().pending().isEmpty();
            boolean rebuildEqual = Projections.rebuild(store).state().equals(ledger.projections().state());
            check(out, "sum(balances) + in-flight == credits issued minus withdrawn before recovery", conservedMidSaga);
            check(out, "sum(balances) == credits issued minus withdrawn (" + netIssued + ") after recovery", conserved);
            check(out, "no negative balance", noneNegative);
            check(out, "no unsettled transfers", settled);
            check(out, "rebuild-from-scratch projections == incrementally maintained", rebuildEqual);
            for (Map.Entry<String, Long> e : ledger.projections().balances().state().entrySet()) {
                out.println("  balance " + e.getKey() + " = " + e.getValue());
            }
            out.println("  statement for alice:");
            for (StatementRow r : ledger.statement("alice")) {
                out.println("    " + r);
            }
            boolean ok = conservedMidSaga && conserved && noneNegative && settled && rebuildEqual;
            out.println(ok ? "RESULT: all invariants hold" : "RESULT: INVARIANT VIOLATION");
            return ok;
        } finally {
            store.close();
        }
    }

    private static long sum(Ledger l) {
        return l.totalBalance(); // catches the projections up with the log first
    }

    private static void send(PrintStream out, Ledger ledger, String label, Command c) {
        Ledger.Result r = ledger.handle(c);
        out.println("  " + label + " -> " + r.outcome() + " (" + r.detail() + ")");
    }

    private static void check(PrintStream out, String what, boolean ok) {
        out.println("  [" + (ok ? "PASS" : "FAIL") + "] " + what);
    }
}
