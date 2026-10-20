package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.NewEvent;
import com.github.sup3rmar10.ledger.LedgerEvent.AccountOpened;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsDeposited;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Crude single-run micro-benchmark (no JMH): numbers vary by machine, disk and JIT warm-up.
 * Run: java -cp target/classes com.github.sup3rmar10.ledger.Bench
 */
public final class Bench {
    private Bench() {
    }

    public static void main(String[] args) throws IOException {
        Path dir = Files.createTempDirectory("ledger-bench");
        try {
            appendBenchmarks(dir);
            replayBenchmarks(dir);
        } finally {
            try (var files = Files.list(dir)) {
                for (Path p : (Iterable<Path>) files::iterator) {
                    Files.deleteIfExists(p);
                }
            }
            Files.deleteIfExists(dir);
        }
    }

    private static void appendBenchmarks(Path dir) {
        appendRate(new InMemoryEventStore(), 100_000, 100); // warm-up
        report("append in-memory (500k events)", appendRate(new InMemoryEventStore(), 500_000, 100));

        try (FileEventStore warm = new FileEventStore(dir.resolve("warm.jsonl"), false)) {
            appendRate(warm, 20_000, 100);
        }
        try (FileEventStore s = new FileEventStore(dir.resolve("nofsync.jsonl"), false)) {
            report("append file, no fsync (100k events)", appendRate(s, 100_000, 100));
        }
        try (FileEventStore s = new FileEventStore(dir.resolve("fsync.jsonl"), true)) {
            report("append file, fsync per append (2k events)", appendRate(s, 2_000, 100));
        }
    }

    private static void replayBenchmarks(Path dir) {
        int n = 200_050; // not a multiple of 100, so the snapshot path still replays a 50-event tail
        InMemoryEventStore store = new InMemoryEventStore();
        AccountRepository withSnap = new AccountRepository(store, 100);
        AccountRepository noSnap = new AccountRepository(store, 0);
        Account acc = withSnap.save(Account.empty("x"), new AccountOpened("x", "bench", "CRD"), "c0", 1L);
        for (int i = 1; i < n; i++) {
            acc = withSnap.save(acc, new CreditsDeposited("x", 1), "c" + i, 1L);
        }
        double full = timeLoadMillis(noSnap, 20);
        double snap = timeLoadMillis(withSnap, 20);
        System.out.printf(Locale.ROOT, "load one account with %d events, full replay:       %.3f ms%n", n, full);
        System.out.printf(Locale.ROOT, "load one account with %d events, snapshot every 100: %.3f ms%n", n, snap);

        Path file = dir.resolve("reopen.jsonl");
        int events = 100_000;
        try (FileEventStore s = new FileEventStore(file, false)) {
            appendRate(s, events, 1000);
        }
        long t0 = System.nanoTime();
        try (FileEventStore s = new FileEventStore(file, false)) {
            double ms = (System.nanoTime() - t0) / 1e6;
            System.out.printf(Locale.ROOT, "reopen file store with %d events (parse + index): %.1f ms%n",
                    s.lastSeq(), ms);
        }
    }

    private static double timeLoadMillis(AccountRepository repo, int iterations) {
        for (int i = 0; i < 5; i++) {
            repo.load("x");
        }
        long t0 = System.nanoTime();
        long sink = 0;
        for (int i = 0; i < iterations; i++) {
            sink += repo.load("x").balance();
        }
        if (sink < 0) {
            System.out.println(sink);
        }
        return (System.nanoTime() - t0) / 1e6 / iterations;
    }

    private static double appendRate(EventStore s, int n, int streams) {
        long[] ver = new long[streams];
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            int k = i % streams;
            s.append("acct-" + k, ver[k], List.of(new NewEvent(new CreditsDeposited("a" + k, 1), "c" + i, 1L)));
            ver[k]++;
        }
        return n / ((System.nanoTime() - t0) / 1e9);
    }

    private static void report(String label, double eventsPerSec) {
        System.out.printf(Locale.ROOT, "%s: %,.0f events/sec%n", label, eventsPerSec);
    }
}
