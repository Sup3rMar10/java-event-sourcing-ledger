package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.CorruptLogException;
import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import com.github.sup3rmar10.ledger.LedgerEvent.AccountOpened;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static com.github.sup3rmar10.ledger.Fixtures.dep;
import static com.github.sup3rmar10.ledger.Fixtures.one;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileEventStoreTest extends EventStoreContract {

    @Override
    EventStore create(Path dir) {
        return new FileEventStore(dir.resolve("contract.jsonl"), false);
    }

    private Path threeEventFile(String name) {
        Path file = dir.resolve(name);
        try (FileEventStore s = new FileEventStore(file, false)) {
            s.append("s", 0, one(dep("a", 10), "c1"));
            s.append("s", 1, one(dep("a", 20), "c2"));
            s.append("s", 2, one(dep("a", 30), "c3"));
        }
        return file;
    }

    private static int lastLineStart(byte[] data) {
        int i = data.length - 2; // skip the final '\n'
        while (i >= 0 && data[i] != '\n') {
            i--;
        }
        return i + 1;
    }

    @Test
    void reopenRestoresEventsAndContinuesTheSequence() throws IOException {
        Path file = threeEventFile("reopen.jsonl");
        try (FileEventStore s = new FileEventStore(file, false)) {
            assertEquals(3, s.lastSeq());
            assertEquals(3, s.streamVersion("s"));
            assertEquals(0, s.recoveredTornBytes());
            assertEquals(dep("a", 20), s.readStream("s", 1).get(0).event());
            StoredEvent e = s.append("s", 3, one(dep("a", 40), "c4")).get(0);
            assertEquals(4, e.seq());
        }
        try (FileEventStore s = new FileEventStore(file, false)) {
            assertEquals(4, s.lastSeq());
        }
    }

    @Test
    void fsyncVariantWritesTheSameDurableFormat() {
        Path file = dir.resolve("fsync.jsonl");
        try (FileEventStore s = new FileEventStore(file, true)) {
            s.append("s", 0, one(dep("a", 1), "c1"));
            s.append("s", 1, one(dep("a", 2), "c2"));
        }
        try (FileEventStore s = new FileEventStore(file, false)) {
            assertEquals(2, s.lastSeq());
        }
    }

    @Test
    void tornTailIsDroppedAndTruncatedAtEveryCutPoint() throws IOException {
        Path file = threeEventFile("torn.jsonl");
        byte[] full = Files.readAllBytes(file);
        int lastStart = lastLineStart(full);
        // cut == lastStart means a clean 2-event file; every later cut leaves a partial 3rd line,
        // including the cut that removes only the final '\n' (complete JSON, no terminator).
        for (int cut = lastStart; cut < full.length; cut++) {
            Files.write(file, Arrays.copyOf(full, cut));
            try (FileEventStore s = new FileEventStore(file, false)) {
                assertEquals(2, s.lastSeq(), "cut=" + cut);
                assertEquals(cut - lastStart, s.recoveredTornBytes(), "cut=" + cut);
            }
            assertEquals(lastStart, Files.size(file), "file must be physically truncated, cut=" + cut);
        }
    }

    @Test
    void appendAfterTornRecoveryLeavesACleanLog() throws IOException {
        Path file = threeEventFile("tornappend.jsonl");
        byte[] full = Files.readAllBytes(file);
        Files.write(file, Arrays.copyOf(full, full.length - 7));
        try (FileEventStore s = new FileEventStore(file, false)) {
            assertEquals(2, s.lastSeq());
            s.append("s", 2, one(dep("a", 99), "c3-retry"));
        }
        try (FileEventStore s = new FileEventStore(file, false)) {
            assertEquals(3, s.lastSeq());
            assertEquals(0, s.recoveredTornBytes());
            assertEquals(dep("a", 99), s.readStream("s", 2).get(0).event());
        }
        String text = Files.readString(file, StandardCharsets.UTF_8);
        assertTrue(text.endsWith("\n"));
        assertEquals(3, text.split("\n").length);
    }

    @Test
    void completeLineThatDoesNotParseIsCorruptionNotATornTail() throws IOException {
        Path file = threeEventFile("corrupt.jsonl");
        List<String> lines = Files.readAllLines(file);
        Files.writeString(file, lines.get(0) + "\n" + "{this is not json}\n" + lines.get(2) + "\n");
        CorruptLogException e = assertThrows(CorruptLogException.class, () -> new FileEventStore(file, false));
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
    }

    @Test
    void sequenceGapIsCorruption() throws IOException {
        Path file = threeEventFile("gap.jsonl");
        List<String> lines = Files.readAllLines(file);
        Files.writeString(file, lines.get(0) + "\n" + lines.get(2) + "\n");
        assertThrows(CorruptLogException.class, () -> new FileEventStore(file, false));
    }

    @Test
    void sequenceGapAcrossDifferentStreamsIsCorruptionToo() throws IOException {
        // every stream version is 1, so only the global seq (1,3) reveals that a line went missing
        Path file = dir.resolve("gap2.jsonl");
        try (FileEventStore s = new FileEventStore(file, false)) {
            s.append("s1", 0, one(dep("a", 1), "c1"));
            s.append("s2", 0, one(dep("b", 1), "c2"));
            s.append("s3", 0, one(dep("c", 1), "c3"));
        }
        List<String> lines = Files.readAllLines(file);
        Files.writeString(file, lines.get(0) + "\n" + lines.get(2) + "\n");
        CorruptLogException e = assertThrows(CorruptLogException.class, () -> new FileEventStore(file, false));
        assertTrue(e.getMessage().contains("expected seq 2 but found 3"), e.getMessage());
    }

    @Test
    void blankLineInTheMiddleIsCorruption() throws IOException {
        Path file = threeEventFile("blank.jsonl");
        List<String> lines = Files.readAllLines(file);
        Files.writeString(file, lines.get(0) + "\n\n" + lines.get(1) + "\n");
        assertThrows(CorruptLogException.class, () -> new FileEventStore(file, false));
    }

    @Test
    void readsLegacyV1LinesUpcastsThemAndNeverRewritesThem() throws IOException {
        Path file = dir.resolve("legacy.jsonl");
        String v1 = "{\"seq\":1,\"stream\":\"acct-old\",\"ver\":1,\"schema\":1,\"cmd\":\"c0\",\"ts\":1000,"
                + "\"type\":\"AccountOpened\",\"accountId\":\"old\",\"owner\":\"Legacy Owner\"}\n";
        Files.writeString(file, v1);
        try (FileEventStore s = new FileEventStore(file, false)) {
            assertEquals(new AccountOpened("old", "Legacy Owner", "CRD"), s.readAll(0).get(0).event());
            s.append("acct-old", 1, one(dep("old", 5), "c1"));
        }
        byte[] bytes = Files.readAllBytes(file);
        byte[] v1Bytes = v1.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(v1Bytes, Arrays.copyOf(bytes, v1Bytes.length), "old line must be untouched");
        String[] lines = new String(bytes, StandardCharsets.UTF_8).split("\n");
        assertTrue(lines[1].contains("\"schema\":2"), lines[1]);
    }
}
