package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.ConcurrencyException;
import com.github.sup3rmar10.ledger.EventStore.NewEvent;
import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static com.github.sup3rmar10.ledger.Fixtures.dep;
import static com.github.sup3rmar10.ledger.Fixtures.one;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Behaviour every EventStore implementation must share. */
abstract class EventStoreContract {
    @TempDir
    Path dir;
    EventStore store;

    abstract EventStore create(Path dir);

    @BeforeEach
    void openStore() {
        store = create(dir);
    }

    @AfterEach
    void closeStore() {
        store.close();
    }

    @Test
    void assignsGaplessSeqAndPerStreamVersions() {
        List<StoredEvent> a = store.append("s-a", 0, one(dep("a", 1), "c1"));
        List<StoredEvent> b = store.append("s-b", 0, one(dep("b", 2), "c2"));
        List<StoredEvent> a2 = store.append("s-a", 1, one(dep("a", 3), "c3"));
        assertEquals(1, a.get(0).seq());
        assertEquals(2, b.get(0).seq());
        assertEquals(3, a2.get(0).seq());
        assertEquals(1, a.get(0).version());
        assertEquals(1, b.get(0).version());
        assertEquals(2, a2.get(0).version());
        assertEquals(2, store.streamVersion("s-a"));
        assertEquals(0, store.streamVersion("nope"));
        assertEquals(3, store.lastSeq());
    }

    @Test
    void wrongExpectedVersionIsRejectedAndNothingIsWritten() {
        store.append("s", 0, one(dep("a", 1), "c1"));
        assertThrows(ConcurrencyException.class, () -> store.append("s", 0, one(dep("a", 2), "c2")));
        assertThrows(ConcurrencyException.class, () -> store.append("s", 5, one(dep("a", 2), "c2")));
        assertEquals(1, store.streamVersion("s"));
        assertEquals(1, store.lastSeq());
        assertEquals(1, store.readAll(0).size());
    }

    @Test
    void creatingAnExistingStreamConflicts() {
        store.append("s", 0, one(dep("a", 1), "c1"));
        ConcurrencyException e = assertThrows(ConcurrencyException.class,
                () -> store.append("s", 0, one(dep("a", 1), "c1b")));
        assertEquals(true, e.getMessage().contains("expected version 0 but was 1"), e.getMessage());
    }

    @Test
    void anyVersionSkipsTheCheck() {
        store.append("s", 0, one(dep("a", 1), "c1"));
        store.append("s", EventStore.ANY_VERSION, one(dep("a", 2), "c2"));
        assertEquals(2, store.streamVersion("s"));
    }

    @Test
    void readStreamAndReadAllHonourTheirCursors() {
        store.append("s1", 0, one(dep("a", 1), "c1"));
        store.append("s2", 0, one(dep("b", 1), "c2"));
        store.append("s1", 1, one(dep("a", 2), "c3"));
        store.append("s1", 2, one(dep("a", 3), "c4"));
        assertEquals(List.of(1L, 3L, 4L), store.readStream("s1", 0).stream().map(StoredEvent::seq).toList());
        assertEquals(List.of(3L, 4L), store.readStream("s1", 1).stream().map(StoredEvent::seq).toList());
        assertEquals(List.of(), store.readStream("s1", 3));
        assertEquals(List.of(), store.readStream("missing", 0));
        assertEquals(List.of(3L, 4L), store.readAll(2).stream().map(StoredEvent::seq).toList());
        assertEquals(List.of(), store.readAll(4));
        assertEquals(4, store.readAll(0).size());
    }

    @Test
    void batchAppendIsContiguousAndEmptyBatchIsANoOp() {
        List<NewEvent> batch = List.of(new NewEvent(dep("a", 1), "c1", 1), new NewEvent(dep("a", 2), "c2", 2),
                new NewEvent(dep("a", 3), "c3", 3));
        List<StoredEvent> stored = store.append("s", 0, batch);
        assertEquals(List.of(1L, 2L, 3L), stored.stream().map(StoredEvent::version).toList());
        assertEquals(List.of(1L, 2L, 3L), stored.stream().map(StoredEvent::seq).toList());
        assertEquals(List.of(), store.append("s", 3, List.of()));
        assertEquals(3, store.lastSeq());
    }
}
