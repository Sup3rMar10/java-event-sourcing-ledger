package com.github.sup3rmar10.ledger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory index shared by both stores. Subclasses add durability by overriding persist(), which
 * runs after the version check but BEFORE the events become visible: if it throws, nothing is indexed.
 */
public abstract class AbstractEventStore implements EventStore {
    private final List<StoredEvent> log = new ArrayList<>();
    private final Map<String, List<StoredEvent>> streams = new HashMap<>();

    @Override
    public synchronized List<StoredEvent> append(String stream, long expectedVersion, List<NewEvent> events) {
        long actual = streamVersion(stream);
        if (expectedVersion != ANY_VERSION && expectedVersion != actual) {
            throw new ConcurrencyException(stream, expectedVersion, actual);
        }
        List<StoredEvent> stored = new ArrayList<>(events.size());
        long seq = log.size();
        for (NewEvent n : events) {
            stored.add(new StoredEvent(++seq, stream, actual + stored.size() + 1, n.commandId(),
                    n.timestampMillis(), n.event()));
        }
        if (stored.isEmpty()) {
            return stored;
        }
        persist(stored);
        for (StoredEvent s : stored) {
            index(s);
        }
        return stored;
    }

    /** Make the events durable. Default: nothing (pure in-memory). */
    protected void persist(List<StoredEvent> events) {
    }

    /** Adds an event to the in-memory index, insisting on a gap-free seq and per-stream version. */
    protected final synchronized void index(StoredEvent e) {
        if (e.seq() != log.size() + 1L) {
            throw new CorruptLogException("expected seq " + (log.size() + 1) + " but found " + e.seq());
        }
        List<StoredEvent> s = streams.computeIfAbsent(e.stream(), k -> new ArrayList<>());
        if (e.version() != s.size() + 1L) {
            throw new CorruptLogException("stream " + e.stream() + ": expected version " + (s.size() + 1)
                    + " but found " + e.version());
        }
        log.add(e);
        s.add(e);
    }

    @Override
    public synchronized List<StoredEvent> readStream(String stream, long afterVersion) {
        return tail(streams.getOrDefault(stream, List.of()), afterVersion);
    }

    @Override
    public synchronized List<StoredEvent> readAll(long afterSeq) {
        return tail(log, afterSeq);
    }

    private static List<StoredEvent> tail(List<StoredEvent> list, long after) {
        if (after >= list.size()) {
            return List.of();
        }
        return new ArrayList<>(list.subList((int) Math.max(0, after), list.size()));
    }

    @Override
    public synchronized long streamVersion(String stream) {
        return streams.getOrDefault(stream, List.of()).size();
    }

    @Override
    public synchronized long lastSeq() {
        return log.size();
    }
}
