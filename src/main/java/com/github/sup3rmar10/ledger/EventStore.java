package com.github.sup3rmar10.ledger;

import java.util.List;
import java.util.Objects;

/**
 * Append-only log with per-stream optimistic concurrency and a gap-free global sequence.
 * A stream's version is the number of events in it (0 = stream does not exist yet).
 */
public interface EventStore extends AutoCloseable {

    /** Pass as expectedVersion to skip the concurrency check. */
    long ANY_VERSION = -1;

    record NewEvent(LedgerEvent event, String commandId, long timestampMillis) {
        public NewEvent {
            Objects.requireNonNull(event, "event");
            Objects.requireNonNull(commandId, "commandId");
        }
    }

    record StoredEvent(long seq, String stream, long version, String commandId, long timestampMillis,
                       LedgerEvent event) {
    }

    /** expectedVersion did not match the stream's current version; nothing was written. */
    final class ConcurrencyException extends RuntimeException {
        public ConcurrencyException(String stream, long expected, long actual) {
            super("stream " + stream + ": expected version " + expected + " but was " + actual);
        }
    }

    /** The log is damaged somewhere other than a torn final line; refusing to guess. */
    final class CorruptLogException extends RuntimeException {
        public CorruptLogException(String message) {
            super(message);
        }
    }

    /** Appends atomically with respect to the version check; returns the stored events. */
    List<StoredEvent> append(String stream, long expectedVersion, List<NewEvent> events);

    /** Events of one stream with version greater than afterVersion, in order. */
    List<StoredEvent> readStream(String stream, long afterVersion);

    /** All events with global seq greater than afterSeq, in order. */
    List<StoredEvent> readAll(long afterSeq);

    long streamVersion(String stream);

    long lastSeq();

    @Override
    default void close() {
    }
}
