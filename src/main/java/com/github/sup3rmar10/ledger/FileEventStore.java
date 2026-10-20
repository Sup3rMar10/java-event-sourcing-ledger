package com.github.sup3rmar10.ledger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * JSONL file: one event per line, each line terminated by '\n'.
 *
 * Torn-last-line rule on open: an entry exists only if its line is newline-terminated and parses.
 * Bytes after the last '\n' are a torn tail (a crash mid-write; the append was never acknowledged),
 * so they are truncated away. A newline-terminated line that does not parse, or a seq/version gap,
 * is real corruption and raises CorruptLogException instead of being silently dropped.
 *
 * Optional fsync-on-append calls FileChannel.force(false) before append() returns.
 */
public final class FileEventStore extends AbstractEventStore {
    private final FileChannel channel;
    private final boolean fsyncOnAppend;
    private final long recoveredTornBytes;
    private long position;

    public FileEventStore(Path file, boolean fsyncOnAppend) {
        this.fsyncOnAppend = fsyncOnAppend;
        try {
            byte[] data = Files.exists(file) ? Files.readAllBytes(file) : new byte[0];
            int start = 0;
            int lineNo = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] != '\n') {
                    continue;
                }
                lineNo++;
                String line = new String(data, start, i - start, StandardCharsets.UTF_8);
                try {
                    index(EventCodec.decode(line));
                } catch (IllegalArgumentException ex) {
                    throw new CorruptLogException("line " + lineNo + " of " + file.getFileName() + ": "
                            + ex.getMessage());
                }
                start = i + 1;
            }
            this.recoveredTornBytes = data.length - start;
            this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            if (recoveredTornBytes > 0) {
                channel.truncate(start);
                channel.force(true);
            }
            this.position = start;
            channel.position(start);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** How many bytes of torn tail were discarded when this store was opened (0 = clean). */
    public long recoveredTornBytes() {
        return recoveredTornBytes;
    }

    @Override
    protected void persist(List<StoredEvent> events) {
        StringBuilder sb = new StringBuilder();
        for (StoredEvent e : events) {
            sb.append(EventCodec.encode(e)).append('\n');
        }
        ByteBuffer buf = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
        long len = buf.remaining();
        try {
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            if (fsyncOnAppend) {
                channel.force(false);
            }
            position += len;
        } catch (IOException e) {
            try {
                channel.truncate(position);
                channel.position(position);
            } catch (IOException ignored) {
                // best effort: the next open will treat any partial line as a torn tail
            }
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
