package com.github.sup3rmar10.ledger;

import java.nio.file.Path;

class InMemoryEventStoreTest extends EventStoreContract {
    @Override
    EventStore create(Path dir) {
        return new InMemoryEventStore();
    }
}
