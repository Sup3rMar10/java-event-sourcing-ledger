package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import com.github.sup3rmar10.ledger.LedgerEvent.AccountOpened;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsDeposited;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsWithdrawn;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCompensated;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCredited;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferDebited;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventCodecTest {

    static final List<LedgerEvent> SAMPLES = List.of(
            new AccountOpened("a1", "Alice \"A\"", "CRD"),
            new CreditsDeposited("a1", 100),
            new CreditsWithdrawn("a1", 30),
            new TransferDebited("a1", "t1", "a2", 25),
            new TransferCredited("a2", "t1", "a1", 25),
            new TransferCompensated("a1", "t1", 25, "account a2 is not open"));

    @Test
    void everyEventTypeRoundTripsAndSamplesCoverTheSealedHierarchy() {
        long seq = 1;
        for (LedgerEvent ev : SAMPLES) {
            StoredEvent se = new StoredEvent(seq, "acct-x", seq, "cmd-" + seq, 1_700_000_000_000L + seq, ev);
            assertEquals(se, EventCodec.decode(EventCodec.encode(se)), ev.type());
            seq++;
        }
        Set<Class<?>> covered = new HashSet<>();
        SAMPLES.forEach(e -> covered.add(e.getClass()));
        Set<Class<?>> permitted = new HashSet<>(List.of(LedgerEvent.class.getPermittedSubclasses()));
        assertEquals(permitted, covered, "a new LedgerEvent subtype needs a codec case and a sample");
        // apply()/delta() must also know every subtype (instanceof chains have no compiler exhaustiveness check)
        SAMPLES.forEach(e -> Account.delta(e));
        SAMPLES.forEach(e -> Account.empty("a1").apply(e));
    }

    @Test
    void writesCurrentSchemaAndOneLine() {
        String line = EventCodec.encode(new StoredEvent(1, "acct-a", 1, "c1", 5, new AccountOpened("a", "o", "CRD")));
        assertTrue(line.contains("\"schema\":2"), line);
        assertTrue(line.contains("\"currency\":\"CRD\""), line);
        assertEquals(false, line.contains("\n"));
    }

    @Test
    void upcastsV1AccountOpenedByAddingDefaultCurrency() {
        String v1 = "{\"seq\":1,\"stream\":\"acct-old\",\"ver\":1,\"schema\":1,\"cmd\":\"c0\",\"ts\":1000,"
                + "\"type\":\"AccountOpened\",\"accountId\":\"old\",\"owner\":\"Legacy Owner\"}";
        StoredEvent e = EventCodec.decode(v1);
        assertEquals(new AccountOpened("old", "Legacy Owner", "CRD"), e.event());
        assertEquals("CRD", ((AccountOpened) e.event()).currency());
    }

    @Test
    void v1EventsThatDidNotChangeAreLeftAlone() {
        String v1 = "{\"seq\":2,\"stream\":\"acct-old\",\"ver\":2,\"schema\":1,\"cmd\":\"c1\",\"ts\":1001,"
                + "\"type\":\"CreditsDeposited\",\"accountId\":\"old\",\"amount\":77}";
        assertEquals(new CreditsDeposited("old", 77), EventCodec.decode(v1).event());
    }

    @Test
    void lineWithoutSchemaFieldIsTreatedAsV1() {
        String legacy = "{\"seq\":1,\"stream\":\"acct-z\",\"ver\":1,\"cmd\":\"c\",\"ts\":1,"
                + "\"type\":\"AccountOpened\",\"accountId\":\"z\",\"owner\":\"o\"}";
        assertEquals("CRD", ((AccountOpened) EventCodec.decode(legacy).event()).currency());
    }

    @Test
    void v2LineKeepsItsOwnCurrency() {
        String v2 = "{\"seq\":1,\"stream\":\"acct-z\",\"ver\":1,\"schema\":2,\"cmd\":\"c\",\"ts\":1,"
                + "\"type\":\"AccountOpened\",\"accountId\":\"z\",\"owner\":\"o\",\"currency\":\"XYZ\"}";
        assertEquals("XYZ", ((AccountOpened) EventCodec.decode(v2).event()).currency());
    }

    @Test
    void rejectsFutureSchemaUnknownTypeAndMissingFields() {
        String base = "{\"seq\":1,\"stream\":\"s\",\"ver\":1,\"schema\":%d,\"cmd\":\"c\",\"ts\":1,\"type\":\"%s\","
                + "\"accountId\":\"a\",\"amount\":5}";
        assertThrows(IllegalArgumentException.class,
                () -> EventCodec.decode(String.format(base, 3, "CreditsDeposited")));
        assertThrows(IllegalArgumentException.class,
                () -> EventCodec.decode(String.format(base, 2, "SomethingElse")));
        assertThrows(IllegalArgumentException.class, () -> EventCodec.decode(
                "{\"seq\":1,\"stream\":\"s\",\"ver\":1,\"schema\":2,\"cmd\":\"c\",\"ts\":1,"
                        + "\"type\":\"CreditsDeposited\",\"accountId\":\"a\"}"));
    }
}
