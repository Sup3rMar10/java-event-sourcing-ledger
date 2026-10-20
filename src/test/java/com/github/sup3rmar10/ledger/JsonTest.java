package com.github.sup3rmar10.ledger;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonTest {

    @Test
    void roundTripsStringsAndLongsIncludingAwkwardCharacters() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("quote", "say \"hi\" \\ back\\slash");
        m.put("ws", "line1\nline2\r\ttab");
        m.put("ctl", "bell\u0007end");
        m.put("uni", "café ☃");
        m.put("neg", -42L);
        m.put("max", Long.MAX_VALUE);
        m.put("min", Long.MIN_VALUE);
        m.put("empty", "");
        String json = Json.encode(m);
        assertEquals(m, Json.decode(json));
        assertEquals(false, json.contains("\n"), "encoded text must stay on one line");
    }

    @Test
    void decodesUnicodeEscapesAndWhitespace() {
        Map<String, Object> m = Json.decode("  { \"a\" : \"caf\\u00e9\" , \"b\" : 7 }  ");
        assertEquals("café", m.get("a"));
        assertEquals(7L, m.get("b"));
        assertEquals(Map.of(), Json.decode("{}"));
    }

    @Test
    void rejectsValuesOutsideTheFlatSubset() {
        for (String bad : new String[] {"{\"a\":{}}", "{\"a\":[1]}", "{\"a\":1.5}", "{\"a\":true}",
                "{\"a\":null}", "{\"a\":1e3}", "{\"a\":99999999999999999999}"}) {
            assertThrows(IllegalArgumentException.class, () -> Json.decode(bad), bad);
        }
    }

    @Test
    void rejectsTrailingGarbageAndDuplicateKeys() {
        assertThrows(IllegalArgumentException.class, () -> Json.decode("{\"a\":1} x"));
        assertThrows(IllegalArgumentException.class, () -> Json.decode("{\"a\":1,\"a\":2}"));
    }

    @Test
    void everyProperPrefixOfAnEncodedLineIsRejected() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "CreditsDeposited");
        m.put("amount", 12345L);
        m.put("note", "a\"b");
        String json = Json.encode(m);
        for (int cut = 0; cut < json.length(); cut++) {
            String prefix = json.substring(0, cut);
            assertThrows(IllegalArgumentException.class, () -> Json.decode(prefix), "prefix length " + prefix.length());
        }
    }

    @Test
    void encodeRejectsUnsupportedValueTypes() {
        assertThrows(IllegalArgumentException.class, () -> Json.encode(Map.of("a", 1.5)));
        assertThrows(IllegalArgumentException.class, () -> Json.encode(Map.of("a", true)));
    }
}
