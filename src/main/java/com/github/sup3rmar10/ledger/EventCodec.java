package com.github.sup3rmar10.ledger;

import com.github.sup3rmar10.ledger.EventStore.StoredEvent;
import com.github.sup3rmar10.ledger.LedgerEvent.AccountOpened;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsDeposited;
import com.github.sup3rmar10.ledger.LedgerEvent.CreditsWithdrawn;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCompensated;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferCredited;
import com.github.sup3rmar10.ledger.LedgerEvent.TransferDebited;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One event = one flat JSON line. Envelope keys: seq, stream, ver, schema, cmd, ts, type;
 * the remaining keys are the event's own fields.
 *
 * Schema history: v1 had no currency on AccountOpened; v2 added it. Old lines are upcast on read
 * (the file is never rewritten); new lines are always written as the current schema.
 */
public final class EventCodec {
    public static final int CURRENT_SCHEMA = 2;
    public static final String DEFAULT_CURRENCY = "CRD";

    private EventCodec() {
    }

    public static String encode(StoredEvent e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seq", e.seq());
        m.put("stream", e.stream());
        m.put("ver", e.version());
        m.put("schema", (long) CURRENT_SCHEMA);
        m.put("cmd", e.commandId());
        m.put("ts", e.timestampMillis());
        m.put("type", e.event().type());
        LedgerEvent ev = e.event();
        m.put("accountId", ev.accountId());
        if (ev instanceof AccountOpened o) {
            m.put("owner", o.owner());
            m.put("currency", o.currency());
        } else if (ev instanceof CreditsDeposited d) {
            m.put("amount", d.amount());
        } else if (ev instanceof CreditsWithdrawn w) {
            m.put("amount", w.amount());
        } else if (ev instanceof TransferDebited t) {
            m.put("transferId", t.transferId());
            m.put("to", t.to());
            m.put("amount", t.amount());
        } else if (ev instanceof TransferCredited t) {
            m.put("transferId", t.transferId());
            m.put("from", t.from());
            m.put("amount", t.amount());
        } else if (ev instanceof TransferCompensated t) {
            m.put("transferId", t.transferId());
            m.put("amount", t.amount());
            m.put("reason", t.reason());
        } else {
            throw new IllegalStateException("unhandled event type " + ev.type());
        }
        return Json.encode(m);
    }

    /** @throws IllegalArgumentException for anything that is not a valid, known-schema event line */
    public static StoredEvent decode(String line) {
        Map<String, Object> m = upcast(Json.decode(line));
        String acct = str(m, "accountId");
        LedgerEvent ev = switch (str(m, "type")) {
            case "AccountOpened" -> new AccountOpened(acct, str(m, "owner"), str(m, "currency"));
            case "CreditsDeposited" -> new CreditsDeposited(acct, num(m, "amount"));
            case "CreditsWithdrawn" -> new CreditsWithdrawn(acct, num(m, "amount"));
            case "TransferDebited" ->
                    new TransferDebited(acct, str(m, "transferId"), str(m, "to"), num(m, "amount"));
            case "TransferCredited" ->
                    new TransferCredited(acct, str(m, "transferId"), str(m, "from"), num(m, "amount"));
            case "TransferCompensated" ->
                    new TransferCompensated(acct, str(m, "transferId"), num(m, "amount"), str(m, "reason"));
            default -> throw new IllegalArgumentException("unknown event type " + m.get("type"));
        };
        return new StoredEvent(num(m, "seq"), str(m, "stream"), num(m, "ver"), str(m, "cmd"), num(m, "ts"), ev);
    }

    /** Raises an old-format map to the current schema, one version step at a time. */
    static Map<String, Object> upcast(Map<String, Object> m) {
        long v = m.containsKey("schema") ? num(m, "schema") : 1;
        if (v < 1 || v > CURRENT_SCHEMA) {
            throw new IllegalArgumentException("unsupported schema version " + v);
        }
        while (v < CURRENT_SCHEMA) {
            if (v == 1) {
                v1ToV2(m);
            }
            v++;
        }
        m.put("schema", v);
        return m;
    }

    private static void v1ToV2(Map<String, Object> m) {
        if ("AccountOpened".equals(m.get("type"))) {
            m.putIfAbsent("currency", DEFAULT_CURRENCY);
        }
    }

    private static String str(Map<String, Object> m, String key) {
        if (m.get(key) instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException("missing or non-string field: " + key);
    }

    private static long num(Map<String, Object> m, String key) {
        if (m.get(key) instanceof Long n) {
            return n;
        }
        throw new IllegalArgumentException("missing or non-integer field: " + key);
    }
}
