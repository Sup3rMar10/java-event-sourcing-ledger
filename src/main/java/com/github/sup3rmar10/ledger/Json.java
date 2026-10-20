package com.github.sup3rmar10.ledger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tiny JSON encoder/decoder for FLAT objects whose values are strings or integers.
 * That is all the event log needs; nested values, floats, booleans and null are rejected
 * on purpose so a malformed line fails loudly instead of being half-understood.
 */
public final class Json {
    private Json() {
    }

    public static String encode(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            quote(sb, e.getKey()).append(':');
            Object v = e.getValue();
            if (v instanceof String s) {
                quote(sb, s);
            } else if (v instanceof Long || v instanceof Integer) {
                sb.append(((Number) v).longValue());
            } else {
                throw new IllegalArgumentException("unsupported value type for key " + e.getKey());
            }
        }
        return sb.append('}').toString();
    }

    private static StringBuilder quote(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"');
    }

    public static Map<String, Object> decode(String text) {
        Parser p = new Parser(text);
        Map<String, Object> m = p.object();
        p.skipWs();
        if (p.pos != text.length()) {
            throw p.error("trailing characters");
        }
        return m;
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        IllegalArgumentException error(String msg) {
            return new IllegalArgumentException("json error at " + pos + ": " + msg);
        }

        void skipWs() {
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        char peek() {
            if (pos >= s.length()) {
                throw error("unexpected end of input");
            }
            return s.charAt(pos);
        }

        char next() {
            char c = peek();
            pos++;
            return c;
        }

        void expect(char c) {
            if (next() != c) {
                throw error("expected '" + c + "'");
            }
        }

        Map<String, Object> object() {
            skipWs();
            expect('{');
            Map<String, Object> m = new LinkedHashMap<>();
            skipWs();
            if (peek() == '}') {
                pos++;
                return m;
            }
            while (true) {
                skipWs();
                String key = string();
                skipWs();
                expect(':');
                skipWs();
                Object v = value();
                if (m.put(key, v) != null) {
                    throw error("duplicate key " + key);
                }
                skipWs();
                char c = next();
                if (c == '}') {
                    return m;
                }
                if (c != ',') {
                    throw error("expected ',' or '}'");
                }
            }
        }

        Object value() {
            char c = peek();
            if (c == '"') {
                return string();
            }
            if (c == '-' || (c >= '0' && c <= '9')) {
                return number();
            }
            throw error("unsupported value (only strings and integers)");
        }

        Long number() {
            int start = pos;
            if (peek() == '-') {
                pos++;
            }
            int digits = pos;
            while (pos < s.length() && s.charAt(pos) >= '0' && s.charAt(pos) <= '9') {
                pos++;
            }
            if (pos == digits) {
                throw error("bad number");
            }
            if (pos < s.length() && (s.charAt(pos) == '.' || s.charAt(pos) == 'e' || s.charAt(pos) == 'E')) {
                throw error("only integers are supported");
            }
            try {
                return Long.parseLong(s.substring(start, pos));
            } catch (NumberFormatException e) {
                throw error("number out of range");
            }
        }

        String string() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    char e = next();
                    switch (e) {
                        case '"', '\\', '/' -> sb.append(e);
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if (pos + 4 > s.length()) {
                                throw error("truncated unicode escape");
                            }
                            try {
                                sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                            } catch (NumberFormatException ex) {
                                throw error("bad unicode escape");
                            }
                            pos += 4;
                        }
                        default -> throw error("bad escape \\" + e);
                    }
                } else if (c < 0x20) {
                    throw error("unescaped control character");
                } else {
                    sb.append(c);
                }
            }
        }
    }
}
