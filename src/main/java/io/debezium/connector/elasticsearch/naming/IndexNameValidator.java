/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.naming;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;

/**
 * Validation and sanitization of Elasticsearch index names: lowercase; none of
 * {@code \ / * ? " < > | , # :} or space; not starting with {@code - _ +}; not {@code .} or
 * {@code ..}; at most 255 bytes.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 4.1"
 */
public final class IndexNameValidator {

    private static final String DISALLOWED = "\\/*?\"<>|, #:";
    private static final int MAX_BYTES = 255;

    private IndexNameValidator() {
    }

    /**
     * Returns the specific rule the given name breaks, or empty when the name is valid.
     */
    public static Optional<String> violation(String name) {
        if (name == null || name.isEmpty()) {
            return Optional.of("the name is empty");
        }
        if (".".equals(name) || "..".equals(name)) {
            return Optional.of("the name must not be '.' or '..'");
        }
        if (!name.equals(name.toLowerCase(Locale.ROOT))) {
            return Optional.of("the name must be lowercase");
        }
        final char first = name.charAt(0);
        if (first == '-' || first == '_' || first == '+') {
            return Optional.of("the name must not start with '-', '_', or '+'");
        }
        for (char c : name.toCharArray()) {
            if (DISALLOWED.indexOf(c) >= 0) {
                return Optional.of("the name must not contain '" + c + "'");
            }
        }
        if (name.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
            return Optional.of("the name exceeds " + MAX_BYTES + " bytes");
        }
        return Optional.empty();
    }

    /**
     * Lowercases the name and replaces every disallowed character with the given replacement.
     * The caller is responsible for logging each distinct transformation.
     */
    public static String sanitize(String name, String replacement) {
        final StringBuilder result = new StringBuilder(name.length());
        final String lowered = name.toLowerCase(Locale.ROOT);
        for (int i = 0; i < lowered.length(); i++) {
            final char c = lowered.charAt(i);
            if (DISALLOWED.indexOf(c) >= 0) {
                result.append(replacement);
            }
            else if (i == 0 && (c == '-' || c == '_' || c == '+')) {
                result.append(replacement);
            }
            else {
                result.append(c);
            }
        }
        String sanitized = result.toString();
        while (sanitized.startsWith("-") || sanitized.startsWith("_") || sanitized.startsWith("+")) {
            sanitized = sanitized.substring(1);
        }
        if (sanitized.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
            sanitized = truncateToMaxBytes(sanitized);
        }
        return sanitized;
    }

    /**
     * Cuts at the last whole code point whose UTF-8 encoding still fits, so multi-byte names
     * neither stay oversized nor lose half a surrogate pair.
     */
    private static String truncateToMaxBytes(String name) {
        int bytes = 0;
        int end = 0;
        while (end < name.length()) {
            final int codePoint = name.codePointAt(end);
            final int width = codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x1_0000 ? 3 : 4;
            if (bytes + width > MAX_BYTES) {
                break;
            }
            bytes += width;
            end += Character.charCount(codePoint);
        }
        return name.substring(0, end);
    }
}
