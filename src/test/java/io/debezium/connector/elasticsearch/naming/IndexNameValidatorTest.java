/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.naming;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link IndexNameValidator}: every Elasticsearch index naming rule and the
 * sanitization that repairs a violation.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class IndexNameValidatorTest {

    @ParameterizedTest
    @ValueSource(strings = { "customers", "inventory.customers", "a-b_c+d", "orders-2024.03.15", "x" })
    void shouldAcceptValidNames(String name) {
        assertThat(IndexNameValidator.violation(name)).isEmpty();
    }

    @Test
    void shouldRejectEmptyAndNull() {
        assertThat(IndexNameValidator.violation("")).contains("the name is empty");
        assertThat(IndexNameValidator.violation(null)).contains("the name is empty");
    }

    @ParameterizedTest
    @ValueSource(strings = { ".", ".." })
    void shouldRejectDotNames(String name) {
        assertThat(IndexNameValidator.violation(name)).contains("the name must not be '.' or '..'");
    }

    @ParameterizedTest
    @ValueSource(strings = { "Customers", "inventory.Customers", "ÄBC" })
    void shouldRejectUppercase(String name) {
        assertThat(IndexNameValidator.violation(name)).contains("the name must be lowercase");
    }

    @ParameterizedTest
    @ValueSource(strings = { "-customers", "_customers", "+customers" })
    void shouldRejectLeadingDashUnderscoreOrPlus(String name) {
        assertThat(IndexNameValidator.violation(name)).contains("the name must not start with '-', '_', or '+'");
    }

    @ParameterizedTest
    @ValueSource(strings = { "\\", "/", "*", "?", "\"", "<", ">", "|", ",", " ", "#", ":" })
    void shouldRejectEachDisallowedCharacter(String character) {
        assertThat(IndexNameValidator.violation("abc" + character + "def"))
                .contains("the name must not contain '" + character + "'");
    }

    @Test
    void shouldEnforceByteLimitNotCharacterLimit() {
        assertThat(IndexNameValidator.violation("a".repeat(255))).isEmpty();
        assertThat(IndexNameValidator.violation("a".repeat(256))).contains("the name exceeds 255 bytes");
        // 'ü' is two bytes in UTF-8, so 128 of them is 256 bytes although only 128 characters.
        assertThat(IndexNameValidator.violation("ü".repeat(128))).contains("the name exceeds 255 bytes");
        assertThat(IndexNameValidator.violation("ü".repeat(127))).isEmpty();
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "Customers|customers",
            "inventory/Customers|inventory_customers",
            "a b,c#d:e|a_b_c_d_e",
            "-leading|leading",
            "__double|double",
            "+Plus|plus",
            "a*b?c|a_b_c"
    })
    void shouldSanitizeToAValidName(String name, String expected) {
        final String sanitized = IndexNameValidator.sanitize(name, "_");
        assertThat(sanitized).isEqualTo(expected);
        assertThat(IndexNameValidator.violation(sanitized)).isEmpty();
    }

    @Test
    void shouldSanitizeWithCustomReplacement() {
        assertThat(IndexNameValidator.sanitize("a/b c", "-")).isEqualTo("a-b-c");
    }

    @Test
    void shouldTruncateSanitizedNameOnWholeCodePoints() {
        // Two-byte characters: 255 bytes is not a multiple of 2, so a naive cut splits a code point.
        final String sanitized = IndexNameValidator.sanitize("ü".repeat(200), "_");
        assertThat(sanitized.getBytes(StandardCharsets.UTF_8).length).isEqualTo(254);
        assertThat(sanitized).isEqualTo("ü".repeat(127));
        assertThat(IndexNameValidator.violation(sanitized)).isEmpty();
    }

    @Test
    void shouldTruncateSanitizedNameWithoutSplittingSurrogatePairs() {
        // A four-byte code point (outside the BMP) at the boundary must be dropped whole.
        final String name = "a".repeat(253) + "😀" + "bbbb";
        final String sanitized = IndexNameValidator.sanitize(name, "_");
        assertThat(sanitized).isEqualTo("a".repeat(253));
        assertThat(sanitized.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(255);
    }
}
