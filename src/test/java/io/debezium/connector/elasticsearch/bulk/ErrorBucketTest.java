/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link ErrorBucket#parseOverride(String)}: the four assignable buckets, and
 * the refusal of {@code success} per DDD-61 section 9.1.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ErrorBucketTest {

    @ParameterizedTest
    @CsvSource({
            "transient, TRANSIENT",
            "record, RECORD",
            "resource_fatal, RESOURCE_FATAL",
            "task_fatal, TASK_FATAL",
            "TRANSIENT, TRANSIENT",
            "Resource_Fatal, RESOURCE_FATAL",
            "'  record  ', RECORD"
    })
    void shouldParseAssignableBucketsIgnoringCaseAndWhitespace(String value, ErrorBucket expected) {
        assertThat(ErrorBucket.parseOverride(value)).isEqualTo(expected);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "success", "SUCCESS", "", "unknown", "resource-fatal" })
    void shouldRejectSuccessAndUnknownNames(String value) {
        // DDD-61 9.1: reclassifying a failure as a success is how a document is lost silently.
        assertThat(ErrorBucket.parseOverride(value)).isNull();
    }
}
