/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.junit.jupiter;

import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import io.debezium.connector.elasticsearch.util.CloudEventsSinkRecordFactory;
import io.debezium.connector.elasticsearch.util.DebeziumSinkRecordFactory;
import io.debezium.connector.elasticsearch.util.FlatHeaderSinkRecordFactory;
import io.debezium.connector.elasticsearch.util.FlatSinkRecordFactory;

/**
 * Supplies one record factory per input contract of DDD-61 2 (envelope, flattened with marker
 * fields, flattened with marker headers, CloudEvents) so a parameterized test runs once per
 * contract and asserts they converge on the same index state.
 *
 * @author Chris Cranford
 */
public class SinkRecordFactoryArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
                Arguments.of(new DebeziumSinkRecordFactory()),
                Arguments.of(new FlatSinkRecordFactory()),
                Arguments.of(new FlatHeaderSinkRecordFactory()),
                Arguments.of(new CloudEventsSinkRecordFactory()));
    }
}
