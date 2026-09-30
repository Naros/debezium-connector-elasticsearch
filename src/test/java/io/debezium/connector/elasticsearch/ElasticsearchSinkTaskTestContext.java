/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * A {@link SinkTaskContext} for driving the task outside Kafka Connect. Records routed to the
 * errant record reporter are captured so tests can assert on the dead letter queue path.
 *
 * @author Chris Cranford
 */
public class ElasticsearchSinkTaskTestContext implements SinkTaskContext {

    /**
     * One record the task handed to the errant record reporter, with the failure it attached.
     */
    public record ReportedRecord(SinkRecord record, Throwable error) {
    }

    private final Map<String, String> properties;
    private final List<ReportedRecord> reportedRecords = new CopyOnWriteArrayList<>();

    public ElasticsearchSinkTaskTestContext(Map<String, String> properties) {
        this.properties = properties;
    }

    public List<ReportedRecord> reportedRecords() {
        return reportedRecords;
    }

    @Override
    public Map<String, String> configs() {
        return properties;
    }

    @Override
    public ErrantRecordReporter errantRecordReporter() {
        return (record, error) -> {
            reportedRecords.add(new ReportedRecord(record, error));
            return completed();
        };
    }

    private static Future<Void> completed() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void offset(TopicPartition topicPartition, long offset) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void timeout(long timeoutMs) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public Set<TopicPartition> assignment() {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void pause(TopicPartition... partitions) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void resume(TopicPartition... partitions) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void requestCommit() {
        // no-op
    }

    @Override
    public PluginMetrics pluginMetrics() {
        return null;
    }
}
