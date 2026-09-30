/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.metrics;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.JmxUtils;
import io.debezium.sink.spi.SinkProgressListener;
import io.debezium.util.Sanitizer;

/**
 * JMX-registered metrics implementation, also serving as the {@link SinkProgressListener}.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class ElasticsearchSinkConnectorMetrics implements ElasticsearchSinkConnectorMetricsMXBean, SinkProgressListener {

    private static final String JMX_OBJECT_NAME_FORMAT = "debezium.elasticsearch:type=connector-metrics,context=sink,server=%s,task=%s";

    private final ObjectName objectName;

    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong truncates = new AtomicLong();
    private final AtomicLong filtered = new AtomicLong();
    private final AtomicLong errant = new AtomicLong();
    private final AtomicLong bulkRequests = new AtomicLong();
    private final AtomicLong throttleEvents = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong versionConflicts = new AtomicLong();
    private final AtomicInteger effectiveBatchSize = new AtomicInteger();
    private final AtomicLong lastSuccessEpochMs = new AtomicLong(-1);
    private final AtomicInteger blockedResources = new AtomicInteger();
    private final Set<String> distinctResources = ConcurrentHashMap.newKeySet();
    private final AtomicReference<String> clusterVersion = new AtomicReference<>("");
    private final AtomicReference<String> apiCompatibilityMode = new AtomicReference<>("");

    public ElasticsearchSinkConnectorMetrics(String connectorName, String taskId) {
        // Connector names are free-form; quoting unsafe values keeps them registrable rather than
        // silently dropping the bean.
        final String name = String.format(JMX_OBJECT_NAME_FORMAT, Sanitizer.jmxSanitize(connectorName), Sanitizer.jmxSanitize(taskId));
        try {
            this.objectName = new ObjectName(name);
        }
        catch (MalformedObjectNameException e) {
            throw new DebeziumException("Invalid metric name '" + name + "'", e);
        }
    }

    public void register() {
        JmxUtils.registerMXBean(objectName, this);
    }

    public void unregister() {
        JmxUtils.unregisterMXBean(objectName);
    }

    @Override
    public void written(long count) {
        writes.addAndGet(count);
    }

    @Override
    public void deleted(long count) {
        deletes.addAndGet(count);
    }

    @Override
    public void truncated() {
        truncates.incrementAndGet();
    }

    @Override
    public void filtered() {
        filtered.incrementAndGet();
    }

    @Override
    public void errantRecordsReported(long count) {
        errant.addAndGet(count);
    }

    @Override
    public void tableCreated() {
    }

    @Override
    public void tableAltered() {
    }

    public void bulkRequestCompleted() {
        bulkRequests.incrementAndGet();
        lastSuccessEpochMs.set(System.currentTimeMillis());
    }

    public void throttled(long totalThrottleEvents) {
        throttleEvents.set(totalThrottleEvents);
    }

    public void retried() {
        retries.incrementAndGet();
    }

    public void versionConflict() {
        versionConflicts.incrementAndGet();
    }

    public void effectiveBatchSize(int size) {
        effectiveBatchSize.set(size);
    }

    public void blockedResources(int count) {
        blockedResources.set(count);
    }

    public void resourceWritten(String resource) {
        distinctResources.add(resource);
    }

    public void connectionResolved(String version, String compatibilityMode) {
        clusterVersion.set(version);
        apiCompatibilityMode.set(compatibilityMode);
    }

    @Override
    public long getTotalNumberOfWrites() {
        return writes.get();
    }

    @Override
    public long getTotalNumberOfDeletes() {
        return deletes.get();
    }

    @Override
    public long getTotalNumberOfTruncates() {
        return truncates.get();
    }

    @Override
    public long getTotalNumberOfFilteredEvents() {
        return filtered.get();
    }

    @Override
    public long getTotalNumberOfErrantRecords() {
        return errant.get();
    }

    @Override
    public long getTotalNumberOfBulkRequests() {
        return bulkRequests.get();
    }

    @Override
    public long getTotalNumberOfThrottleEvents() {
        return throttleEvents.get();
    }

    @Override
    public long getTotalNumberOfRetries() {
        return retries.get();
    }

    @Override
    public long getTotalNumberOfVersionConflicts() {
        return versionConflicts.get();
    }

    @Override
    public int getEffectiveBatchSize() {
        return effectiveBatchSize.get();
    }

    @Override
    public long getMillisSinceLastSuccessfulBulkResponse() {
        final long last = lastSuccessEpochMs.get();
        return last < 0 ? -1 : System.currentTimeMillis() - last;
    }

    @Override
    public int getBlockedResourceCount() {
        return blockedResources.get();
    }

    @Override
    public int getDistinctResourceCount() {
        return distinctResources.size();
    }

    @Override
    public String getClusterVersion() {
        return clusterVersion.get();
    }

    @Override
    public String getApiCompatibilityMode() {
        return apiCompatibilityMode.get();
    }
}
