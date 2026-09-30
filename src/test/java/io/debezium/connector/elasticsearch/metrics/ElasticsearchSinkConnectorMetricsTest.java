/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ElasticsearchSinkConnectorMetrics}: the counters and gauges fed by the
 * sink and the bulk writer, and the JMX registration operators rely on per DDD-61 9.3.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ElasticsearchSinkConnectorMetricsTest {

    private static final String[] ATTRIBUTES = {
            "TotalNumberOfWrites",
            "TotalNumberOfDeletes",
            "TotalNumberOfTruncates",
            "TotalNumberOfFilteredEvents",
            "TotalNumberOfErrantRecords",
            "TotalNumberOfBulkRequests",
            "TotalNumberOfThrottleEvents",
            "TotalNumberOfRetries",
            "TotalNumberOfVersionConflicts",
            "EffectiveBatchSize",
            "MillisSinceLastSuccessfulBulkResponse",
            "BlockedResourceCount",
            "DistinctResourceCount",
            "ClusterVersion",
            "ApiCompatibilityMode" };

    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private final ElasticsearchSinkConnectorMetrics metrics = new ElasticsearchSinkConnectorMetrics("orders", "1");

    @AfterEach
    void afterEach() {
        metrics.unregister();
    }

    @Test
    void shouldStartAtZeroWithNoConnectionResolved() {
        assertThat(metrics.getTotalNumberOfWrites()).isZero();
        assertThat(metrics.getTotalNumberOfDeletes()).isZero();
        assertThat(metrics.getTotalNumberOfTruncates()).isZero();
        assertThat(metrics.getTotalNumberOfFilteredEvents()).isZero();
        assertThat(metrics.getTotalNumberOfErrantRecords()).isZero();
        assertThat(metrics.getTotalNumberOfBulkRequests()).isZero();
        assertThat(metrics.getTotalNumberOfThrottleEvents()).isZero();
        assertThat(metrics.getTotalNumberOfRetries()).isZero();
        assertThat(metrics.getTotalNumberOfVersionConflicts()).isZero();
        assertThat(metrics.getEffectiveBatchSize()).isZero();
        assertThat(metrics.getBlockedResourceCount()).isZero();
        assertThat(metrics.getDistinctResourceCount()).isZero();
        assertThat(metrics.getMillisSinceLastSuccessfulBulkResponse()).as("-1 before the first bulk response").isEqualTo(-1);
        assertThat(metrics.getClusterVersion()).isEmpty();
        assertThat(metrics.getApiCompatibilityMode()).isEmpty();
    }

    @Test
    void shouldAccumulateCounters() {
        metrics.written(2);
        metrics.written(3);
        metrics.deleted(1);
        metrics.deleted(4);
        metrics.truncated();
        metrics.truncated();
        metrics.filtered();
        metrics.filtered();
        metrics.filtered();
        metrics.errantRecordsReported(2);
        metrics.errantRecordsReported(1);
        metrics.bulkRequestCompleted();
        metrics.bulkRequestCompleted();
        metrics.retried();
        metrics.retried();
        metrics.versionConflict();

        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(5);
        assertThat(metrics.getTotalNumberOfDeletes()).isEqualTo(5);
        assertThat(metrics.getTotalNumberOfTruncates()).isEqualTo(2);
        assertThat(metrics.getTotalNumberOfFilteredEvents()).isEqualTo(3);
        assertThat(metrics.getTotalNumberOfErrantRecords()).isEqualTo(3);
        assertThat(metrics.getTotalNumberOfBulkRequests()).isEqualTo(2);
        assertThat(metrics.getTotalNumberOfRetries()).isEqualTo(2);
        assertThat(metrics.getTotalNumberOfVersionConflicts()).isEqualTo(1);
    }

    @Test
    void shouldReportGaugesAsTheLatestValue() {
        // Throttle events, batch size, and blocked resources are owned by the writer's round loop
        // and published as absolutes, so a later value replaces rather than adds.
        metrics.throttled(3);
        metrics.throttled(5);
        metrics.effectiveBatchSize(200);
        metrics.effectiveBatchSize(50);
        metrics.blockedResources(2);
        metrics.blockedResources(0);

        assertThat(metrics.getTotalNumberOfThrottleEvents()).isEqualTo(5);
        assertThat(metrics.getEffectiveBatchSize()).isEqualTo(50);
        assertThat(metrics.getBlockedResourceCount()).isZero();
    }

    @Test
    void shouldCountEachResourceOnce() {
        metrics.resourceWritten("inventory.customers");
        metrics.resourceWritten("inventory.orders");
        metrics.resourceWritten("inventory.customers");

        assertThat(metrics.getDistinctResourceCount()).isEqualTo(2);
    }

    @Test
    void shouldExposeTheResolvedConnection() {
        metrics.connectionResolved("8.19.20", "8");

        assertThat(metrics.getClusterVersion()).isEqualTo("8.19.20");
        assertThat(metrics.getApiCompatibilityMode()).isEqualTo("8");
    }

    @Test
    void shouldMeasureTimeSinceTheLastSuccessfulBulkResponse() {
        assertThat(metrics.getMillisSinceLastSuccessfulBulkResponse()).isEqualTo(-1);

        metrics.bulkRequestCompleted();

        // DDD-61 10.1.1: this is the observable behind progress-based health, so it must be a
        // non-negative age rather than a timestamp.
        assertThat(metrics.getMillisSinceLastSuccessfulBulkResponse()).isBetween(0L, 60_000L);
    }

    @Test
    void shouldIgnoreTableLifecycleCallbacks() {
        // Elasticsearch has no tables; the framework's callbacks must not disturb any counter.
        metrics.tableCreated();
        metrics.tableAltered();

        assertThat(metrics.getTotalNumberOfWrites()).isZero();
        assertThat(metrics.getTotalNumberOfBulkRequests()).isZero();
        assertThat(metrics.getDistinctResourceCount()).isZero();
    }

    @Test
    void shouldRegisterUnderTheConnectorAndTaskName() throws Exception {
        final ObjectName name = new ObjectName("debezium.elasticsearch:type=connector-metrics,context=sink,server=orders,task=1");
        assertThat(server.isRegistered(name)).isFalse();

        metrics.register();
        metrics.written(4);
        metrics.connectionResolved("9.5.1", "9");

        assertThat(server.isRegistered(name)).isTrue();
        assertThat(server.getAttribute(name, "TotalNumberOfWrites")).isEqualTo(4L);
        assertThat(server.getAttribute(name, "ClusterVersion")).isEqualTo("9.5.1");
        assertThat(server.getAttribute(name, "ApiCompatibilityMode")).isEqualTo("9");

        metrics.unregister();
        assertThat(server.isRegistered(name)).isFalse();
    }

    @Test
    void shouldExposeEveryDesignedAttributeOverJmx() throws Exception {
        metrics.register();

        final ObjectName name = new ObjectName("debezium.elasticsearch:type=connector-metrics,context=sink,server=orders,task=1");
        final String[] exposed = Arrays.stream(server.getMBeanInfo(name).getAttributes())
                .map(MBeanAttributeInfo::getName)
                .toArray(String[]::new);
        assertThat(exposed).containsExactlyInAnyOrder(ATTRIBUTES);
        for (String attribute : ATTRIBUTES) {
            assertThat(server.getAttribute(name, attribute)).as(attribute).isNotNull();
        }
    }

    @Test
    void shouldTolerateUnregisteringABeanThatIsNotRegistered() throws Exception {
        final ObjectName name = new ObjectName("debezium.elasticsearch:type=connector-metrics,context=sink,server=orders,task=1");

        assertThatCode(metrics::unregister).as("unregister before register").doesNotThrowAnyException();
        metrics.register();
        assertThat(server.isRegistered(name)).isTrue();

        metrics.unregister();
        assertThatCode(metrics::unregister).as("unregister twice").doesNotThrowAnyException();
        assertThat(server.isRegistered(name)).isFalse();
    }

    @Test
    void shouldQuoteAConnectorNameThatIsUnsafeInAnObjectName() throws Exception {
        // A comma is a property separator in an ObjectName; the name is quoted rather than the
        // bean being dropped, matching the other sinks.
        final ElasticsearchSinkConnectorMetrics quoted = new ElasticsearchSinkConnectorMetrics("orders,eu", "0");
        final ObjectName name = new ObjectName("debezium.elasticsearch:type=connector-metrics,context=sink,server=\"orders,eu\",task=0");
        try {
            quoted.register();
            quoted.written(1);

            assertThat(server.isRegistered(name)).isTrue();
            assertThat(server.getAttribute(name, "TotalNumberOfWrites")).isEqualTo(1L);
        }
        finally {
            quoted.unregister();
        }
    }
}
