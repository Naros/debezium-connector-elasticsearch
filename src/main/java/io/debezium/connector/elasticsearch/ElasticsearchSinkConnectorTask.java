/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static io.debezium.config.ConfigurationNames.TASK_ID_PROPERTY_NAME;
import static io.debezium.openlineage.dataset.DatasetMetadata.STREAM_DATASET_TYPE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DataStore.KAFKA;
import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.INPUT;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.InternalSinkRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.connector.common.UUIDUtils;
import io.debezium.connector.elasticsearch.client.ElasticsearchClientFactory;
import io.debezium.connector.elasticsearch.client.ElasticsearchConnection;
import io.debezium.connector.elasticsearch.metrics.ElasticsearchSinkConnectorMetrics;
import io.debezium.dlq.ErrorReporter;
import io.debezium.dlq.ErrorReporters;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetDataExtractor;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.util.Strings;

/**
 * The Kafka Connect adapter: task lifecycle, offset tracking, and delegation to the runtime
 * neutral {@link ElasticsearchChangeEventSink}. The Debezium Server change consumer delegates to
 * this task as well, which is what keeps the two runtimes from drifting.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 14"
 */
public class ElasticsearchSinkConnectorTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSinkConnectorTask.class);

    private static final Class<?>[] EMPTY_CLASS_ARRAY = new Class[0];
    private static final DatasetDataExtractor DATASET_DATA_EXTRACTOR = new DatasetDataExtractor();

    private enum State {
        RUNNING,
        STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);
    private final ReentrantLock stateLock = new ReentrantLock();
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private final Set<TopicPartition> assignedPartitions = new HashSet<>();

    private ElasticsearchConnection elasticsearchConnection;
    private ElasticsearchChangeEventSink changeEventSink;
    private ElasticsearchSinkConnectorMetrics metrics;
    private ConnectorContext connectorContext;
    private Throwable previousPutException;

    /**
     * Pre-Kafka-3.8 InternalSinkRecord access for resolving the original topic name (DBZ-6491);
     * the SinkRecord's topic may have been mutated by SMTs.
     */
    private boolean usePre380OriginalRecordAccess = false;
    private Method pre380OriginalRecordMethod = null;

    public ElasticsearchSinkConnectorTask() {
        try {
            pre380OriginalRecordMethod = InternalSinkRecord.class.getMethod("originalRecord", EMPTY_CLASS_ARRAY);
            usePre380OriginalRecordAccess = true;
        }
        catch (NoSuchMethodException | SecurityException e) {
            // Kafka 3.8+
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        stateLock.lock();
        try {
            final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
            final String connectorName = Strings.defaultIfBlank(
                    props.get(ConfigurationNames.CONNECTOR_NAME_PROPERTY), config.getConnectorName());
            final String taskId = props.getOrDefault(TASK_ID_PROPERTY_NAME, "0");
            connectorContext = new ConnectorContext(connectorName, Module.name(), taskId, Module.version(),
                    UUIDUtils.generateNewUUID(), Configuration.from(props).withMaskedPasswords().asMap());

            if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
                LOGGER.info("Connector has already been started");
                return;
            }

            DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.INITIAL);
            previousPutException = null;

            config.validate();

            elasticsearchConnection = new ElasticsearchClientFactory(config, props).connect();

            metrics = new ElasticsearchSinkConnectorMetrics(connectorName, taskId);
            metrics.register();

            final ErrorReporter errorReporter = ErrorReporters.fromContext(context);
            ErrorReporters.validateConfiguration(errorReporter, props);

            changeEventSink = new ElasticsearchChangeEventSink(config, elasticsearchConnection, errorReporter, metrics, props);
            DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RUNNING);
        }
        catch (RuntimeException e) {
            closeResources();
            state.set(State.STOPPED);
            throw e;
        }
        finally {
            stateLock.unlock();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (previousPutException != null) {
            LOGGER.error("Elasticsearch sink connector failure", previousPutException);
            DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RESTARTING, previousPutException);
            throw new ConnectException("Elasticsearch sink connector failure", previousPutException);
        }

        LOGGER.debug("Received {} changes.", records.size());
        records.forEach(record -> DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RUNNING,
                List.of(new DatasetMetadata(record.topic(), INPUT, STREAM_DATASET_TYPE, KAFKA,
                        DATASET_DATA_EXTRACTOR.extract(record)))));

        try {
            changeEventSink.execute(records);
            records.forEach(this::markProcessed);
        }
        catch (Throwable throwable) {
            LOGGER.error("Failed to process records: {}", throwable.getMessage(), throwable);
            previousPutException = throwable;
            records.forEach(this::markNotProcessed);
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            assignedPartitions.add(partition);
            // A previous assignment of this partition was flushed on close; the first record, or
            // the offsets Connect provides, establish the position for the new assignment.
            offsets.remove(partition);
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            assignedPartitions.remove(partition);
            offsets.remove(partition);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Prefer the offsets this task has confirmed; fall back to Connect's for partitions with
        // no tracked position yet.
        final Map<TopicPartition, OffsetAndMetadata> flushedOffsets = new HashMap<>();
        for (TopicPartition partition : assignedPartitions) {
            final OffsetAndMetadata offset = offsets.getOrDefault(partition, currentOffsets.get(partition));
            if (offset != null) {
                flushedOffsets.put(partition, offset);
            }
        }
        flush(flushedOffsets);
        return flushedOffsets;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (changeEventSink != null) {
            changeEventSink.flushAll();
        }
    }

    @Override
    public void stop() {
        stateLock.lock();
        try {
            closeResources();
            if (connectorContext != null) {
                DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.STOPPED);
            }
        }
        finally {
            previousPutException = null;
            changeEventSink = null;
            metrics = null;
            state.set(State.STOPPED);
            stateLock.unlock();
            if (connectorContext != null) {
                DebeziumOpenLineageEmitter.cleanup(connectorContext);
            }
        }
    }

    private void closeResources() {
        if (metrics != null) {
            metrics.unregister();
        }
        if (changeEventSink != null) {
            try {
                changeEventSink.close();
            }
            catch (Exception e) {
                LOGGER.error("Failed to close the change event sink gracefully.", e);
            }
        }
        if (elasticsearchConnection != null) {
            elasticsearchConnection.close();
            elasticsearchConnection = null;
        }
    }

    private void markProcessed(SinkRecord record) {
        final String topicName = getOriginalTopicName(record);
        if (Strings.isNullOrBlank(topicName)) {
            return;
        }
        final TopicPartition topicPartition = new TopicPartition(topicName, getOriginalKafkaPartition(record));
        offsets.put(topicPartition, new OffsetAndMetadata(getOriginalKafkaOffset(record) + 1L));
    }

    private void markNotProcessed(SinkRecord record) {
        // Only rewind if no earlier record already established an offset for the partition.
        final String topicName = getOriginalTopicName(record);
        if (Strings.isNullOrBlank(topicName)) {
            return;
        }
        final TopicPartition topicPartition = new TopicPartition(topicName, getOriginalKafkaPartition(record));
        offsets.putIfAbsent(topicPartition, new OffsetAndMetadata(getOriginalKafkaOffset(record)));
    }

    @SuppressWarnings("unchecked")
    private String getOriginalTopicName(SinkRecord record) {
        // DBZ-6491: resolve the original broker topic name; SinkRecord#topic may have been
        // mutated by SMTs.
        if (record instanceof InternalSinkRecord) {
            if (usePre380OriginalRecordAccess) {
                try {
                    return ((ConsumerRecord<byte[], byte[]>) pre380OriginalRecordMethod.invoke(record, (Object[]) EMPTY_CLASS_ARRAY)).topic();
                }
                catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new DebeziumException("Failed to access original record data", e);
                }
            }
            return ((InternalSinkRecord) record).context().original().topic();
        }
        try {
            // KIP-793 (Kafka 3.6): the pre-transformation topic is on the record itself.
            return record.originalTopic();
        }
        catch (NoSuchMethodError e) {
            return record.topic();
        }
    }

    private Integer getOriginalKafkaPartition(SinkRecord record) {
        try {
            return record.originalKafkaPartition();
        }
        catch (NoSuchMethodError e) {
            return record.kafkaPartition();
        }
    }

    private long getOriginalKafkaOffset(SinkRecord record) {
        try {
            return record.originalKafkaOffset();
        }
        catch (NoSuchMethodError e) {
            return record.kafkaOffset();
        }
    }
}
