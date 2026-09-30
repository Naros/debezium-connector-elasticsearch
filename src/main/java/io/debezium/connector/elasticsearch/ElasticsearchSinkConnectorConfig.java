/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.config.ConnectorConfigValidationHelper;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.elasticsearch.bulk.ErrorBucket;
import io.debezium.connector.elasticsearch.naming.ElasticsearchCollectionNamingStrategy;
import io.debezium.data.Envelope;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.filter.FieldFilterFactory;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.util.Strings;

/**
 * Connector configuration for the Elasticsearch sink, covering the full property surface and
 * the cross-property startup validations.
 * <p>
 * A validation only fires on values the user set explicitly; where a connector-level default
 * conflicts with something the user did choose, the default yields, the resolution is collected
 * and logged at WARN during {@link #validate()}, and startup proceeds.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 12"
 */
public class ElasticsearchSinkConnectorConfig implements SinkConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSinkConnectorConfig.class);

    public enum EventFormat implements EnumeratedValue {
        AUTO("auto"),
        DEBEZIUM("debezium"),
        PLAIN("plain");

        private final String value;

        EventFormat(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum TombstoneMode implements EnumeratedValue {
        AUTO("auto"),
        IGNORE("ignore"),
        DELETE("delete"),
        FAIL("fail");

        private final String value;

        TombstoneMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum WriteMethod implements EnumeratedValue {
        UPSERT("upsert"),
        INDEX("index"),
        CREATE("create"),
        UPDATE("update");

        private final String value;

        WriteMethod(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this write method operates through the Update API, which requires {@code _source}
         * on the target and is incompatible with native external versioning.
         */
        public boolean usesUpdateApi() {
            return this == UPSERT || this == UPDATE;
        }
    }

    public enum TruncateMode implements EnumeratedValue {
        IGNORE("ignore"),
        DELETE_BY_QUERY("delete_by_query"),
        RECREATE("recreate"),
        FAIL("fail");

        private final String value;

        TruncateMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean isDestructive() {
            return this == DELETE_BY_QUERY || this == RECREATE;
        }
    }

    public enum ResourceType implements EnumeratedValue {
        INDEX("index"),
        ALIAS_INDEX("alias_index"),
        DATA_STREAM("data_stream"),
        ALIAS_DATA_STREAM("alias_data_stream");

        private final String value;

        ResourceType(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean isDataStream() {
            return this == DATA_STREAM || this == ALIAS_DATA_STREAM;
        }
    }

    public enum InvalidNameHandling implements EnumeratedValue {
        FAIL("fail"),
        SANITIZE("sanitize"),
        ERROR_HANDLER("error_handler");

        private final String value;

        InvalidNameHandling(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum NonKeyOrdering implements EnumeratedValue {
        VERSION("version"),
        LAST_WRITE_WINS("last_write_wins");

        private final String value;

        NonKeyOrdering(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum VersionStrategyType implements EnumeratedValue {
        NONE("none"),
        SOURCE_LSN("source_lsn"),
        SOURCE_TS_MS("source_ts_ms"),
        RECORD_HEADER("record_header"),
        KAFKA_OFFSET("kafka_offset");

        private final String value;

        VersionStrategyType(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum VersionEnforcement implements EnumeratedValue {
        AUTO("auto"),
        EXTERNAL("external"),
        SCRIPT("script");

        private final String value;

        VersionEnforcement(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum VersionConflictMode implements EnumeratedValue {
        SKIP("skip"),
        WARN("warn"),
        FAIL("fail");

        private final String value;

        VersionConflictMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum DecimalOutputMode implements EnumeratedValue {
        NUMERIC("numeric"),
        STRING("string"),
        DOUBLE("double");

        private final String value;

        DecimalOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum TemporalOutputMode implements EnumeratedValue {
        ISO8601("iso8601"),
        EPOCH_MILLIS("epoch_millis"),
        EPOCH_NANOS("epoch_nanos");

        private final String value;

        TemporalOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum IntervalOutputMode implements EnumeratedValue {
        STRING("string"),
        NUMERIC("numeric");

        private final String value;

        IntervalOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum JsonOutputMode implements EnumeratedValue {
        OBJECT("object"),
        STRING("string");

        private final String value;

        JsonOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum BitsOutputMode implements EnumeratedValue {
        BASE64("base64"),
        BOOLEAN_ARRAY("boolean_array"),
        INTEGER("integer");

        private final String value;

        BitsOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum BinaryOutputMode implements EnumeratedValue {
        BASE64("base64"),
        HEX("hex");

        private final String value;

        BinaryOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum GeometryOutputMode implements EnumeratedValue {
        GEOJSON("geojson"),
        WKB("wkb");

        private final String value;

        GeometryOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum VectorOutputMode implements EnumeratedValue {
        ARRAY("array"),
        STRING("string");

        private final String value;

        VectorOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum MapOutputMode implements EnumeratedValue {
        COMPACT("compact"),
        ENTRIES("entries");

        private final String value;

        MapOutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum NullValueHandling implements EnumeratedValue {
        OMIT("omit"),
        WRITE_NULL("write_null");

        private final String value;

        NullValueHandling(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum FieldNameAdjustmentMode implements EnumeratedValue {
        NONE("none"),
        AVRO("avro"),
        AVRO_UNICODE("avro_unicode"),
        ELASTICSEARCH("elasticsearch");

        private final String value;

        FieldNameAdjustmentMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum MappingMode implements EnumeratedValue {
        NONE("none"),
        CREATE_IF_ABSENT("create_if_absent"),
        OVERWRITE("overwrite");

        private final String value;

        MappingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum MappingDynamic implements EnumeratedValue {
        STRICT_BUT_DLQ("strict_but_dlq"),
        TRUE("true"),
        FALSE("false"),
        RUNTIME("runtime");

        private final String value;

        MappingDynamic(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum StringMappingMode implements EnumeratedValue {
        TEXT_WITH_KEYWORD("text_with_keyword"),
        TEXT("text"),
        KEYWORD("keyword");

        private final String value;

        StringMappingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum StructMappingMode implements EnumeratedValue {
        OBJECT("object"),
        NESTED("nested");

        private final String value;

        StructMappingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum JsonMappingMode implements EnumeratedValue {
        OBJECT("object"),
        FLATTENED("flattened");

        private final String value;

        JsonMappingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum AuthMode implements EnumeratedValue {
        NONE("none"),
        BASIC("basic"),
        API_KEY("api_key"),
        BEARER("bearer"),
        KERBEROS("kerberos"),
        CUSTOM("custom");

        private final String value;

        AuthMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum TlsVerificationMode implements EnumeratedValue {
        FULL("full"),
        CERTIFICATE("certificate"),
        NONE("none");

        private final String value;

        TlsVerificationMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum ApiCompatibilityMode implements EnumeratedValue {
        AUTO("auto"),
        ENABLED("enabled"),
        DISABLED("disabled");

        private final String value;

        ApiCompatibilityMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    /**
     * The Kafka record attributes that {@code document.metadata.fields} can project into the
     * document body.
     */
    public enum MetadataField implements EnumeratedValue {
        KEY("key"),
        TOPIC("topic"),
        PARTITION("partition"),
        OFFSET("offset"),
        TIMESTAMP("timestamp");

        private final String value;

        MetadataField(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    // Connection (DDD-61 12.1)
    public static final String CONNECTION_URL = "connection.url";
    public static final String CONNECTION_CLOUD_ID = "connection.cloud.id";
    public static final String CONNECTION_AUTH_MODE = "connection.auth.mode";
    public static final String CONNECTION_USERNAME = "connection.username";
    public static final String CONNECTION_PASSWORD = "connection.password";
    public static final String CONNECTION_API_KEY = "connection.api.key";
    public static final String CONNECTION_BEARER_TOKEN = "connection.bearer.token";
    public static final String CONNECTION_KERBEROS_PRINCIPAL = "connection.kerberos.principal";
    public static final String CONNECTION_KERBEROS_KEYTAB = "connection.kerberos.keytab";
    public static final String CONNECTION_CREDENTIALS_PROVIDER_CLASS = "connection.credentials.provider.class";
    public static final String CONNECTION_HEADERS = "connection.headers";
    public static final String CONNECTION_COMPRESSION = "connection.compression";
    public static final String CONNECTION_TIMEOUT_MS = "connection.timeout.ms";
    public static final String CONNECTION_READ_TIMEOUT_MS = "connection.read.timeout.ms";
    public static final String CONNECTION_IDLE_TIMEOUT_MS = "connection.idle.timeout.ms";
    public static final String CONNECTION_SNIFF_ENABLED = "connection.sniff.enabled";
    public static final String CONNECTION_API_COMPATIBILITY_MODE = "connection.api.compatibility.mode";
    public static final String CONNECTION_TLS_KEYSTORE_LOCATION = "connection.tls.keystore.location";
    public static final String CONNECTION_TLS_KEYSTORE_PASSWORD = "connection.tls.keystore.password";
    public static final String CONNECTION_TLS_KEYSTORE_TYPE = "connection.tls.keystore.type";
    public static final String CONNECTION_TLS_KEY_PASSWORD = "connection.tls.key.password";
    public static final String CONNECTION_TLS_TRUSTSTORE_LOCATION = "connection.tls.truststore.location";
    public static final String CONNECTION_TLS_TRUSTSTORE_PASSWORD = "connection.tls.truststore.password";
    public static final String CONNECTION_TLS_TRUSTSTORE_TYPE = "connection.tls.truststore.type";
    public static final String CONNECTION_TLS_PROTOCOLS = "connection.tls.protocols";
    public static final String CONNECTION_TLS_CIPHER_SUITES = "connection.tls.cipher.suites";
    public static final String CONNECTION_TLS_CA_FINGERPRINT = "connection.tls.ca.fingerprint";
    public static final String CONNECTION_TLS_VERIFICATION_MODE = "connection.tls.verification.mode";
    public static final String CONNECTION_PROXY_HOST = "connection.proxy.host";
    public static final String CONNECTION_PROXY_PORT = "connection.proxy.port";
    public static final String CONNECTION_PROXY_USERNAME = "connection.proxy.username";
    public static final String CONNECTION_PROXY_PASSWORD = "connection.proxy.password";

    // Input contract (DDD-61 12.2)
    public static final String EVENT_FORMAT = "event.format";
    public static final String TOMBSTONE_MODE = "tombstone.mode";

    // Identity (DDD-61 12.3)
    public static final String DOCUMENT_ID_SEPARATOR = "document.id.separator";
    public static final String DOCUMENT_ID_NON_KEY_ORDERING = "document.id.non.key.ordering";
    public static final String DOCUMENT_METADATA_FIELDS = "document.metadata.fields";
    public static final String DOCUMENT_METADATA_PREFIX = "document.metadata.prefix";

    // Naming and resources (DDD-61 12.4)
    public static final String RESOURCE_NAME_TIMEZONE = "resource.name.timezone";
    public static final String RESOURCE_NAME_INVALID_HANDLING = "resource.name.invalid.handling";
    public static final String RESOURCE_NAME_REPLACEMENT = "resource.name.replacement";
    public static final String RESOURCE_TYPE = "resource.type";
    public static final String RESOURCE_AUTO_CREATE = "resource.auto.create";
    public static final String TOPIC_TO_RESOURCE_MAPPING = "topic.to.resource.mapping";
    public static final String DATA_STREAM_TYPE = "data.stream.type";
    public static final String DATA_STREAM_DATASET = "data.stream.dataset";
    public static final String DATA_STREAM_NAMESPACE = "data.stream.namespace";
    public static final String DATA_STREAM_TIMESTAMP_FIELD = "data.stream.timestamp.field";
    public static final String INDEX_ROUTING_FIELD = "index.routing.field";
    public static final String INGEST_PIPELINE = "ingest.pipeline";
    public static final String INGEST_PIPELINE_VALIDATE = "ingest.pipeline.validate";

    // Writes (DDD-61 12.5)
    public static final String WRITE_METHOD = "write.method";
    public static final String WRITE_METHOD_PER_OPERATION = "write.method.per.operation";
    public static final String TRUNCATE_MODE = "truncate.mode";
    public static final String TRUNCATE_ALLOWED_RESOURCES = "truncate.allowed.resources";
    public static final String TRUNCATE_ALLOW_WILDCARD = "truncate.allow.wildcard";

    // Ordering (DDD-61 12.6)
    public static final String VERSION_STRATEGY = "version.strategy";
    public static final String VERSION_ENFORCEMENT = "version.enforcement";
    public static final String VERSION_CONFLICT_MODE = "version.conflict.mode";

    // Batching and flow control (DDD-61 12.7)
    public static final String BULK_SIZE_BYTES = "bulk.size.bytes";
    public static final String LINGER_MS = "linger.ms";
    public static final String FLUSH_TIMEOUT_MS = "flush.timeout.ms";
    public static final String MAX_RETRIES = "max.retries";
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    public static final String RETRY_BACKOFF_MAX_MS = "retry.backoff.max.ms";
    public static final String PROGRESS_STALL_TIMEOUT_MS = "progress.stall.timeout.ms";
    public static final String MAX_REQUESTS_PER_SECOND = "max.requests.per.second";
    public static final String MAX_BYTES_PER_SECOND = "max.bytes.per.second";

    // Types and mapping (DDD-61 12.8)
    public static final String DECIMAL_OUTPUT_MODE = "decimal.output.mode";
    public static final String TEMPORAL_OUTPUT_MODE = "temporal.output.mode";
    public static final String INTERVAL_OUTPUT_MODE = "interval.output.mode";
    public static final String JSON_OUTPUT_MODE = "json.output.mode";
    public static final String BITS_OUTPUT_MODE = "bits.output.mode";
    public static final String BINARY_OUTPUT_MODE = "binary.output.mode";
    public static final String GEOMETRY_OUTPUT_MODE = "geometry.output.mode";
    public static final String VECTOR_OUTPUT_MODE = "vector.output.mode";
    public static final String MAP_OUTPUT_MODE = "map.output.mode";
    public static final String NULL_VALUE_HANDLING = "null.value.handling";
    public static final String FIELD_NAME_ADJUSTMENT_MODE = "field.name.adjustment.mode";
    public static final String FIELD_NAME_SEPARATOR_REPLACEMENT = "field.name.separator.replacement";
    public static final String MAPPING_MODE = "mapping.mode";
    public static final String MAPPING_DYNAMIC = "mapping.dynamic";
    public static final String MAPPING_COMPOSED_OF = "mapping.composed.of";
    public static final String MAPPING_SETTINGS = "mapping.settings";
    public static final String STRING_MAPPING_MODE = "string.mapping.mode";
    public static final String STRUCT_MAPPING_MODE = "struct.mapping.mode";
    public static final String JSON_MAPPING_MODE = "json.mapping.mode";

    // Errors (DDD-61 12.9)
    public static final String ERROR_CLASSIFICATION_OVERRIDES = "error.classification.overrides";
    public static final String LOG_SENSITIVE_DATA = "log.sensitive.data";

    private static final Set<String> MAPPING_SETTINGS_ALLOWED_KEYS = Set.of(
            "number_of_shards", "number_of_replicas", "refresh_interval", "index.default_pipeline");

    public static final Field CONNECTION_URL_FIELD = Field.create(CONNECTION_URL)
            .withDisplayName("Elasticsearch connection URLs")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Comma-separated list of Elasticsearch cluster URLs, e.g. 'https://es1:9200,https://es2:9200'. "
                    + "A context path is honored, e.g. 'https://gateway.internal/es'. Mutually exclusive with '" + CONNECTION_CLOUD_ID + "'.");

    public static final Field CONNECTION_CLOUD_ID_FIELD = Field.create(CONNECTION_CLOUD_ID)
            .withDisplayName("Elastic Cloud ID")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Elastic Cloud deployment identifier. Mutually exclusive with '" + CONNECTION_URL + "'.");

    public static final Field CONNECTION_AUTH_MODE_FIELD = Field.create(CONNECTION_AUTH_MODE)
            .withDisplayName("Authentication mode")
            .withEnum(AuthMode.class)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("How the connector authenticates: 'none', 'basic', 'api_key', 'bearer', 'kerberos', or 'custom'. "
                    + "Defaults to 'basic' when username/password are present, otherwise 'none'. "
                    + "A mode that does not match the supplied credentials is a startup error, never a silent fall-through to unauthenticated.");

    public static final Field CONNECTION_USERNAME_FIELD = Field.create(CONNECTION_USERNAME)
            .withDisplayName("Username")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Username for basic authentication.");

    public static final Field CONNECTION_PASSWORD_FIELD = Field.create(CONNECTION_PASSWORD)
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Password for basic authentication.");

    public static final Field CONNECTION_API_KEY_FIELD = Field.create(CONNECTION_API_KEY)
            .withDisplayName("API key")
            .withType(Type.PASSWORD)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Base64-encoded Elasticsearch API key ('id:api_key') used when '" + CONNECTION_AUTH_MODE + "' is 'api_key'.");

    public static final Field CONNECTION_BEARER_TOKEN_FIELD = Field.create(CONNECTION_BEARER_TOKEN)
            .withDisplayName("Bearer token")
            .withType(Type.PASSWORD)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Bearer token used when '" + CONNECTION_AUTH_MODE + "' is 'bearer'.");

    public static final Field CONNECTION_KERBEROS_PRINCIPAL_FIELD = Field.create(CONNECTION_KERBEROS_PRINCIPAL)
            .withDisplayName("Kerberos principal")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Kerberos user principal used when '" + CONNECTION_AUTH_MODE + "' is 'kerberos'.");

    public static final Field CONNECTION_KERBEROS_KEYTAB_FIELD = Field.create(CONNECTION_KERBEROS_KEYTAB)
            .withDisplayName("Kerberos keytab path")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Path to the Kerberos keytab file used when '" + CONNECTION_AUTH_MODE + "' is 'kerberos'.");

    public static final Field CONNECTION_CREDENTIALS_PROVIDER_CLASS_FIELD = Field.create(CONNECTION_CREDENTIALS_PROVIDER_CLASS)
            .withDisplayName("Custom credentials provider class")
            .withType(Type.CLASS)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isClassName)
            .withDescription("Fully-qualified class name of a ConnectionCredentialsProvider implementation used when '"
                    + CONNECTION_AUTH_MODE + "' is 'custom', e.g. for AWS SigV4 or other cloud-vendor schemes.");

    public static final Field CONNECTION_HEADERS_FIELD = Field.create(CONNECTION_HEADERS)
            .withDisplayName("Static HTTP headers")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Comma-separated list of static HTTP headers in 'Name:Value' form, e.g. for a gateway.");

    public static final Field CONNECTION_COMPRESSION_FIELD = Field.create(CONNECTION_COMPRESSION)
            .withDisplayName("Request compression")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to GZip-compress request bodies. Requires 'http.compression' on the cluster.");

    public static final Field CONNECTION_TIMEOUT_MS_FIELD = Field.create(CONNECTION_TIMEOUT_MS)
            .withDisplayName("Connect timeout (ms)")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5_000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("TCP connect timeout in milliseconds. The default is deliberately higher than the Confluent V1 "
                    + "default of 1000ms, which was the direct cause of frequent spurious startup failures against loaded clusters.");

    public static final Field CONNECTION_READ_TIMEOUT_MS_FIELD = Field.create(CONNECTION_READ_TIMEOUT_MS)
            .withDisplayName("Socket read timeout (ms)")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(60_000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Socket read timeout in milliseconds. The default is deliberately higher than the Confluent V1 "
                    + "default of 3000ms, which is shorter than a routine bulk request against a loaded cluster takes.");

    public static final Field CONNECTION_IDLE_TIMEOUT_MS_FIELD = Field.create(CONNECTION_IDLE_TIMEOUT_MS)
            .withDisplayName("Idle connection timeout (ms)")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(60_000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Drop idle pooled connections before the server does, in milliseconds.");

    public static final Field CONNECTION_SNIFF_ENABLED_FIELD = Field.create(CONNECTION_SNIFF_ENABLED)
            .withDisplayName("Node sniffing")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to periodically discover cluster nodes and add them to the client's round-robin set.");

    public static final Field CONNECTION_API_COMPATIBILITY_MODE_FIELD = Field.create(CONNECTION_API_COMPATIBILITY_MODE)
            .withDisplayName("REST API compatibility mode")
            .withEnum(ApiCompatibilityMode.class, ApiCompatibilityMode.AUTO)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls REST API compatibility headers toward a newer-major cluster: 'auto' (default) sets them "
                    + "when the startup handshake reports a cluster major greater than the client's, 'enabled' and 'disabled' force "
                    + "the choice. The supported-version handshake runs in every mode.");

    public static final Field CONNECTION_TLS_KEYSTORE_LOCATION_FIELD = Field.create(CONNECTION_TLS_KEYSTORE_LOCATION)
            .withDisplayName("TLS keystore location")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Path to the keystore holding the client certificate for mutual TLS.");

    public static final Field CONNECTION_TLS_KEYSTORE_PASSWORD_FIELD = Field.create(CONNECTION_TLS_KEYSTORE_PASSWORD)
            .withDisplayName("TLS keystore password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Password of the TLS keystore.");

    public static final Field CONNECTION_TLS_KEYSTORE_TYPE_FIELD = Field.create(CONNECTION_TLS_KEYSTORE_TYPE)
            .withDisplayName("TLS keystore type")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("PKCS12")
            .withDescription("Type of the TLS keystore, e.g. 'PKCS12' or 'JKS'.");

    public static final Field CONNECTION_TLS_KEY_PASSWORD_FIELD = Field.create(CONNECTION_TLS_KEY_PASSWORD)
            .withDisplayName("TLS key password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Password of the private key within the TLS keystore, when different from the keystore password.");

    public static final Field CONNECTION_TLS_TRUSTSTORE_LOCATION_FIELD = Field.create(CONNECTION_TLS_TRUSTSTORE_LOCATION)
            .withDisplayName("TLS truststore location")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Path to the truststore holding the cluster CA certificate.");

    public static final Field CONNECTION_TLS_TRUSTSTORE_PASSWORD_FIELD = Field.create(CONNECTION_TLS_TRUSTSTORE_PASSWORD)
            .withDisplayName("TLS truststore password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Password of the TLS truststore.");

    public static final Field CONNECTION_TLS_TRUSTSTORE_TYPE_FIELD = Field.create(CONNECTION_TLS_TRUSTSTORE_TYPE)
            .withDisplayName("TLS truststore type")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("PKCS12")
            .withDescription("Type of the TLS truststore, e.g. 'PKCS12' or 'JKS'.");

    public static final Field CONNECTION_TLS_PROTOCOLS_FIELD = Field.create(CONNECTION_TLS_PROTOCOLS)
            .withDisplayName("Enabled TLS protocols")
            .withType(Type.LIST)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Comma-separated list of enabled TLS protocol versions, e.g. 'TLSv1.3'.");

    public static final Field CONNECTION_TLS_CIPHER_SUITES_FIELD = Field.create(CONNECTION_TLS_CIPHER_SUITES)
            .withDisplayName("Enabled TLS cipher suites")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Comma-separated list of enabled TLS cipher suites.");

    public static final Field CONNECTION_TLS_CA_FINGERPRINT_FIELD = Field.create(CONNECTION_TLS_CA_FINGERPRINT)
            .withDisplayName("CA certificate SHA-256 fingerprint")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("SHA-256 fingerprint of the cluster's self-signed CA, as printed by Elasticsearch at first start. "
                    + "The supported way to trust a self-signed cluster without abandoning verification.");

    public static final Field CONNECTION_TLS_VERIFICATION_MODE_FIELD = Field.create(CONNECTION_TLS_VERIFICATION_MODE)
            .withDisplayName("TLS verification mode")
            .withEnum(TlsVerificationMode.class, TlsVerificationMode.FULL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("TLS verification, using Elastic's own vocabulary: 'full' (default) verifies the certificate chain "
                    + "and hostname; 'certificate' verifies the chain but not the hostname; 'none' performs no verification at all, "
                    + "which defeats TLS against an active attacker - prefer '" + CONNECTION_TLS_CA_FINGERPRINT + "' for self-signed clusters.");

    public static final Field CONNECTION_PROXY_HOST_FIELD = Field.create(CONNECTION_PROXY_HOST)
            .withDisplayName("Proxy host")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("HTTP proxy host.");

    public static final Field CONNECTION_PROXY_PORT_FIELD = Field.create(CONNECTION_PROXY_PORT)
            .withDisplayName("Proxy port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(Field::isPositiveInteger)
            .withDescription("HTTP proxy port.");

    public static final Field CONNECTION_PROXY_USERNAME_FIELD = Field.create(CONNECTION_PROXY_USERNAME)
            .withDisplayName("Proxy username")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("HTTP proxy username.");

    public static final Field CONNECTION_PROXY_PASSWORD_FIELD = Field.create(CONNECTION_PROXY_PASSWORD)
            .withDisplayName("Proxy password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("HTTP proxy password.");

    public static final Field EVENT_FORMAT_FIELD = Field.create(EVENT_FORMAT)
            .withDisplayName("Input event format")
            .withEnum(EventFormat.class, EventFormat.AUTO)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Input contract: 'auto' (default) detects per record between a Debezium envelope, a flattened "
                    + "Debezium record, and a plain record; 'debezium' requires a Debezium shape and treats plain records as "
                    + "record-level errors; 'plain' treats every value as an opaque document body.");

    public static final Field TOMBSTONE_MODE_FIELD = Field.create(TOMBSTONE_MODE)
            .withDisplayName("Tombstone handling")
            .withEnum(TombstoneMode.class, TombstoneMode.AUTO)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("What a null-valued record means: 'auto' (default) resolves to 'delete' on every input contract, "
                    + "because batch deduplication can collapse a 'd' event and its tombstone into just the tombstone, so acting "
                    + "on the tombstone is what keeps the delete; 'ignore', 'delete', and 'fail' apply uniformly.");

    public static final Field PRIMARY_KEY_MODE_FIELD = SinkConnectorConfig.PRIMARY_KEY_MODE_FIELD
            .withEnum(PrimaryKeyMode.class, PrimaryKeyMode.RECORD_KEY)
            .withDescription("How the document '_id' is derived: 'record_key' (default), 'record_value', 'record_header', "
                    + "'kafka' (topic+partition+offset), or 'none' (Elasticsearch generates an id; forfeits idempotent redelivery, "
                    + "so a redelivered record becomes a duplicate document).");

    public static final Field DOCUMENT_ID_SEPARATOR_FIELD = Field.create(DOCUMENT_ID_SEPARATOR)
            .withDisplayName("Composite id separator")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(":")
            .withDescription("Separator joining composite key fields into '_id', in the order given by 'primary.key.fields' or "
                    + "schema field order when unset. The order is fixed: reordering silently changes every '_id' in the index.");

    public static final Field DOCUMENT_ID_NON_KEY_ORDERING_FIELD = Field.create(DOCUMENT_ID_NON_KEY_ORDERING)
            .withDisplayName("Ordering decision for non-key document ids")
            .withEnum(NonKeyOrdering.class)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Required, with no default, when '_id' is derived from the record value or a header "
                    + "('record_value', 'record_header'): events for one document then span partitions Kafka never ordered "
                    + "relative to each other. 'version' enforces ordering through '" + VERSION_STRATEGY + "'; "
                    + "'last_write_wins' acknowledges arbitrary resolution.");

    public static final Field DOCUMENT_METADATA_FIELDS_FIELD = Field.create(DOCUMENT_METADATA_FIELDS)
            .withDisplayName("Kafka metadata projected into the document")
            .withType(Type.LIST)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Which of 'key', 'topic', 'partition', 'offset', 'timestamp' to project into the document body. "
                    + "Empty by default. Applied after field include/exclude filtering so an exclusion list written for source "
                    + "columns cannot strip them.");

    public static final Field DOCUMENT_METADATA_PREFIX_FIELD = Field.create(DOCUMENT_METADATA_PREFIX)
            .withDisplayName("Metadata field prefix")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("_kafka_")
            .withDescription("Prefix namespacing projected Kafka metadata fields away from source columns.");

    public static final Field COLLECTION_NAMING_STRATEGY_FIELD = SinkConnectorConfig.COLLECTION_NAMING_STRATEGY_FIELD
            .withDefault(ElasticsearchCollectionNamingStrategy.class.getName());

    public static final Field FIELD_EXCLUDE_LIST_FIELD = SinkConnectorConfig.FIELD_EXCLUDE_LIST_FIELD
            .withValidation(ElasticsearchSinkConnectorConfig::validateFieldExcludeList);

    public static final Field RESOURCE_NAME_TIMEZONE_FIELD = Field.create(RESOURCE_NAME_TIMEZONE)
            .withDisplayName("Timezone for ${date:...} placeholders")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("UTC")
            .withDescription("Timezone in which '${date:pattern}' naming placeholders format the event timestamp.");

    public static final Field RESOURCE_NAME_INVALID_HANDLING_FIELD = Field.create(RESOURCE_NAME_INVALID_HANDLING)
            .withDisplayName("Invalid resource name handling")
            .withEnum(InvalidNameHandling.class, InvalidNameHandling.FAIL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("What to do with a resolved name Elasticsearch cannot accept: 'fail' (default) rejects it loudly, "
                    + "naming the rule it broke; 'sanitize' lowercases and replaces disallowed characters with '"
                    + RESOURCE_NAME_REPLACEMENT + "', logging each distinct transformation once (opt-in, because a silent rename "
                    + "can merge two logically distinct sources into one index); 'error_handler' routes the record to the DLQ.");

    public static final Field RESOURCE_NAME_REPLACEMENT_FIELD = Field.create(RESOURCE_NAME_REPLACEMENT)
            .withDisplayName("Replacement for disallowed name characters")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("_")
            .withDescription("Replacement character used by 'resource.name.invalid.handling=sanitize'.");

    public static final Field RESOURCE_TYPE_FIELD = Field.create(RESOURCE_TYPE)
            .withDisplayName("Target resource type")
            .withEnum(ResourceType.class, ResourceType.INDEX)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Kind of Elasticsearch resource written to: 'index' (default), 'alias_index', 'data_stream', or "
                    + "'alias_data_stream'. Data streams accept only 'create' operations and cannot propagate deletes or truncates.");

    public static final Field RESOURCE_AUTO_CREATE_FIELD = Field.create(RESOURCE_AUTO_CREATE)
            .withDisplayName("Auto-create resources")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(true)
            .withDescription("When true, the connector creates the index or data stream and applies the generated mapping as one "
                    + "step, skipping both if the resource exists. When false, no existence call is made at all: the setting asserts "
                    + "the resource exists, and if it does not the first write fails with the Elasticsearch error.");

    public static final Field TOPIC_TO_RESOURCE_MAPPING_FIELD = Field.create(TOPIC_TO_RESOURCE_MAPPING)
            .withDisplayName("Explicit topic-to-resource overrides")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Comma-separated 'topic:resource' pairs consulted before 'collection.name.format', unconditionally. "
                    + "Unlike Confluent V2, this is an override of the naming strategy, not only a way to address pre-created resources.");

    public static final Field DATA_STREAM_TYPE_FIELD = Field.create(DATA_STREAM_TYPE)
            .withDisplayName("Data stream type")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("logs")
            .withDescription("First component of the composed data stream name '{type}-{dataset}-{namespace}', e.g. 'logs' or 'metrics'.");

    public static final Field DATA_STREAM_DATASET_FIELD = Field.create(DATA_STREAM_DATASET)
            .withDisplayName("Data stream dataset")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Second component of the composed data stream name '{type}-{dataset}-{namespace}'.");

    public static final Field DATA_STREAM_NAMESPACE_FIELD = Field.create(DATA_STREAM_NAMESPACE)
            .withDisplayName("Data stream namespace")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("${topic}")
            .withDescription("Third component of the composed data stream name '{type}-{dataset}-{namespace}'.");

    public static final Field DATA_STREAM_TIMESTAMP_FIELD_FIELD = Field.create(DATA_STREAM_TIMESTAMP_FIELD)
            .withDisplayName("Data stream @timestamp source field(s)")
            .withType(Type.LIST)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Field(s) mapped onto '@timestamp'; the first present in the record wins. When unset, the Kafka "
                    + "record timestamp is used.");

    public static final Field INDEX_ROUTING_FIELD_FIELD = Field.create(INDEX_ROUTING_FIELD)
            .withDisplayName("Routing field")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Dotted path of a document field whose value becomes '_routing'. A record missing the field is a "
                    + "record-level error: routing silently defaulting to '_id' puts documents on the wrong shard, which is "
                    + "unrecoverable without a reindex.");

    public static final Field INGEST_PIPELINE_FIELD = Field.create(INGEST_PIPELINE)
            .withDisplayName("Ingest pipeline")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Static ingest pipeline name or a placeholder expression using the naming vocabulary, "
                    + "e.g. 'pipeline-${source.table}'.");

    public static final Field INGEST_PIPELINE_VALIDATE_FIELD = Field.create(INGEST_PIPELINE_VALIDATE)
            .withDisplayName("Validate ingest pipelines at startup")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(true)
            .withDescription("Verify at startup that each statically-known ingest pipeline exists.");

    public static final Field WRITE_METHOD_FIELD = Field.create(WRITE_METHOD)
            .withDisplayName("Write method")
            .withEnum(WriteMethod.class, WriteMethod.UPSERT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Bulk action used for writes: 'upsert' (default; update with doc_as_upsert, creates or merges), "
                    + "'index' (full replacement), 'create' (fails if the document exists; required for data streams), or "
                    + "'update' (fails if the document does not exist).");

    public static final Field WRITE_METHOD_PER_OPERATION_FIELD = Field.create(WRITE_METHOD_PER_OPERATION)
            .withDisplayName("Write method per operation")
            .withType(Type.LIST)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Optional per-operation override map, e.g. 'c:create,u:upsert', for patterns where creates must not "
                    + "overwrite. Keys are the envelope operation codes 'c', 'r', 'u'.");

    public static final Field DELETE_ENABLED_FIELD = SinkConnectorConfig.DELETE_ENABLED_FIELD
            .withDefault(true)
            .withDescription("Whether delete events are propagated. Defaults to 'true' here, unlike other Debezium sinks: a CDC "
                    + "sink that silently drops deletes is precisely the failure this connector exists to eliminate. Set to "
                    + "'false' explicitly for an append-only index.");

    public static final Field TRUNCATE_MODE_FIELD = Field.create(TRUNCATE_MODE)
            .withDisplayName("Truncate handling")
            .withEnum(TruncateMode.class, TruncateMode.IGNORE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("What a truncate ('t') event does: 'ignore' (default) discards it; 'delete_by_query' empties the "
                    + "resolved resource; 'recreate' deletes the index and rebuilds it from the generated template; 'fail' stops "
                    + "the task. The shared 'truncate.enabled' is derived from this property and must not contradict it. The "
                    + "destructive modes require '" + TRUNCATE_ALLOWED_RESOURCES + "'.");

    public static final Field TRUNCATE_ALLOWED_RESOURCES_FIELD = Field.create(TRUNCATE_ALLOWED_RESOURCES)
            .withDisplayName("Resources a truncate may destroy")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Comma-separated exact names or glob patterns matched against the resolved resource name (not the "
                    + "topic). Required, with no default, whenever '" + TRUNCATE_MODE + "' is destructive: a truncate can destroy "
                    + "data this connector did not write, and the acknowledgement must state the blast radius.");

    public static final Field TRUNCATE_ALLOW_WILDCARD_FIELD = Field.create(TRUNCATE_ALLOW_WILDCARD)
            .withDisplayName("Allow a match-everything truncate pattern")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Required for a bare '*' (or equivalent) in '" + TRUNCATE_ALLOWED_RESOURCES + "'.");

    public static final Field VERSION_STRATEGY_FIELD = Field.create(VERSION_STRATEGY)
            .withDisplayName("External version strategy")
            .withEnum(VersionStrategyType.class, VersionStrategyType.NONE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Where the ordering key for external versioning comes from: 'none' (default; ordering holds by "
                    + "construction), 'source_lsn' (the source commit position, any source position type), 'source_ts_ms', "
                    + "'record_header', or 'kafka_offset' (comparable only within a partition; offered for migration parity).");

    public static final Field VERSION_ENFORCEMENT_FIELD = Field.create(VERSION_ENFORCEMENT)
            .withDisplayName("Version enforcement mechanism")
            .withEnum(VersionEnforcement.class, VersionEnforcement.AUTO)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How versioned writes are made conditional: 'auto' (default) chooses native external versioning "
                    + "when the key is a single long and the write method is 'index' or 'create', and the scripted guard otherwise; "
                    + "'external' and 'script' force the choice.");

    public static final Field VERSION_CONFLICT_MODE_FIELD = Field.create(VERSION_CONFLICT_MODE)
            .withDisplayName("Version conflict handling")
            .withEnum(VersionConflictMode.class, VersionConflictMode.SKIP)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("What a rejected stale write does beyond the 'version_conflicts' metric: 'skip', 'warn', or 'fail'.");

    public static final Field BULK_SIZE_BYTES_FIELD = Field.create(BULK_SIZE_BYTES)
            .withDisplayName("Maximum bulk request size (bytes)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5L * 1024 * 1024)
            .withValidation(ElasticsearchSinkConnectorConfig::validateBulkSizeBytes)
            .withDescription("Upper bound on the serialized size of one bulk request, so a batch of large documents cannot "
                    + "exceed the cluster's 'http.max_content_length'. -1 disables the bound.");

    public static final Field LINGER_MS_FIELD = Field.create(LINGER_MS)
            .withDisplayName("Partial batch linger (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0L)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("How long to accumulate a partial batch under low throughput before writing it. 0 (default) writes "
                    + "partial batches immediately. Confluent V1's equivalent property defaults to 1.");

    public static final Field FLUSH_TIMEOUT_MS_FIELD = Field.create(FLUSH_TIMEOUT_MS)
            .withDisplayName("Flush timeout (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(180_000L)
            .withValidation(Field::isPositiveLong)
            .withDescription("Upper bound on one flush cycle. Exceeding it pauses consumption rather than failing the task; a "
                    + "task dies only when it cannot make progress at all (see '" + PROGRESS_STALL_TIMEOUT_MS + "').");

    public static final Field MAX_RETRIES_FIELD = Field.create(MAX_RETRIES)
            .withDisplayName("Maximum retries")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(5)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("Retry budget for transient failures. Backpressure signals (429 rejections) are throttled, not "
                    + "counted against this budget: counting backpressure as failure converts a busy cluster into a dead task.");

    public static final Field RETRY_BACKOFF_MS_FIELD = Field.create(RETRY_BACKOFF_MS)
            .withDisplayName("Initial retry backoff (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(100L)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("Initial backoff between retries; doubles up to '" + RETRY_BACKOFF_MAX_MS + "'.");

    public static final Field RETRY_BACKOFF_MAX_MS_FIELD = Field.create(RETRY_BACKOFF_MAX_MS)
            .withDisplayName("Maximum retry backoff (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(30_000L)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("Ceiling for the exponential retry backoff.");

    public static final Field PROGRESS_STALL_TIMEOUT_MS_FIELD = Field.create(PROGRESS_STALL_TIMEOUT_MS)
            .withDisplayName("Progress stall timeout (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(300_000L)
            .withValidation(Field::isPositiveLong)
            .withDescription("A task that has made no observable progress within this window transitions to FAILED with the last "
                    + "Elasticsearch error attached, rather than reporting RUNNING while retrying forever. Thread liveness is not health.");

    public static final Field MAX_REQUESTS_PER_SECOND_FIELD = Field.create(MAX_REQUESTS_PER_SECOND)
            .withDisplayName("Proactive request rate ceiling")
            .withType(Type.DOUBLE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(ElasticsearchSinkConnectorConfig::validatePositiveDouble)
            .withDescription("Optional ceiling on bulk requests per second, for clusters shared with other workloads. Unset "
                    + "means no ceiling. Composes with the adaptive throttle: the effective rate is the lower of the two.");

    public static final Field MAX_BYTES_PER_SECOND_FIELD = Field.create(MAX_BYTES_PER_SECOND)
            .withDisplayName("Proactive byte rate ceiling")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(Field::isPositiveLong)
            .withDescription("Optional ceiling on bytes written per second. Unset means no ceiling.");

    public static final Field DECIMAL_OUTPUT_MODE_FIELD = Field.create(DECIMAL_OUTPUT_MODE)
            .withDisplayName("Decimal output")
            .withEnum(DecimalOutputMode.class, DecimalOutputMode.NUMERIC)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How Connect Decimal and VariableScaleDecimal values are written: 'numeric' (default; JSON number, "
                    + "string where the value exceeds IEEE-754 double precision), 'string', or 'double'.");

    public static final Field TEMPORAL_OUTPUT_MODE_FIELD = Field.create(TEMPORAL_OUTPUT_MODE)
            .withDisplayName("Temporal output")
            .withEnum(TemporalOutputMode.class, TemporalOutputMode.ISO8601)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How temporal logical types are written: 'iso8601' (default), 'epoch_millis', or 'epoch_nanos'.");

    public static final Field INTERVAL_OUTPUT_MODE_FIELD = Field.create(INTERVAL_OUTPUT_MODE)
            .withDisplayName("Interval output")
            .withEnum(IntervalOutputMode.class, IntervalOutputMode.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How interval/duration logical types are written: 'string' (ISO-8601 duration, default) or "
                    + "'numeric' (microseconds).");

    public static final Field JSON_OUTPUT_MODE_FIELD = Field.create(JSON_OUTPUT_MODE)
            .withDisplayName("JSON column output")
            .withEnum(JsonOutputMode.class, JsonOutputMode.OBJECT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How io.debezium.data.Json values are written: 'object' (default; embedded JSON, not a quoted "
                    + "string) or 'string'.");

    public static final Field BITS_OUTPUT_MODE_FIELD = Field.create(BITS_OUTPUT_MODE)
            .withDisplayName("Bits output")
            .withEnum(BitsOutputMode.class, BitsOutputMode.BASE64)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How io.debezium.data.Bits values are written: 'base64' (default), 'boolean_array', or 'integer' "
                    + "(bit width <= 64 only).");

    public static final Field BINARY_OUTPUT_MODE_FIELD = Field.create(BINARY_OUTPUT_MODE)
            .withDisplayName("Binary output")
            .withEnum(BinaryOutputMode.class, BinaryOutputMode.BASE64)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How raw BYTES values are written: 'base64' (default) or 'hex'.");

    public static final Field GEOMETRY_OUTPUT_MODE_FIELD = Field.create(GEOMETRY_OUTPUT_MODE)
            .withDisplayName("Geometry output")
            .withEnum(GeometryOutputMode.class, GeometryOutputMode.GEOJSON)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How geometry logical types are written: 'geojson' (default; mapped geo_point/geo_shape) or 'wkb'.");

    public static final Field VECTOR_OUTPUT_MODE_FIELD = Field.create(VECTOR_OUTPUT_MODE)
            .withDisplayName("Vector output")
            .withEnum(VectorOutputMode.class, VectorOutputMode.ARRAY)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How vector logical types are written: 'array' (default; JSON array of numbers, sparse vectors as "
                    + "an index-to-value object) or 'string'.");

    public static final Field MAP_OUTPUT_MODE_FIELD = Field.create(MAP_OUTPUT_MODE)
            .withDisplayName("Map output")
            .withEnum(MapOutputMode.class, MapOutputMode.COMPACT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How Connect MAP values with string keys are written: 'compact' (default; '{\"k\":\"v\"}') or "
                    + "'entries' ('[{\"key\":..,\"value\":..}]'). Maps with non-string keys always use the entries form.");

    public static final Field NULL_VALUE_HANDLING_FIELD = Field.create(NULL_VALUE_HANDLING)
            .withDisplayName("Null field handling")
            .withEnum(NullValueHandling.class, NullValueHandling.OMIT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Whether null-valued fields appear in documents: 'omit' (default) keeps upserts from clobbering "
                    + "fields the source did not send; 'write_null' is for sources whose 'after' block is always complete and "
                    + "where an explicit null must overwrite.");

    public static final Field FIELD_NAME_ADJUSTMENT_MODE_FIELD = Field.create(FIELD_NAME_ADJUSTMENT_MODE)
            .withDisplayName("Field name adjustment")
            .withEnum(FieldNameAdjustmentMode.class, FieldNameAdjustmentMode.NONE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Field name sanitization: 'none' (default), 'avro', 'avro_unicode', or 'elasticsearch', which "
                    + "replaces '.' with '" + FIELD_NAME_SEPARATOR_REPLACEMENT + "' to avoid implicit object nesting from dotted "
                    + "column names.");

    public static final Field FIELD_NAME_SEPARATOR_REPLACEMENT_FIELD = Field.create(FIELD_NAME_SEPARATOR_REPLACEMENT)
            .withDisplayName("Dot replacement in field names")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault("_")
            .withDescription("Replacement for '.' under 'field.name.adjustment.mode=elasticsearch'.");

    public static final Field MAPPING_MODE_FIELD = Field.create(MAPPING_MODE)
            .withDisplayName("Mapping management")
            .withEnum(MappingMode.class, MappingMode.CREATE_IF_ABSENT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether the connector derives the index mapping from the Connect schema: 'create_if_absent' "
                    + "(default) generates and applies it before first write at template priority 0, so any user-authored "
                    + "template wins by priority; 'overwrite' always applies it (a template change does not retroactively alter "
                    + "existing indices); 'none' leaves mapping entirely to Elasticsearch - a first-class escape hatch for users "
                    + "who have already modeled their index.");

    public static final Field MAPPING_DYNAMIC_FIELD = Field.create(MAPPING_DYNAMIC)
            .withDisplayName("Dynamic mapping behavior")
            .withEnum(MappingDynamic.class, MappingDynamic.STRICT_BUT_DLQ)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The 'dynamic' setting of the generated mapping: 'strict_but_dlq' (default) sets 'strict' so an "
                    + "unmapped field is rejected rather than silently typed, and the connector routes the record to the error "
                    + "handler naming the field; 'true', 'false', and 'runtime' are passed through.");

    public static final Field MAPPING_COMPOSED_OF_FIELD = Field.create(MAPPING_COMPOSED_OF)
            .withDisplayName("User component templates")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("User-managed component template names composed into the generated index template ahead of the "
                    + "connector's own. This is how ILM and lifecycle policies are reached: author them once in Elasticsearch "
                    + "and name them here.");

    public static final Field MAPPING_SETTINGS_FIELD = Field.create(MAPPING_SETTINGS)
            .withDisplayName("Index settings passthrough")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Narrow 'key=value' passthrough for the few index settings that materially affect a sink: "
                    + "number_of_shards, number_of_replicas, refresh_interval, index.default_pipeline. Empty by default.");

    public static final Field STRING_MAPPING_MODE_FIELD = Field.create(STRING_MAPPING_MODE)
            .withDisplayName("String field mapping")
            .withEnum(StringMappingMode.class, StringMappingMode.TEXT_WITH_KEYWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How STRING fields are mapped: 'text_with_keyword' (default), 'text', or 'keyword'.");

    public static final Field STRUCT_MAPPING_MODE_FIELD = Field.create(STRUCT_MAPPING_MODE)
            .withDisplayName("Struct field mapping")
            .withEnum(StructMappingMode.class, StructMappingMode.OBJECT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How STRUCT fields are mapped: 'object' (default) or 'nested'.");

    public static final Field JSON_MAPPING_MODE_FIELD = Field.create(JSON_MAPPING_MODE)
            .withDisplayName("JSON field mapping")
            .withEnum(JsonMappingMode.class, JsonMappingMode.OBJECT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("How io.debezium.data.Json fields are mapped: 'object' (default) or 'flattened'.");

    public static final Field ERROR_CLASSIFICATION_OVERRIDES_FIELD = Field.create(ERROR_CLASSIFICATION_OVERRIDES)
            .withDisplayName("Error classification overrides")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Comma-separated '<es-error-type>:<bucket>' pairs moving a specific Elasticsearch error type "
                    + "between classification buckets ('transient', 'record', 'resource_fatal', 'task_fatal') without waiting for "
                    + "a release, e.g. 'circuit_breaking_exception:record'. 'success' is deliberately not assignable: reclassifying "
                    + "a failure as success is how a document is lost silently.");

    public static final Field LOG_SENSITIVE_DATA_FIELD = Field.create(LOG_SENSITIVE_DATA)
            .withDisplayName("Log document bodies on failure")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Whether document bodies may appear in error logs. When false (default), failures log record "
                    + "coordinates, '_id', index, and the Elasticsearch error, but never the payload.");

    protected static final ConfigDefinition CONFIG_DEFINITION = ConfigDefinition.editor()
            .group(Field.Group.CONNECTION,
                    CONNECTION_URL_FIELD,
                    CONNECTION_CLOUD_ID_FIELD,
                    CONNECTION_AUTH_MODE_FIELD,
                    CONNECTION_USERNAME_FIELD,
                    CONNECTION_PASSWORD_FIELD,
                    CONNECTION_API_KEY_FIELD,
                    CONNECTION_BEARER_TOKEN_FIELD,
                    CONNECTION_KERBEROS_PRINCIPAL_FIELD,
                    CONNECTION_KERBEROS_KEYTAB_FIELD,
                    CONNECTION_CREDENTIALS_PROVIDER_CLASS_FIELD,
                    CONNECTION_HEADERS_FIELD,
                    CONNECTION_COMPRESSION_FIELD,
                    CONNECTION_TIMEOUT_MS_FIELD,
                    CONNECTION_READ_TIMEOUT_MS_FIELD,
                    CONNECTION_IDLE_TIMEOUT_MS_FIELD,
                    CONNECTION_SNIFF_ENABLED_FIELD,
                    CONNECTION_API_COMPATIBILITY_MODE_FIELD,
                    CONNECTION_TLS_KEYSTORE_LOCATION_FIELD,
                    CONNECTION_TLS_KEYSTORE_PASSWORD_FIELD,
                    CONNECTION_TLS_KEYSTORE_TYPE_FIELD,
                    CONNECTION_TLS_KEY_PASSWORD_FIELD,
                    CONNECTION_TLS_TRUSTSTORE_LOCATION_FIELD,
                    CONNECTION_TLS_TRUSTSTORE_PASSWORD_FIELD,
                    CONNECTION_TLS_TRUSTSTORE_TYPE_FIELD,
                    CONNECTION_TLS_PROTOCOLS_FIELD,
                    CONNECTION_TLS_CIPHER_SUITES_FIELD,
                    CONNECTION_TLS_CA_FINGERPRINT_FIELD,
                    CONNECTION_TLS_VERIFICATION_MODE_FIELD,
                    CONNECTION_PROXY_HOST_FIELD,
                    CONNECTION_PROXY_PORT_FIELD,
                    CONNECTION_PROXY_USERNAME_FIELD,
                    CONNECTION_PROXY_PASSWORD_FIELD)
            .group(Field.Group.CONNECTOR,
                    EVENT_FORMAT_FIELD,
                    TOMBSTONE_MODE_FIELD,
                    PRIMARY_KEY_MODE_FIELD,
                    SinkConnectorConfig.PRIMARY_KEY_FIELDS_FIELD,
                    DOCUMENT_ID_SEPARATOR_FIELD,
                    DOCUMENT_ID_NON_KEY_ORDERING_FIELD,
                    DOCUMENT_METADATA_FIELDS_FIELD,
                    DOCUMENT_METADATA_PREFIX_FIELD,
                    SinkConnectorConfig.COLLECTION_NAME_FORMAT_FIELD,
                    COLLECTION_NAMING_STRATEGY_FIELD,
                    RESOURCE_NAME_TIMEZONE_FIELD,
                    RESOURCE_NAME_INVALID_HANDLING_FIELD,
                    RESOURCE_NAME_REPLACEMENT_FIELD,
                    RESOURCE_TYPE_FIELD,
                    RESOURCE_AUTO_CREATE_FIELD,
                    TOPIC_TO_RESOURCE_MAPPING_FIELD,
                    DATA_STREAM_TYPE_FIELD,
                    DATA_STREAM_DATASET_FIELD,
                    DATA_STREAM_NAMESPACE_FIELD,
                    DATA_STREAM_TIMESTAMP_FIELD_FIELD,
                    INDEX_ROUTING_FIELD_FIELD,
                    INGEST_PIPELINE_FIELD,
                    INGEST_PIPELINE_VALIDATE_FIELD,
                    WRITE_METHOD_FIELD,
                    WRITE_METHOD_PER_OPERATION_FIELD,
                    DELETE_ENABLED_FIELD,
                    SinkConnectorConfig.TRUNCATE_ENABLED_FIELD,
                    TRUNCATE_MODE_FIELD,
                    TRUNCATE_ALLOWED_RESOURCES_FIELD,
                    TRUNCATE_ALLOW_WILDCARD_FIELD,
                    VERSION_STRATEGY_FIELD,
                    VERSION_ENFORCEMENT_FIELD,
                    VERSION_CONFLICT_MODE_FIELD,
                    SinkConnectorConfig.BATCH_SIZE_FIELD,
                    SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE_FIELD,
                    BULK_SIZE_BYTES_FIELD,
                    LINGER_MS_FIELD,
                    FLUSH_TIMEOUT_MS_FIELD,
                    MAX_RETRIES_FIELD,
                    RETRY_BACKOFF_MS_FIELD,
                    RETRY_BACKOFF_MAX_MS_FIELD,
                    PROGRESS_STALL_TIMEOUT_MS_FIELD,
                    MAX_REQUESTS_PER_SECOND_FIELD,
                    MAX_BYTES_PER_SECOND_FIELD,
                    SinkConnectorConfig.FIELD_INCLUDE_LIST_FIELD,
                    FIELD_EXCLUDE_LIST_FIELD,
                    SinkConnectorConfig.CLOUDEVENTS_SCHEMA_NAME_PATTERN_FIELD)
            .group(Field.Group.CONNECTOR_ADVANCED,
                    DECIMAL_OUTPUT_MODE_FIELD,
                    TEMPORAL_OUTPUT_MODE_FIELD,
                    INTERVAL_OUTPUT_MODE_FIELD,
                    JSON_OUTPUT_MODE_FIELD,
                    BITS_OUTPUT_MODE_FIELD,
                    BINARY_OUTPUT_MODE_FIELD,
                    GEOMETRY_OUTPUT_MODE_FIELD,
                    VECTOR_OUTPUT_MODE_FIELD,
                    MAP_OUTPUT_MODE_FIELD,
                    NULL_VALUE_HANDLING_FIELD,
                    FIELD_NAME_ADJUSTMENT_MODE_FIELD,
                    FIELD_NAME_SEPARATOR_REPLACEMENT_FIELD,
                    MAPPING_MODE_FIELD,
                    MAPPING_DYNAMIC_FIELD,
                    MAPPING_COMPOSED_OF_FIELD,
                    MAPPING_SETTINGS_FIELD,
                    STRING_MAPPING_MODE_FIELD,
                    STRUCT_MAPPING_MODE_FIELD,
                    JSON_MAPPING_MODE_FIELD,
                    ERROR_CLASSIFICATION_OVERRIDES_FIELD,
                    LOG_SENSITIVE_DATA_FIELD)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final Configuration config;
    private final List<String> startupWarnings = new ArrayList<>();

    private final EventFormat eventFormat;
    private final TombstoneMode tombstoneMode;
    private final PrimaryKeyMode primaryKeyMode;
    private final Set<String> primaryKeyFields;
    private final String documentIdSeparator;
    private final NonKeyOrdering nonKeyOrdering;
    private final Set<MetadataField> documentMetadataFields;
    private final String documentMetadataPrefix;
    private final String collectionNameFormat;
    private final CollectionNamingStrategy collectionNamingStrategy;
    private final String resourceNameTimezone;
    private final InvalidNameHandling invalidNameHandling;
    private final String resourceNameReplacement;
    private final ResourceType resourceType;
    private final boolean resourceAutoCreate;
    private final Map<String, String> topicToResourceMapping;
    private final String indexRoutingField;
    private final String ingestPipeline;
    private final boolean ingestPipelineValidate;
    private final WriteMethod writeMethod;
    private final Map<Envelope.Operation, WriteMethod> writeMethodPerOperation;
    private final boolean deleteEnabled;
    private final TruncateMode truncateMode;
    private final List<String> truncateAllowedResources;
    private final boolean truncateAllowWildcard;
    private final VersionStrategyType versionStrategy;
    private final VersionEnforcement versionEnforcement;
    private final VersionConflictMode versionConflictMode;
    private final int batchSize;
    private final long bulkSizeBytes;
    private final long lingerMs;
    private final long flushTimeoutMs;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private final long progressStallTimeoutMs;
    private final Double maxRequestsPerSecond;
    private final Long maxBytesPerSecond;
    private final DecimalOutputMode decimalOutputMode;
    private final TemporalOutputMode temporalOutputMode;
    private final IntervalOutputMode intervalOutputMode;
    private final JsonOutputMode jsonOutputMode;
    private final BitsOutputMode bitsOutputMode;
    private final BinaryOutputMode binaryOutputMode;
    private final GeometryOutputMode geometryOutputMode;
    private final VectorOutputMode vectorOutputMode;
    private final MapOutputMode mapOutputMode;
    private final NullValueHandling nullValueHandling;
    private final FieldNameAdjustmentMode fieldNameAdjustmentMode;
    private final String fieldNameSeparatorReplacement;
    private final MappingMode mappingMode;
    private final MappingDynamic mappingDynamic;
    private final List<String> mappingComposedOf;
    private final Map<String, String> mappingSettings;
    private final StringMappingMode stringMappingMode;
    private final StructMappingMode structMappingMode;
    private final JsonMappingMode jsonMappingMode;
    private final Map<String, ErrorBucket> errorClassificationOverrides;
    private final boolean logSensitiveData;
    private final AuthMode authMode;
    private final FieldNameFilter fieldsFilter;
    private final String cloudEventsSchemaNamePattern;

    public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
        this.config = Configuration.from(props);

        this.eventFormat = enumValue(EventFormat.class, EVENT_FORMAT_FIELD);
        this.tombstoneMode = enumValue(TombstoneMode.class, TOMBSTONE_MODE_FIELD);
        this.primaryKeyMode = PrimaryKeyMode.parse(config.getString(PRIMARY_KEY_MODE_FIELD));
        // Ordered and trimmed: the configured order is the '_id' composition order (DDD-61 3).
        this.primaryKeyFields = new LinkedHashSet<>(
                Strings.listOf(config.getString(SinkConnectorConfig.PRIMARY_KEY_FIELDS_FIELD), s -> s.split(","), String::trim)
                        .stream().filter(s -> !s.isEmpty()).toList());
        this.documentIdSeparator = config.getString(DOCUMENT_ID_SEPARATOR_FIELD);
        this.nonKeyOrdering = EnumeratedValue.parse(NonKeyOrdering.class, config.getString(DOCUMENT_ID_NON_KEY_ORDERING_FIELD));
        this.documentMetadataFields = parseMetadataFields(config.getString(DOCUMENT_METADATA_FIELDS_FIELD));
        this.documentMetadataPrefix = config.getString(DOCUMENT_METADATA_PREFIX_FIELD);
        this.collectionNameFormat = config.getString(SinkConnectorConfig.COLLECTION_NAME_FORMAT_FIELD);
        this.resourceNameTimezone = config.getString(RESOURCE_NAME_TIMEZONE_FIELD);
        this.invalidNameHandling = enumValue(InvalidNameHandling.class, RESOURCE_NAME_INVALID_HANDLING_FIELD);
        this.resourceNameReplacement = config.getString(RESOURCE_NAME_REPLACEMENT_FIELD);
        this.resourceType = enumValue(ResourceType.class, RESOURCE_TYPE_FIELD);
        this.resourceAutoCreate = config.getBoolean(RESOURCE_AUTO_CREATE_FIELD);
        this.topicToResourceMapping = parsePairs(config.getString(TOPIC_TO_RESOURCE_MAPPING_FIELD), TOPIC_TO_RESOURCE_MAPPING);
        this.indexRoutingField = config.getString(INDEX_ROUTING_FIELD_FIELD);
        this.ingestPipeline = config.getString(INGEST_PIPELINE_FIELD);
        this.ingestPipelineValidate = config.getBoolean(INGEST_PIPELINE_VALIDATE_FIELD);
        this.truncateMode = enumValue(TruncateMode.class, TRUNCATE_MODE_FIELD);
        this.truncateAllowedResources = Strings.listOf(config.getString(TRUNCATE_ALLOWED_RESOURCES_FIELD), s -> s.split(","), String::trim)
                .stream().filter(s -> !s.isEmpty()).toList();
        this.truncateAllowWildcard = config.getBoolean(TRUNCATE_ALLOW_WILDCARD_FIELD);
        this.versionStrategy = enumValue(VersionStrategyType.class, VERSION_STRATEGY_FIELD);
        this.versionEnforcement = enumValue(VersionEnforcement.class, VERSION_ENFORCEMENT_FIELD);
        this.versionConflictMode = enumValue(VersionConflictMode.class, VERSION_CONFLICT_MODE_FIELD);
        this.batchSize = config.getInteger(SinkConnectorConfig.BATCH_SIZE_FIELD);
        this.bulkSizeBytes = config.getLong(BULK_SIZE_BYTES_FIELD);
        this.lingerMs = config.getLong(LINGER_MS_FIELD);
        this.flushTimeoutMs = config.getLong(FLUSH_TIMEOUT_MS_FIELD);
        this.maxRetries = config.getInteger(MAX_RETRIES_FIELD);
        this.retryBackoffMs = config.getLong(RETRY_BACKOFF_MS_FIELD);
        this.retryBackoffMaxMs = config.getLong(RETRY_BACKOFF_MAX_MS_FIELD);
        this.progressStallTimeoutMs = config.getLong(PROGRESS_STALL_TIMEOUT_MS_FIELD);
        // Parsed leniently here; a malformed value is reported by the field validator in validate()
        // with the property named, rather than as a bare NumberFormatException from construction.
        this.maxRequestsPerSecond = config.hasKey(MAX_REQUESTS_PER_SECOND) ? parseDoubleOrNull(config.getString(MAX_REQUESTS_PER_SECOND_FIELD)) : null;
        this.maxBytesPerSecond = config.hasKey(MAX_BYTES_PER_SECOND) ? parseLongOrNull(config.getString(MAX_BYTES_PER_SECOND_FIELD)) : null;
        this.decimalOutputMode = enumValue(DecimalOutputMode.class, DECIMAL_OUTPUT_MODE_FIELD);
        this.temporalOutputMode = enumValue(TemporalOutputMode.class, TEMPORAL_OUTPUT_MODE_FIELD);
        this.intervalOutputMode = enumValue(IntervalOutputMode.class, INTERVAL_OUTPUT_MODE_FIELD);
        this.jsonOutputMode = enumValue(JsonOutputMode.class, JSON_OUTPUT_MODE_FIELD);
        this.bitsOutputMode = enumValue(BitsOutputMode.class, BITS_OUTPUT_MODE_FIELD);
        this.binaryOutputMode = enumValue(BinaryOutputMode.class, BINARY_OUTPUT_MODE_FIELD);
        this.geometryOutputMode = enumValue(GeometryOutputMode.class, GEOMETRY_OUTPUT_MODE_FIELD);
        this.vectorOutputMode = enumValue(VectorOutputMode.class, VECTOR_OUTPUT_MODE_FIELD);
        this.mapOutputMode = enumValue(MapOutputMode.class, MAP_OUTPUT_MODE_FIELD);
        this.nullValueHandling = enumValue(NullValueHandling.class, NULL_VALUE_HANDLING_FIELD);
        this.fieldNameAdjustmentMode = enumValue(FieldNameAdjustmentMode.class, FIELD_NAME_ADJUSTMENT_MODE_FIELD);
        this.fieldNameSeparatorReplacement = config.getString(FIELD_NAME_SEPARATOR_REPLACEMENT_FIELD);
        this.mappingMode = enumValue(MappingMode.class, MAPPING_MODE_FIELD);
        this.mappingDynamic = enumValue(MappingDynamic.class, MAPPING_DYNAMIC_FIELD);
        this.mappingComposedOf = Strings.listOf(config.getString(MAPPING_COMPOSED_OF_FIELD), s -> s.split(","), String::trim);
        this.mappingSettings = parseMappingSettings(config.getString(MAPPING_SETTINGS_FIELD));
        this.stringMappingMode = enumValue(StringMappingMode.class, STRING_MAPPING_MODE_FIELD);
        this.structMappingMode = enumValue(StructMappingMode.class, STRUCT_MAPPING_MODE_FIELD);
        this.jsonMappingMode = enumValue(JsonMappingMode.class, JSON_MAPPING_MODE_FIELD);
        this.errorClassificationOverrides = parseClassificationOverrides(config.getString(ERROR_CLASSIFICATION_OVERRIDES_FIELD));
        this.logSensitiveData = config.getBoolean(LOG_SENSITIVE_DATA_FIELD);
        this.cloudEventsSchemaNamePattern = config.getString(SinkConnectorConfig.CLOUDEVENTS_SCHEMA_NAME_PATTERN_FIELD);
        this.authMode = resolveAuthMode();
        this.writeMethod = resolveWriteMethod();
        this.deleteEnabled = resolveDeleteEnabled();
        this.writeMethodPerOperation = parseWriteMethodPerOperation(config.getString(WRITE_METHOD_PER_OPERATION_FIELD));
        this.fieldsFilter = FieldFilterFactory.createFieldFilter(
                config.getString(SinkConnectorConfig.FIELD_INCLUDE_LIST_FIELD),
                config.getString(SinkConnectorConfig.FIELD_EXCLUDE_LIST_FIELD));
        this.collectionNamingStrategy = resolveCollectionNamingStrategy(props);
    }

    /**
     * Runs field validation and the DDD-61 cross-property validations, then logs the Requirement-2
     * default resolutions collected during construction. Throws on any combination that cannot work,
     * with a message naming every property involved.
     */
    public void validate() {
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting {} with configuration:", getClass().getSimpleName());
            config.withMaskedPasswords().forEach((propName, propValue) -> LOGGER.info("   {} = {}", propName, propValue));
        }

        validateConnection();
        validateInputAndIdentity();
        validateWritesAndTruncate();
        validateOrdering();
        validateBatching();
        rejectNotYetImplemented();

        startupWarnings.forEach(LOGGER::warn);
    }

    private void validateConnection() {
        final boolean hasUrl = config.hasKey(CONNECTION_URL);
        final boolean hasCloudId = config.hasKey(CONNECTION_CLOUD_ID);
        if (hasUrl && hasCloudId) {
            throw new ConnectException(String.format(
                    "'%s' and '%s' are mutually exclusive; configure exactly one.", CONNECTION_URL, CONNECTION_CLOUD_ID));
        }
        if (!hasUrl && !hasCloudId) {
            throw new ConnectException(String.format(
                    "One of '%s' or '%s' must be configured.", CONNECTION_URL, CONNECTION_CLOUD_ID));
        }

        final boolean hasBasic = config.hasKey(CONNECTION_USERNAME) || config.hasKey(CONNECTION_PASSWORD);
        final boolean hasApiKey = config.hasKey(CONNECTION_API_KEY);
        final boolean hasBearer = config.hasKey(CONNECTION_BEARER_TOKEN);
        final boolean hasKerberos = config.hasKey(CONNECTION_KERBEROS_PRINCIPAL) || config.hasKey(CONNECTION_KERBEROS_KEYTAB);
        final boolean hasCustom = config.hasKey(CONNECTION_CREDENTIALS_PROVIDER_CLASS);

        record CredentialCheck(AuthMode mode, boolean present, String properties) {
        }
        final List<CredentialCheck> checks = List.of(
                new CredentialCheck(AuthMode.BASIC, hasBasic, CONNECTION_USERNAME + "'/'" + CONNECTION_PASSWORD),
                new CredentialCheck(AuthMode.API_KEY, hasApiKey, CONNECTION_API_KEY),
                new CredentialCheck(AuthMode.BEARER, hasBearer, CONNECTION_BEARER_TOKEN),
                new CredentialCheck(AuthMode.KERBEROS, hasKerberos, CONNECTION_KERBEROS_PRINCIPAL + "'/'" + CONNECTION_KERBEROS_KEYTAB),
                new CredentialCheck(AuthMode.CUSTOM, hasCustom, CONNECTION_CREDENTIALS_PROVIDER_CLASS));

        for (CredentialCheck check : checks) {
            if (authMode == check.mode() && !check.present()) {
                throw new ConnectException(String.format(
                        "'%s' is '%s' but '%s' is not configured.", CONNECTION_AUTH_MODE, check.mode().getValue(), check.properties()));
            }
            if (authMode != check.mode() && check.present()) {
                throw new ConnectException(String.format(
                        "'%s' is configured but '%s' is '%s'; credentials that do not match the authentication mode are a "
                                + "configuration error, never a silent fall-through to unauthenticated.",
                        check.properties(), CONNECTION_AUTH_MODE, authMode.getValue()));
            }
        }

        if (authMode == AuthMode.BASIC && (!config.hasKey(CONNECTION_USERNAME) || !config.hasKey(CONNECTION_PASSWORD))) {
            throw new ConnectException(String.format(
                    "Basic authentication requires both '%s' and '%s'.", CONNECTION_USERNAME, CONNECTION_PASSWORD));
        }
        if (authMode == AuthMode.KERBEROS && (!config.hasKey(CONNECTION_KERBEROS_PRINCIPAL) || !config.hasKey(CONNECTION_KERBEROS_KEYTAB))) {
            throw new ConnectException(String.format(
                    "Kerberos authentication requires both '%s' and '%s'.", CONNECTION_KERBEROS_PRINCIPAL, CONNECTION_KERBEROS_KEYTAB));
        }

        if (tlsVerificationMode() != TlsVerificationMode.FULL) {
            startupWarnings.add(String.format(
                    "'%s' is '%s' for '%s'; anything other than 'full' weakens TLS. '%s' is the supported way to trust a "
                            + "self-signed cluster without abandoning verification.",
                    CONNECTION_TLS_VERIFICATION_MODE, tlsVerificationMode().getValue(),
                    hasUrl ? String.join(",", connectionUrls()) : connectionCloudId(), CONNECTION_TLS_CA_FINGERPRINT));
        }
    }

    private void validateInputAndIdentity() {
        if (primaryKeyMode == PrimaryKeyMode.NONE) {
            // Explicit combinations that cannot work; the matching defaults resolve in
            // resolveWriteMethod()/resolveDeleteEnabled() instead of failing here.
            if (config.hasKey(DELETE_ENABLED) && config.getBoolean(DELETE_ENABLED_FIELD)) {
                throw new ConnectException(String.format(
                        "'%s=none' cannot be combined with '%s=true': there is no document identity to delete.",
                        PRIMARY_KEY_MODE, DELETE_ENABLED));
            }
            if (config.hasKey(WRITE_METHOD)) {
                final WriteMethod explicit = enumValue(WriteMethod.class, WRITE_METHOD_FIELD);
                if (explicit.usesUpdateApi()) {
                    throw new ConnectException(String.format(
                            "'%s=none' cannot be combined with '%s=%s': there is no document identity to update.",
                            PRIMARY_KEY_MODE, WRITE_METHOD, explicit.getValue()));
                }
            }
        }

        if (primaryKeyMode == PrimaryKeyMode.KAFKA && deleteEnabled) {
            startupWarnings.add(String.format(
                    "'%s=kafka' with '%s=true': a delete keyed on topic+partition+offset cannot address the document a "
                            + "previous offset created.",
                    PRIMARY_KEY_MODE, DELETE_ENABLED));
        }

        if ((primaryKeyMode == PrimaryKeyMode.RECORD_VALUE || primaryKeyMode == PrimaryKeyMode.RECORD_HEADER)
                && primaryKeyFields.isEmpty()) {
            throw new ConnectException(String.format(
                    "'%s=%s' requires '%s' to name the fields the document id is derived from.",
                    PRIMARY_KEY_MODE, primaryKeyMode.getValue(), SinkConnectorConfig.PRIMARY_KEY_FIELDS));
        }

        if ((primaryKeyMode == PrimaryKeyMode.RECORD_VALUE || primaryKeyMode == PrimaryKeyMode.RECORD_HEADER) && deleteEnabled) {
            // Inherited behavior of the shared sink base class, compensated per Requirement 1
            // with a startup warning; raised as a concern in DDD-61 for an upstream fix.
            startupWarnings.add(String.format(
                    "Tombstone records are discarded by the shared sink framework when '%s' is '%s'; deletes reach the index "
                            + "only as Debezium 'd' events or flattened '__deleted' markers.",
                    PRIMARY_KEY_MODE, primaryKeyMode.getValue()));
        }

        for (String key : topicToResourceMapping.keySet()) {
            if (Strings.isNullOrBlank(key)) {
                throw new ConnectException(String.format("'%s' contains an entry with an empty topic name.", TOPIC_TO_RESOURCE_MAPPING));
            }
        }
    }

    private void validateWritesAndTruncate() {
        if (KeyedMessageBatchMode.PASSTHROUGH == KeyedMessageBatchMode.parse(config.getString(SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE_FIELD))) {
            throw new ConnectException(String.format(
                    "'%s=passthrough' is not supported by the Elasticsearch sink: batch reduction to one write per document id "
                            + "is part of the ordering correctness argument, not an optimization. Remove the property or set it "
                            + "to 'deduplication'.",
                    SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE));
        }

        // truncate.mode governs; the shared truncate.enabled is derived from it and an explicit
        // contradiction is rejected naming both properties.
        final boolean derivedTruncateEnabled = truncateMode != TruncateMode.IGNORE;
        if (config.hasKey(TRUNCATE_ENABLED) && config.getBoolean(SinkConnectorConfig.TRUNCATE_ENABLED_FIELD) != derivedTruncateEnabled) {
            throw new ConnectException(String.format(
                    "'%s=%s' contradicts '%s=%s', from which it is derived (%s when '%s' is 'ignore', %s otherwise). "
                            + "Configure '%s' only.",
                    TRUNCATE_ENABLED, config.getBoolean(SinkConnectorConfig.TRUNCATE_ENABLED_FIELD),
                    TRUNCATE_MODE, truncateMode.getValue(),
                    "false", TRUNCATE_MODE, "true", TRUNCATE_MODE));
        }

        if (truncateMode.isDestructive()) {
            if (truncateAllowedResources.isEmpty()) {
                throw new ConnectException(String.format(
                        "'%s=%s' is destructive and requires '%s' to state the blast radius: a comma-separated list of exact "
                                + "names or glob patterns matched against the resolved resource name.",
                        TRUNCATE_MODE, truncateMode.getValue(), TRUNCATE_ALLOWED_RESOURCES));
            }
            final List<String> wildcards = truncateAllowedResources.stream().filter(p -> p.replace("*", "").isEmpty()).toList();
            if (!wildcards.isEmpty() && !truncateAllowWildcard) {
                throw new ConnectException(String.format(
                        "'%s' contains the match-everything pattern '%s', which requires '%s=true'.",
                        TRUNCATE_ALLOWED_RESOURCES, wildcards.get(0), TRUNCATE_ALLOW_WILDCARD));
            }
            if (!wildcards.isEmpty()) {
                startupWarnings.add(String.format(
                        "'%s' permits truncating every resource this connector writes ('%s' with '%s=true').",
                        TRUNCATE_ALLOWED_RESOURCES, wildcards.get(0), TRUNCATE_ALLOW_WILDCARD));
            }
        }

        if (truncateMode == TruncateMode.RECREATE) {
            if (!resourceAutoCreate) {
                throw new ConnectException(String.format(
                        "'%s=recreate' requires '%s=true': the recreated index is rebuilt from the generated template.",
                        TRUNCATE_MODE, RESOURCE_AUTO_CREATE));
            }
            if (mappingMode == MappingMode.NONE) {
                throw new ConnectException(String.format(
                        "'%s=recreate' cannot be combined with '%s=none': the recreated index would lose its mapping.",
                        TRUNCATE_MODE, MAPPING_MODE));
            }
            if (resourceType == ResourceType.ALIAS_INDEX || resourceType.isDataStream()) {
                throw new ConnectException(String.format(
                        "'%s=recreate' is not supported for '%s=%s'.", TRUNCATE_MODE, RESOURCE_TYPE, resourceType.getValue()));
            }
        }

        if (resourceType.isDataStream()) {
            if (config.hasKey(WRITE_METHOD) && writeMethod != WriteMethod.CREATE) {
                throw new ConnectException(String.format(
                        "'%s=%s' cannot be combined with '%s=%s': data streams accept only 'create' operations.",
                        RESOURCE_TYPE, resourceType.getValue(), WRITE_METHOD, config.getString(WRITE_METHOD_FIELD)));
            }
            if (config.hasKey(DELETE_ENABLED) && config.getBoolean(DELETE_ENABLED_FIELD)) {
                throw new ConnectException(String.format(
                        "'%s=%s' cannot be combined with '%s=true': deletes cannot be propagated to a data stream.",
                        RESOURCE_TYPE, resourceType.getValue(), DELETE_ENABLED));
            }
            if (truncateMode != TruncateMode.IGNORE) {
                throw new ConnectException(String.format(
                        "'%s=%s' cannot be combined with '%s=%s': truncates cannot be propagated to a data stream.",
                        RESOURCE_TYPE, resourceType.getValue(), TRUNCATE_MODE, truncateMode.getValue()));
            }
        }

        for (Map.Entry<Envelope.Operation, WriteMethod> entry : writeMethodPerOperation.entrySet()) {
            if (resourceType.isDataStream() && entry.getValue() != WriteMethod.CREATE) {
                throw new ConnectException(String.format(
                        "'%s' maps operation '%s' to '%s', but '%s=%s' forces 'create'.",
                        WRITE_METHOD_PER_OPERATION, entry.getKey().code(), entry.getValue().getValue(),
                        RESOURCE_TYPE, resourceType.getValue()));
            }
            if (primaryKeyMode == PrimaryKeyMode.NONE && entry.getValue().usesUpdateApi()) {
                throw new ConnectException(String.format(
                        "'%s' maps operation '%s' to '%s', which requires a document identity, but '%s' is 'none'.",
                        WRITE_METHOD_PER_OPERATION, entry.getKey().code(), entry.getValue().getValue(), PRIMARY_KEY_MODE));
            }
        }

        if (!deleteEnabled) {
            startupWarnings.add(String.format(
                    "Delete events will be discarded because '%s' is false.", DELETE_ENABLED));
        }
        if (truncateMode == TruncateMode.IGNORE && !config.hasKey(TRUNCATE_MODE)) {
            // An explicit 'ignore' is an intentional choice; only the out-of-box default is
            // called out, and at INFO so a vanilla deployment does not start with a warning.
            LOGGER.info("Truncate events will be discarded because '{}' defaults to 'ignore'.", TRUNCATE_MODE);
        }
    }

    private void validateOrdering() {
        final boolean nonKeyId = primaryKeyMode == PrimaryKeyMode.RECORD_VALUE || primaryKeyMode == PrimaryKeyMode.RECORD_HEADER;
        if (nonKeyId && nonKeyOrdering == null) {
            throw new ConnectException(String.format(
                    "'%s=%s' derives the document id from something other than the record key, so events for one document may "
                            + "span partitions Kafka never ordered. An explicit ordering decision is required: set '%s' to "
                            + "'version' (with a '%s') or 'last_write_wins'. There is no default.",
                    PRIMARY_KEY_MODE, primaryKeyMode.getValue(), DOCUMENT_ID_NON_KEY_ORDERING, VERSION_STRATEGY));
        }
        if (nonKeyOrdering == NonKeyOrdering.VERSION && versionStrategy == VersionStrategyType.NONE) {
            throw new ConnectException(String.format(
                    "'%s=version' requires '%s' to be something other than 'none': 'none' supplies no ordering key.",
                    DOCUMENT_ID_NON_KEY_ORDERING, VERSION_STRATEGY));
        }
        if (versionEnforcement == VersionEnforcement.EXTERNAL && writeMethod.usesUpdateApi()) {
            throw new ConnectException(String.format(
                    "'%s=external' cannot be combined with '%s=%s': the Update API does not accept native external versioning. "
                            + "Use '%s=script', or a '%s' of 'index' or 'create'.",
                    VERSION_ENFORCEMENT, WRITE_METHOD, writeMethod.getValue(), VERSION_ENFORCEMENT, WRITE_METHOD));
        }
    }

    private void validateBatching() {
        // The single-field bounds live on the field definitions as Field validators; only the
        // cross-field relationship is checked here.
        if (retryBackoffMs > retryBackoffMaxMs) {
            throw new ConnectException(String.format("'%s' must not exceed '%s'.", RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS));
        }
    }

    private static int validateBulkSizeBytes(Configuration config, Field field, ValidationOutput problems) {
        final String value = config.getString(field);
        try {
            final long bytes = Long.parseLong(value);
            if (bytes > 0 || bytes == -1) {
                return 0;
            }
        }
        catch (NumberFormatException e) {
        }
        problems.accept(field, value, "A positive long value, or -1 to disable the bound, is expected");
        return 1;
    }

    private static int validatePositiveDouble(Configuration config, Field field, ValidationOutput problems) {
        final String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            if (Double.parseDouble(value) > 0) {
                return 0;
            }
        }
        catch (NumberFormatException e) {
        }
        problems.accept(field, value, "A positive number is expected");
        return 1;
    }

    private static int validateFieldExcludeList(Configuration config, Field field, ValidationOutput problems) {
        return ConnectorConfigValidationHelper.validateExcludeField(
                config, SinkConnectorConfig.FIELD_INCLUDE_LIST_FIELD, field, problems);
    }

    /**
     * Features whose configuration surface exists (per implementation step 1) but whose runtime
     * arrives in a later DDD-61 implementation step fail loudly here rather than being silently
     * accepted, per Requirement 1.
     */
    private void rejectNotYetImplemented() {
        if (resourceType != ResourceType.INDEX) {
            throw new ConnectException(String.format(
                    "'%s=%s' is not yet implemented; this build supports 'index' (DDD-61 implementation step 10).",
                    RESOURCE_TYPE, resourceType.getValue()));
        }
        if (versionStrategy != VersionStrategyType.NONE) {
            throw new ConnectException(String.format(
                    "'%s=%s' is not yet implemented; this build supports 'none' (DDD-61 implementation step 13).",
                    VERSION_STRATEGY, versionStrategy.getValue()));
        }
        if (config.hasKey(VERSION_ENFORCEMENT) || config.hasKey(VERSION_CONFLICT_MODE)) {
            throw new ConnectException(String.format(
                    "'%s' and '%s' are not yet implemented; they arrive with '%s' (DDD-61 implementation step 13).",
                    VERSION_ENFORCEMENT, VERSION_CONFLICT_MODE, VERSION_STRATEGY));
        }
        if (config.hasKey(FLUSH_TIMEOUT_MS)) {
            throw new ConnectException(String.format(
                    "'%s' is not yet honored by this build; the flush cycle is bounded by '%s' and '%s'.",
                    FLUSH_TIMEOUT_MS, MAX_RETRIES, PROGRESS_STALL_TIMEOUT_MS));
        }
        final List<String> perTopicOverrides = config.asMap().keySet().stream()
                .filter(k -> k.startsWith("topic.") && !k.startsWith("topic.to.resource"))
                .toList();
        if (!perTopicOverrides.isEmpty()) {
            throw new ConnectException(String.format(
                    "Per-topic overrides (%s) are not yet implemented (DDD-61 implementation step 15).",
                    String.join(", ", perTopicOverrides)));
        }
    }

    private AuthMode resolveAuthMode() {
        if (config.hasKey(CONNECTION_AUTH_MODE)) {
            return EnumeratedValue.parse(AuthMode.class, config.getString(CONNECTION_AUTH_MODE_FIELD), AuthMode.NONE.getValue());
        }
        return config.hasKey(CONNECTION_USERNAME) || config.hasKey(CONNECTION_PASSWORD) ? AuthMode.BASIC : AuthMode.NONE;
    }

    private WriteMethod resolveWriteMethod() {
        final WriteMethod configured = enumValue(WriteMethod.class, WRITE_METHOD_FIELD);
        if (primaryKeyMode == PrimaryKeyMode.NONE && !config.hasKey(WRITE_METHOD) && configured.usesUpdateApi()) {
            startupWarnings.add(String.format(
                    "'%s' resolved to 'create' because '%s' is 'none' and the default '%s' requires a document identity.",
                    WRITE_METHOD, PRIMARY_KEY_MODE, configured.getValue()));
            return WriteMethod.CREATE;
        }
        if (resourceType.isDataStream() && !config.hasKey(WRITE_METHOD)) {
            return WriteMethod.CREATE;
        }
        return configured;
    }

    private boolean resolveDeleteEnabled() {
        final boolean configured = config.getBoolean(DELETE_ENABLED_FIELD);
        if (configured && !config.hasKey(DELETE_ENABLED)) {
            if (primaryKeyMode == PrimaryKeyMode.NONE) {
                startupWarnings.add(String.format(
                        "'%s' resolved to 'false' because '%s' is 'none': there is no document identity to delete.",
                        DELETE_ENABLED, PRIMARY_KEY_MODE));
                return false;
            }
            if (resourceType.isDataStream()) {
                startupWarnings.add(String.format(
                        "'%s' resolved to 'false' because '%s' is '%s': deletes cannot be propagated to a data stream.",
                        DELETE_ENABLED, RESOURCE_TYPE, resourceType.getValue()));
                return false;
            }
        }
        return configured;
    }

    private CollectionNamingStrategy resolveCollectionNamingStrategy(Map<String, String> props) {
        final CollectionNamingStrategy strategy = config.getInstance(COLLECTION_NAMING_STRATEGY_FIELD, CollectionNamingStrategy.class);
        strategy.configure(props);
        return strategy;
    }

    private Set<MetadataField> parseMetadataFields(String value) {
        if (Strings.isNullOrBlank(value)) {
            return Set.of();
        }
        final EnumSet<MetadataField> result = EnumSet.noneOf(MetadataField.class);
        for (String entry : value.split(",")) {
            final MetadataField field = EnumeratedValue.parse(MetadataField.class, entry);
            if (field == null) {
                throw new ConnectException(String.format(
                        "'%s' contains unknown metadata field '%s'; allowed values are %s.",
                        DOCUMENT_METADATA_FIELDS, entry.trim(),
                        Arrays.stream(MetadataField.values()).map(MetadataField::getValue).collect(Collectors.joining(", "))));
            }
            result.add(field);
        }
        return result;
    }

    private Map<Envelope.Operation, WriteMethod> parseWriteMethodPerOperation(String value) {
        final Map<Envelope.Operation, WriteMethod> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : parsePairs(value, WRITE_METHOD_PER_OPERATION).entrySet()) {
            final Envelope.Operation operation = Envelope.Operation.forCode(entry.getKey());
            if (!(operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ || operation == Envelope.Operation.UPDATE)) {
                throw new ConnectException(String.format(
                        "'%s' key '%s' is not a writable envelope operation; allowed keys are 'c', 'r', 'u'.",
                        WRITE_METHOD_PER_OPERATION, entry.getKey()));
            }
            final WriteMethod method = EnumeratedValue.parse(WriteMethod.class, entry.getValue());
            if (method == null) {
                throw new ConnectException(String.format(
                        "'%s' maps operation '%s' to unknown write method '%s'.",
                        WRITE_METHOD_PER_OPERATION, entry.getKey(), entry.getValue()));
            }
            result.put(operation, method);
        }
        return result;
    }

    private Map<String, ErrorBucket> parseClassificationOverrides(String value) {
        final Map<String, ErrorBucket> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : parsePairs(value, ERROR_CLASSIFICATION_OVERRIDES).entrySet()) {
            final ErrorBucket bucket = ErrorBucket.parseOverride(entry.getValue());
            if (bucket == null) {
                throw new ConnectException(String.format(
                        "'%s' assigns error type '%s' to unknown bucket '%s'; allowed buckets are transient, record, "
                                + "resource_fatal, task_fatal. 'success' is deliberately not assignable.",
                        ERROR_CLASSIFICATION_OVERRIDES, entry.getKey(), entry.getValue()));
            }
            result.put(entry.getKey(), bucket);
        }
        return result;
    }

    private Map<String, String> parseMappingSettings(String value) {
        final Map<String, String> result = parsePairs(value, MAPPING_SETTINGS, "=");
        for (String key : result.keySet()) {
            if (!MAPPING_SETTINGS_ALLOWED_KEYS.contains(key)) {
                throw new ConnectException(String.format(
                        "'%s' key '%s' is not in the supported set %s.", MAPPING_SETTINGS, key, MAPPING_SETTINGS_ALLOWED_KEYS));
            }
        }
        return result;
    }

    private Map<String, String> parsePairs(String value, String propertyName) {
        return parsePairs(value, propertyName, ":");
    }

    private Map<String, String> parsePairs(String value, String propertyName, String separator) {
        final Map<String, String> result = new LinkedHashMap<>();
        if (Strings.isNullOrBlank(value)) {
            return result;
        }
        for (String entry : value.split(",")) {
            if (Strings.isNullOrBlank(entry)) {
                continue;
            }
            final String[] parts = entry.trim().split(separator, 2);
            if (parts.length != 2 || Strings.isNullOrBlank(parts[0]) || Strings.isNullOrBlank(parts[1])) {
                throw new ConnectException(String.format(
                        "'%s' entry '%s' is not of the form 'key%svalue'.", propertyName, entry, separator));
            }
            result.put(parts[0].trim(), parts[1].trim());
        }
        return result;
    }

    private static Double parseDoubleOrNull(String value) {
        try {
            return value == null ? null : Double.valueOf(value.trim());
        }
        catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLongOrNull(String value) {
        try {
            return value == null ? null : Long.valueOf(value.trim());
        }
        catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * The configured enum option for a field, or the field's declared default when it is absent.
     */
    private <T extends Enum<T> & EnumeratedValue> T enumValue(Class<T> type, Field field) {
        return EnumeratedValue.parse(type, config.getString(field), field.defaultValueAsString());
    }

    /**
     * The Connect connector name, which scopes the templates this connector owns
     * (DDD-61 6.3); the module name is the fallback outside a Connect runtime.
     */
    public String getConnectorName() {
        return Strings.defaultIfBlank(config.getString(ConfigurationNames.CONNECTOR_NAME_PROPERTY), Module.name());
    }

    public List<String> connectionUrls() {
        return Strings.listOf(config.getString(CONNECTION_URL_FIELD), s -> s.split(","), String::trim);
    }

    public String connectionCloudId() {
        return config.getString(CONNECTION_CLOUD_ID_FIELD);
    }

    public AuthMode authMode() {
        return authMode;
    }

    public String connectionUsername() {
        return config.getString(CONNECTION_USERNAME_FIELD);
    }

    public String connectionPassword() {
        return config.getString(CONNECTION_PASSWORD_FIELD);
    }

    public String connectionApiKey() {
        return config.getString(CONNECTION_API_KEY_FIELD);
    }

    public String connectionBearerToken() {
        return config.getString(CONNECTION_BEARER_TOKEN_FIELD);
    }

    public String kerberosPrincipal() {
        return config.getString(CONNECTION_KERBEROS_PRINCIPAL_FIELD);
    }

    public String kerberosKeytab() {
        return config.getString(CONNECTION_KERBEROS_KEYTAB_FIELD);
    }

    public String credentialsProviderClass() {
        return config.getString(CONNECTION_CREDENTIALS_PROVIDER_CLASS_FIELD);
    }

    public Map<String, String> connectionHeaders() {
        return parsePairs(config.getString(CONNECTION_HEADERS_FIELD), CONNECTION_HEADERS);
    }

    public boolean isConnectionCompression() {
        return config.getBoolean(CONNECTION_COMPRESSION_FIELD);
    }

    public int connectionTimeoutMs() {
        return config.getInteger(CONNECTION_TIMEOUT_MS_FIELD);
    }

    public int connectionReadTimeoutMs() {
        return config.getInteger(CONNECTION_READ_TIMEOUT_MS_FIELD);
    }

    public int connectionIdleTimeoutMs() {
        return config.getInteger(CONNECTION_IDLE_TIMEOUT_MS_FIELD);
    }

    public boolean isSniffEnabled() {
        return config.getBoolean(CONNECTION_SNIFF_ENABLED_FIELD);
    }

    public ApiCompatibilityMode apiCompatibilityMode() {
        return enumValue(ApiCompatibilityMode.class, CONNECTION_API_COMPATIBILITY_MODE_FIELD);
    }

    public TlsVerificationMode tlsVerificationMode() {
        return enumValue(TlsVerificationMode.class, CONNECTION_TLS_VERIFICATION_MODE_FIELD);
    }

    public String tlsKeystoreLocation() {
        return config.getString(CONNECTION_TLS_KEYSTORE_LOCATION_FIELD);
    }

    public String tlsKeystorePassword() {
        return config.getString(CONNECTION_TLS_KEYSTORE_PASSWORD_FIELD);
    }

    public String tlsKeystoreType() {
        return config.getString(CONNECTION_TLS_KEYSTORE_TYPE_FIELD);
    }

    public String tlsKeyPassword() {
        return config.getString(CONNECTION_TLS_KEY_PASSWORD_FIELD);
    }

    public String tlsTruststoreLocation() {
        return config.getString(CONNECTION_TLS_TRUSTSTORE_LOCATION_FIELD);
    }

    public String tlsTruststorePassword() {
        return config.getString(CONNECTION_TLS_TRUSTSTORE_PASSWORD_FIELD);
    }

    public String tlsTruststoreType() {
        return config.getString(CONNECTION_TLS_TRUSTSTORE_TYPE_FIELD);
    }

    public List<String> tlsProtocols() {
        return Strings.listOf(config.getString(CONNECTION_TLS_PROTOCOLS_FIELD), s -> s.split(","), String::trim);
    }

    public List<String> tlsCipherSuites() {
        return Strings.listOf(config.getString(CONNECTION_TLS_CIPHER_SUITES_FIELD), s -> s.split(","), String::trim);
    }

    public String tlsCaFingerprint() {
        return config.getString(CONNECTION_TLS_CA_FINGERPRINT_FIELD);
    }

    public String proxyHost() {
        return config.getString(CONNECTION_PROXY_HOST_FIELD);
    }

    public Integer proxyPort() {
        return config.getInteger(CONNECTION_PROXY_PORT_FIELD);
    }

    public String proxyUsername() {
        return config.getString(CONNECTION_PROXY_USERNAME_FIELD);
    }

    public String proxyPassword() {
        return config.getString(CONNECTION_PROXY_PASSWORD_FIELD);
    }

    public EventFormat eventFormat() {
        return eventFormat;
    }

    public TombstoneMode tombstoneMode() {
        return tombstoneMode;
    }

    public String documentIdSeparator() {
        return documentIdSeparator;
    }

    public NonKeyOrdering nonKeyOrdering() {
        return nonKeyOrdering;
    }

    public Set<MetadataField> documentMetadataFields() {
        return documentMetadataFields;
    }

    public String documentMetadataPrefix() {
        return documentMetadataPrefix;
    }

    public InvalidNameHandling invalidNameHandling() {
        return invalidNameHandling;
    }

    public String resourceNameReplacement() {
        return resourceNameReplacement;
    }

    public String resourceNameTimezone() {
        return resourceNameTimezone;
    }

    public ResourceType resourceType() {
        return resourceType;
    }

    public boolean isResourceAutoCreate() {
        return resourceAutoCreate;
    }

    public Map<String, String> topicToResourceMapping() {
        return topicToResourceMapping;
    }

    public String indexRoutingField() {
        return indexRoutingField;
    }

    public String ingestPipeline() {
        return ingestPipeline;
    }

    public boolean isIngestPipelineValidate() {
        return ingestPipelineValidate;
    }

    public WriteMethod writeMethod() {
        return writeMethod;
    }

    /**
     * The effective write method for the given envelope operation, honoring
     * {@code write.method.per.operation} overrides.
     */
    public WriteMethod writeMethodFor(Envelope.Operation operation) {
        return operation == null ? writeMethod : writeMethodPerOperation.getOrDefault(operation, writeMethod);
    }

    public TruncateMode truncateMode() {
        return truncateMode;
    }

    public List<String> truncateAllowedResources() {
        return truncateAllowedResources;
    }

    public boolean isTruncateAllowWildcard() {
        return truncateAllowWildcard;
    }

    public VersionStrategyType versionStrategy() {
        return versionStrategy;
    }

    public VersionEnforcement versionEnforcement() {
        return versionEnforcement;
    }

    public VersionConflictMode versionConflictMode() {
        return versionConflictMode;
    }

    public long bulkSizeBytes() {
        return bulkSizeBytes;
    }

    public long lingerMs() {
        return lingerMs;
    }

    public long flushTimeoutMs() {
        return flushTimeoutMs;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long retryBackoffMs() {
        return retryBackoffMs;
    }

    public long retryBackoffMaxMs() {
        return retryBackoffMaxMs;
    }

    public long progressStallTimeoutMs() {
        return progressStallTimeoutMs;
    }

    public Double maxRequestsPerSecond() {
        return maxRequestsPerSecond;
    }

    public Long maxBytesPerSecond() {
        return maxBytesPerSecond;
    }

    public DecimalOutputMode decimalOutputMode() {
        return decimalOutputMode;
    }

    public TemporalOutputMode temporalOutputMode() {
        return temporalOutputMode;
    }

    public IntervalOutputMode intervalOutputMode() {
        return intervalOutputMode;
    }

    public JsonOutputMode jsonOutputMode() {
        return jsonOutputMode;
    }

    public BitsOutputMode bitsOutputMode() {
        return bitsOutputMode;
    }

    public BinaryOutputMode binaryOutputMode() {
        return binaryOutputMode;
    }

    public GeometryOutputMode geometryOutputMode() {
        return geometryOutputMode;
    }

    public VectorOutputMode vectorOutputMode() {
        return vectorOutputMode;
    }

    public MapOutputMode mapOutputMode() {
        return mapOutputMode;
    }

    public NullValueHandling nullValueHandling() {
        return nullValueHandling;
    }

    public FieldNameAdjustmentMode fieldNameAdjustmentMode() {
        return fieldNameAdjustmentMode;
    }

    public String fieldNameSeparatorReplacement() {
        return fieldNameSeparatorReplacement;
    }

    public MappingMode mappingMode() {
        return mappingMode;
    }

    public MappingDynamic mappingDynamic() {
        return mappingDynamic;
    }

    public List<String> mappingComposedOf() {
        return mappingComposedOf;
    }

    public Map<String, String> mappingSettings() {
        return mappingSettings;
    }

    public StringMappingMode stringMappingMode() {
        return stringMappingMode;
    }

    public StructMappingMode structMappingMode() {
        return structMappingMode;
    }

    public JsonMappingMode jsonMappingMode() {
        return jsonMappingMode;
    }

    public Map<String, ErrorBucket> errorClassificationOverrides() {
        return errorClassificationOverrides;
    }

    public boolean isLogSensitiveData() {
        return logSensitiveData;
    }

    @Override
    public String getCollectionNameFormat() {
        return collectionNameFormat;
    }

    @Override
    public KeyedMessageBatchMode getKeyedMessageBatchMode() {
        // Fixed to deduplication: reduction to one write per document id is part of the ordering
        // correctness argument (DDD-61 7.2); an explicit 'passthrough' is rejected in validate().
        return KeyedMessageBatchMode.DEDUPLICATION;
    }

    @Override
    public PrimaryKeyMode getPrimaryKeyMode() {
        return primaryKeyMode;
    }

    @Override
    public Set<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    @Override
    public boolean isTruncateEnabled() {
        return truncateMode != TruncateMode.IGNORE;
    }

    @Override
    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    @Override
    public String useTimeZone() {
        return resourceNameTimezone;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public CollectionNamingStrategy getCollectionNamingStrategy() {
        return collectionNamingStrategy;
    }

    @Override
    public FieldNameFilter fieldFilter() {
        return fieldsFilter;
    }

    @Override
    public String cloudEventsSchemaNamePattern() {
        return cloudEventsSchemaNamePattern;
    }

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * Case-normalized lookup used by tests and diagnostics.
     */
    public String rawProperty(String name) {
        return config.getString(name.toLowerCase(Locale.ROOT));
    }
}
