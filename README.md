[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE.txt)
[![Maven Central](https://img.shields.io/maven-central/v/io.debezium/debezium-connector-elasticsearch.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:io.debezium%20AND%20a:debezium-connector-elasticsearch)
[![Build Status](https://github.com/debezium/debezium-connector-elasticsearch/actions/workflows/maven.yml/badge.svg)](https://github.com/debezium/debezium-connector-elasticsearch/actions/workflows/maven.yml)
[![Community](https://img.shields.io/badge/Community-Zulip-blue.svg)](https://debezium.zulipchat.com/#narrow/stream/302529-users)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Sink Connector for Elasticsearch

A [Debezium](https://debezium.io/) sink connector for streaming change events into
[Elasticsearch](https://www.elastic.co/elasticsearch) indexes. Debezium change events, in
their envelope, flattened, or plain structured form, are applied as document writes and
deletes with logical-type fidelity: decimals arrive as numbers, temporals as ISO-8601,
JSON columns as embedded objects, geometry as GeoJSON, and vectors as arrays.

This connector is currently in an **incubating** state; details are subject to change.

## Documentation

The connector is being implemented against the
[DDD-61 design document](https://github.com/debezium/debezium-design-documents/blob/main/DDD-61.md),
which is the authoritative reference for the configuration surface, input contracts,
document identity and ordering, mapping generation, error classification, and monitoring
until the connector documentation lands on the
[Debezium site](https://debezium.io/documentation/).

## Quick start

Install the plugin from Maven Central
([`io.debezium:debezium-connector-elasticsearch`](https://search.maven.org/search?q=g:io.debezium%20AND%20a:debezium-connector-elasticsearch))
into your Kafka Connect `plugin.path` and register a connector:

```json
{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.debezium.connector.elasticsearch.ElasticsearchSinkConnector",
    "topics": "inventory.customers",
    "connection.url": "http://elasticsearch:9200",
    "connection.auth.mode": "basic",
    "connection.username": "elastic",
    "connection.password": "elastic"
  }
}
```

By default each topic maps to an index of the same name, the index is created on first
write with a mapping generated from the record schema, and the document `_id` is derived
from the record key. A topic name that violates Elasticsearch index naming rules is a
record-level error unless `resource.name.invalid.handling=sanitize` is configured. Naming,
identity, write methods, deletes, truncates, and error handling are all configurable; see
the design document above for the full property surface.

## Building

Requirements: JDK 17+, Docker (for integration tests).

```bash
./mvnw clean install
```

Build the connector plugin archive:

```bash
./mvnw clean package -Passembly
```

## Testing

Unit tests run with the standard build. Integration tests (`*IT`) start an Elasticsearch
container through Testcontainers and need Docker:

```bash
./mvnw clean verify
```

The integration tests run against Elasticsearch 8.x by default. Point them at another image,
such as a 9.x release, with the `elasticsearch.image` property:

```bash
./mvnw clean verify -Delasticsearch.image=docker.elastic.co/elasticsearch/elasticsearch:9.5.1
```

Skip the integration tests with `-DskipITs`, or skip Docker entirely with `-Dquick`.

## Contributing

The Debezium community welcomes anyone who wants to help out in any way, whether that
includes reporting problems, helping with documentation, or contributing code changes to
fix bugs, add tests, or implement new features. See
[CONTRIBUTING.md](https://github.com/debezium/debezium/blob/main/CONTRIBUTING.md) for details.

Issues are tracked in the [Debezium Issue Tracker](https://github.com/debezium/dbz/issues)
under the `component/elasticsearch-connector` component. Community chat happens in the
[users channel on Debezium Zulip](https://debezium.zulipchat.com/#narrow/stream/302529-users).
