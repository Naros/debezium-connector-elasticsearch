/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import java.util.Map;

import org.elasticsearch.client.RestClientBuilder;

/**
 * SPI for authentication schemes that live outside the connector core, selected by
 * {@code connection.auth.mode=custom} and {@code connection.credentials.provider.class};
 * the seam through which AWS SigV4 and other cloud-vendor schemes attach.
 *
 * @author Chris Cranford
 */
public interface ConnectionCredentialsProvider {

    /**
     * Configures the provider from the raw connector properties before the client is built.
     *
     * @param properties the connector configuration properties, never {@code null}
     */
    void configure(Map<String, String> properties);

    /**
     * Applies the credentials to the client under construction, e.g. by installing default
     * headers, an interceptor, or an HTTP client callback.
     *
     * @param builder the REST client builder, never {@code null}
     */
    void apply(RestClientBuilder builder);
}
