/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.junit.jupiter;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import io.debezium.connector.elasticsearch.junit.ElasticsearchTestCluster;

/**
 * Starts the shared {@link ElasticsearchTestCluster} before a test class runs and injects it into
 * any test or lifecycle method that declares a parameter of that type.
 *
 * @author Chris Cranford
 */
public class ElasticsearchExtension implements BeforeAllCallback, ParameterResolver {

    @Override
    public void beforeAll(ExtensionContext context) {
        ElasticsearchTestCluster.get();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return ElasticsearchTestCluster.class.equals(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return ElasticsearchTestCluster.get();
    }
}
