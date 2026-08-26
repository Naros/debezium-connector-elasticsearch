/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.Sniffer;

import io.debezium.DebeziumException;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.util.Strings;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * Builds the {@link ElasticsearchConnection} from the connector configuration: URLs or Cloud ID,
 * authentication mode, TLS with three-valued verification, proxy, timeouts, compression, static
 * headers, and optional node sniffing.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 10.2"
 * @see "DDD-61 Section 10.3"
 */
public class ElasticsearchClientFactory {

    private final ElasticsearchSinkConnectorConfig config;
    private final Map<String, String> rawProperties;

    public ElasticsearchClientFactory(ElasticsearchSinkConnectorConfig config, Map<String, String> rawProperties) {
        this.config = config;
        this.rawProperties = rawProperties;
    }

    public ElasticsearchConnection connect() {
        final List<HttpHost> hosts = resolveHosts();
        final RestClientBuilder builder = RestClient.builder(hosts.toArray(HttpHost[]::new));

        final String pathPrefix = resolvePathPrefix();
        if (!Strings.isNullOrBlank(pathPrefix)) {
            builder.setPathPrefix(pathPrefix);
        }

        applyDefaultHeaders(builder);
        applyRequestTimeouts(builder);
        applyHttpClientConfig(builder);
        builder.setCompressionEnabled(config.isConnectionCompression());

        if (config.authMode() == ElasticsearchSinkConnectorConfig.AuthMode.CUSTOM) {
            customCredentialsProvider().apply(builder);
        }

        final RestClient restClient = builder.build();
        final Sniffer sniffer = config.isSniffEnabled() ? Sniffer.builder(restClient).build() : null;

        final ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        final ElasticsearchClient client = new ElasticsearchClient(transport);
        final ClusterVersionHandshake handshake = new ClusterVersionHandshake(
                restClient, config.maxRetries(), config.retryBackoffMs(), config.retryBackoffMaxMs());
        return new ElasticsearchConnection(restClient, transport, client, handshake, sniffer);
    }

    private List<HttpHost> resolveHosts() {
        if (!Strings.isNullOrBlank(config.connectionCloudId())) {
            return List.of(decodeCloudId(config.connectionCloudId()));
        }
        final List<HttpHost> hosts = new ArrayList<>();
        for (String url : config.connectionUrls()) {
            final URI uri = URI.create(url);
            if (uri.getHost() == null) {
                // A schemeless "host:port" parses as scheme "host" with no host at all.
                throw new DebeziumException(String.format(
                        "'%s' contains '%s', which has no host; URLs must include a scheme, e.g. 'http://%s'.",
                        ElasticsearchSinkConnectorConfig.CONNECTION_URL, url, url));
            }
            final String scheme = uri.getScheme() != null ? uri.getScheme() : "http";
            final int port = uri.getPort() != -1 ? uri.getPort() : ("https".equals(scheme) ? 443 : 9200);
            hosts.add(new HttpHost(uri.getHost(), port, scheme));
        }
        return hosts;
    }

    /**
     * A context path on the connection URL is honored, e.g. {@code https://gateway.internal/es},
     * for clusters behind a reverse proxy. The REST client supports one prefix for all hosts.
     */
    private String resolvePathPrefix() {
        if (!Strings.isNullOrBlank(config.connectionCloudId())) {
            return null;
        }
        String prefix = null;
        boolean first = true;
        for (String url : config.connectionUrls()) {
            final String path = URI.create(url).getPath();
            final String normalized = Strings.isNullOrBlank(path) || "/".equals(path) ? null : path;
            if (first) {
                prefix = normalized;
                first = false;
            }
            else if (!Objects.equals(prefix, normalized)) {
                throw new DebeziumException(String.format(
                        "'%s' lists URLs with different context paths ('%s' vs '%s'); the client supports one path prefix "
                                + "for all hosts.",
                        ElasticsearchSinkConnectorConfig.CONNECTION_URL,
                        prefix == null ? "(none)" : prefix, normalized == null ? "(none)" : normalized));
            }
        }
        return prefix;
    }

    static HttpHost decodeCloudId(String cloudId) {
        final String[] labelAndData = cloudId.split(":", 2);
        if (labelAndData.length != 2) {
            throw new DebeziumException("Invalid Elastic Cloud ID: expected '<label>:<base64 data>'");
        }
        final String decoded = new String(Base64.getDecoder().decode(labelAndData[1]), StandardCharsets.UTF_8);
        final String[] parts = decoded.split("\\$");
        if (parts.length < 2) {
            throw new DebeziumException("Invalid Elastic Cloud ID: decoded data is not of the form 'host$es_uuid$...'");
        }
        final String[] hostAndPort = parts[0].split(":", 2);
        final int port = hostAndPort.length == 2 ? Integer.parseInt(hostAndPort[1]) : 443;
        return new HttpHost(parts[1] + "." + hostAndPort[0], port, "https");
    }

    private void applyDefaultHeaders(RestClientBuilder builder) {
        final List<BasicHeader> headers = new ArrayList<>();
        config.connectionHeaders().forEach((name, value) -> headers.add(new BasicHeader(name, value)));
        switch (config.authMode()) {
            case API_KEY -> headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, "ApiKey " + config.connectionApiKey()));
            case BEARER -> headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + config.connectionBearerToken()));
            default -> {
            }
        }
        if (!headers.isEmpty()) {
            builder.setDefaultHeaders(headers.toArray(BasicHeader[]::new));
        }
    }

    private void applyRequestTimeouts(RestClientBuilder builder) {
        builder.setRequestConfigCallback(requestConfig -> requestConfig
                .setConnectTimeout(config.connectionTimeoutMs())
                .setSocketTimeout(config.connectionReadTimeoutMs())
                .setConnectionRequestTimeout(config.connectionTimeoutMs()));
    }

    private void applyHttpClientConfig(RestClientBuilder builder) {
        builder.setHttpClientConfigCallback(httpClient -> {
            final BasicCredentialsProvider credentials = new BasicCredentialsProvider();
            boolean hasCredentials = false;

            if (config.authMode() == ElasticsearchSinkConnectorConfig.AuthMode.BASIC) {
                credentials.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(config.connectionUsername(), config.connectionPassword()));
                hasCredentials = true;
            }

            if (!Strings.isNullOrBlank(config.proxyHost())) {
                final HttpHost proxy = new HttpHost(config.proxyHost(), config.proxyPort() != null ? config.proxyPort() : 8080);
                httpClient.setProxy(proxy);
                if (!Strings.isNullOrBlank(config.proxyUsername())) {
                    credentials.setCredentials(new AuthScope(proxy),
                            new UsernamePasswordCredentials(config.proxyUsername(), config.proxyPassword()));
                    hasCredentials = true;
                }
            }
            if (hasCredentials) {
                httpClient.setDefaultCredentialsProvider(credentials);
            }

            if (config.authMode() == ElasticsearchSinkConnectorConfig.AuthMode.KERBEROS) {
                new KerberosAuthentication(config.kerberosPrincipal(), config.kerberosKeytab()).apply(httpClient);
            }

            applyTls(httpClient);
            httpClient.setConnectionTimeToLive(config.connectionIdleTimeoutMs(), TimeUnit.MILLISECONDS);
            return httpClient;
        });
    }

    private void applyTls(HttpAsyncClientBuilder httpClient) {
        final SSLContext sslContext = buildSslContext();
        if (sslContext == null) {
            return;
        }
        final HostnameVerifier verifier = switch (config.tlsVerificationMode()) {
            case FULL -> SSLIOSessionStrategy.getDefaultHostnameVerifier();
            case CERTIFICATE, NONE -> NoopHostnameVerifier.INSTANCE;
        };
        final List<String> protocols = config.tlsProtocols();
        final List<String> ciphers = config.tlsCipherSuites();
        httpClient.setSSLStrategy(new SSLIOSessionStrategy(
                sslContext,
                protocols.isEmpty() ? null : protocols.toArray(String[]::new),
                ciphers.isEmpty() ? null : ciphers.toArray(String[]::new),
                verifier));
    }

    private SSLContext buildSslContext() {
        try {
            if (config.tlsVerificationMode() == ElasticsearchSinkConnectorConfig.TlsVerificationMode.NONE) {
                return SSLContexts.custom().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build();
            }
            if (!Strings.isNullOrBlank(config.tlsCaFingerprint())) {
                return TransportUtils.sslContextFromCaFingerprint(config.tlsCaFingerprint());
            }

            final boolean hasTruststore = !Strings.isNullOrBlank(config.tlsTruststoreLocation());
            final boolean hasKeystore = !Strings.isNullOrBlank(config.tlsKeystoreLocation());
            if (!hasTruststore && !hasKeystore) {
                return null;
            }

            final SSLContextBuilder builder = SSLContexts.custom();
            if (hasTruststore) {
                builder.loadTrustMaterial(
                        loadKeyStore(config.tlsTruststoreLocation(), config.tlsTruststorePassword(), config.tlsTruststoreType()),
                        null);
            }
            if (hasKeystore) {
                final char[] keyPassword = !Strings.isNullOrBlank(config.tlsKeyPassword())
                        ? config.tlsKeyPassword().toCharArray()
                        : passwordChars(config.tlsKeystorePassword());
                builder.loadKeyMaterial(
                        loadKeyStore(config.tlsKeystoreLocation(), config.tlsKeystorePassword(), config.tlsKeystoreType()),
                        keyPassword);
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to initialize the TLS context from the connection.tls.* properties", e);
        }
    }

    private KeyStore loadKeyStore(String location, String password, String type) throws Exception {
        final KeyStore keyStore = KeyStore.getInstance(type);
        try (var stream = Files.newInputStream(Path.of(location))) {
            keyStore.load(stream, passwordChars(password));
        }
        return keyStore;
    }

    private char[] passwordChars(String password) {
        return password != null ? password.toCharArray() : null;
    }

    private ConnectionCredentialsProvider customCredentialsProvider() {
        try {
            final Class<?> providerClass = Class.forName(config.credentialsProviderClass(), true,
                    getClass().getClassLoader());
            final ConnectionCredentialsProvider provider = (ConnectionCredentialsProvider) providerClass
                    .getDeclaredConstructor().newInstance();
            provider.configure(rawProperties);
            return provider;
        }
        catch (Exception e) {
            throw new DebeziumException(String.format(
                    "Failed to instantiate '%s' ('%s')", config.credentialsProviderClass(),
                    ElasticsearchSinkConnectorConfig.CONNECTION_CREDENTIALS_PROVIDER_CLASS), e);
        }
    }
}
