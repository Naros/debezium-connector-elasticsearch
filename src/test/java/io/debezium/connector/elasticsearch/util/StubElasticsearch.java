/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * A scriptable stand-in for an Elasticsearch HTTP endpoint on the JDK's built-in server, for
 * exercising the client-facing classes against responses a live cluster cannot be made to
 * produce on demand: a 429 on one bulk item, a dropped connection, a media-type rejection, a
 * malformed body. Every request is recorded; responses are answered from a per-route queue of
 * scripted handlers, then from overridable defaults that behave like an empty, healthy cluster.
 *
 * @author Chris Cranford
 */
public final class StubElasticsearch implements AutoCloseable {

    /**
     * One request as received.
     */
    public record RecordedRequest(String method, String path, String query, Map<String, List<String>> headers, String body) {

        public boolean is(String method, String path) {
            return this.method.equalsIgnoreCase(method) && this.path.equals(path);
        }

        /**
         * The query string parsed into parameters; a repeated key keeps the last value.
         */
        public Map<String, String> queryParameters() {
            final Map<String, String> parameters = new LinkedHashMap<>();
            if (query != null) {
                for (String pair : query.split("&")) {
                    final int at = pair.indexOf('=');
                    parameters.put(at < 0 ? pair : pair.substring(0, at), at < 0 ? "" : pair.substring(at + 1));
                }
            }
            return parameters;
        }

        public List<BulkAction> bulkActions() {
            return parseBulk(body);
        }
    }

    /**
     * One action line of a bulk request with its source line, if any.
     */
    public record BulkAction(String operation, String index, String id, String source) {
    }

    /**
     * What to answer: a status and JSON body, or a dropped connection.
     */
    public record StubResponse(int status, String body, boolean drop) {

        public static StubResponse ok(String body) {
            return new StubResponse(200, body, false);
        }

        public static StubResponse status(int status, String body) {
            return new StubResponse(status, body, false);
        }

        public static StubResponse dropConnection() {
            return new StubResponse(0, null, true);
        }

        public static StubResponse error(int status, String type, String reason) {
            return status(status, "{\"error\":{\"type\":\"" + type + "\",\"reason\":\"" + reason + "\"},\"status\":" + status + "}");
        }
    }

    /**
     * The outcome of one bulk item: a status and, unless it succeeded, an error.
     */
    public record ItemResult(int status, String errorType, String errorReason) {

        public static ItemResult ok() {
            return new ItemResult(200, null, null);
        }

        public static ItemResult error(int status, String type) {
            return new ItemResult(status, type, type + " for testing");
        }
    }

    private record Route(String method, Predicate<String> path, Function<RecordedRequest, StubResponse> handler) {
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpServer server;
    private final RestClient restClient;
    private final ElasticsearchClient client;
    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
    private final Map<String, Deque<Function<RecordedRequest, StubResponse>>> scripted = new LinkedHashMap<>();
    private final List<Route> defaults = new ArrayList<>();

    private volatile String versionNumber = "8.19.20";
    private volatile String distribution = "elasticsearch";
    private volatile String pathPrefix = "";

    public StubElasticsearch() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        restClient = RestClient.builder(new HttpHost("127.0.0.1", server.getAddress().getPort(), "http")).build();
        client = new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
        installDefaults();
    }

    private void installDefaults() {
        defaults.add(new Route("GET", "/"::equals, r -> StubResponse.ok(versionDocument())));
        defaults.add(new Route("POST", "/_bulk"::equals, r -> bulkResponse(r.bulkActions(), a -> ItemResult.ok())));
        defaults.add(new Route("HEAD", p -> p.startsWith("/_index_template/"), r -> StubResponse.status(404, "")));
        defaults.add(new Route("PUT", p -> p.startsWith("/_index_template/") || p.startsWith("/_component_template/"),
                r -> StubResponse.ok("{\"acknowledged\":true}")));
        defaults.add(new Route("GET", p -> p.startsWith("/_ingest/pipeline/"), r -> StubResponse.error(404, "resource_not_found_exception", "no pipeline")));
        defaults.add(new Route("POST", p -> p.endsWith("/_delete_by_query"),
                r -> StubResponse.ok("{\"took\":1,\"timed_out\":false,\"total\":0,\"deleted\":0,\"batches\":0,\"version_conflicts\":0,"
                        + "\"noops\":0,\"retries\":{\"bulk\":0,\"search\":0},\"throttled_millis\":0,\"requests_per_second\":-1,"
                        + "\"throttled_until_millis\":0,\"failures\":[]}")));
        defaults.add(new Route("POST", p -> p.endsWith("/_refresh"),
                r -> StubResponse.ok("{\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}")));
        defaults.add(new Route("PUT", p -> p.endsWith("/_mapping"), r -> StubResponse.ok("{\"acknowledged\":true}")));
        defaults.add(new Route("HEAD", p -> !p.startsWith("/_"), r -> StubResponse.status(404, "")));
        defaults.add(new Route("PUT", p -> !p.startsWith("/_"),
                r -> StubResponse.ok("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"" + r.path().substring(1) + "\"}")));
        defaults.add(new Route("DELETE", p -> !p.startsWith("/_"), r -> StubResponse.ok("{\"acknowledged\":true}")));
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public RestClient restClient() {
        return restClient;
    }

    public ElasticsearchClient client() {
        return client;
    }

    public List<RecordedRequest> requests() {
        return requests;
    }

    public List<RecordedRequest> requests(String method, String path) {
        return requests.stream().filter(r -> r.is(method, path)).toList();
    }

    public void clearRequests() {
        requests.clear();
    }

    public StubElasticsearch withVersion(String number) {
        this.versionNumber = number;
        return this;
    }

    public StubElasticsearch withDistribution(String distribution) {
        this.distribution = distribution;
        return this;
    }

    /**
     * Serves the cluster under a context path, as a reverse proxy would: the prefix is stripped
     * before routing, while recorded requests keep the full path the client sent.
     */
    public StubElasticsearch withPathPrefix(String prefix) {
        this.pathPrefix = prefix == null ? "" : prefix.replaceAll("/+$", "");
        return this;
    }

    /**
     * Queues one response for the next request to this method and path; queued responses are
     * consumed in order before the defaults apply.
     */
    public StubElasticsearch enqueue(String method, String path, Function<RecordedRequest, StubResponse> handler) {
        scripted.computeIfAbsent(key(method, path), k -> new ArrayDeque<>()).add(handler);
        return this;
    }

    public StubElasticsearch enqueue(String method, String path, StubResponse response) {
        return enqueue(method, path, r -> response);
    }

    /**
     * Queues one bulk response answering each action through the given function.
     */
    public StubElasticsearch enqueueBulk(Function<BulkAction, ItemResult> outcome) {
        return enqueue("POST", "/_bulk", r -> bulkResponse(r.bulkActions(), outcome));
    }

    /**
     * Replaces the default for a method and path predicate; scripted responses still win.
     */
    public StubElasticsearch withDefault(String method, Predicate<String> path, Function<RecordedRequest, StubResponse> handler) {
        defaults.add(0, new Route(method, path, handler));
        return this;
    }

    @Override
    public void close() throws IOException {
        restClient.close();
        server.stop(0);
    }

    public String versionDocument() {
        return "{\"name\":\"stub\",\"cluster_name\":\"stub\",\"version\":{\"number\":\"" + versionNumber + "\","
                + "\"distribution\":\"" + distribution + "\",\"build_flavor\":\"default\"},\"tagline\":\"You Know, for Search\"}";
    }

    public static StubResponse bulkResponse(List<BulkAction> actions, Function<BulkAction, ItemResult> outcome) {
        final ObjectNode root = JSON.createObjectNode();
        root.put("took", 1);
        final ArrayNode items = root.putArray("items");
        boolean errors = false;
        for (BulkAction action : actions) {
            final ItemResult result = outcome.apply(action);
            final ObjectNode item = items.addObject().putObject(action.operation());
            item.put("_index", action.index());
            if (action.id() != null) {
                item.put("_id", action.id());
            }
            item.put("status", result.status());
            if (result.errorType() != null) {
                errors = true;
                item.putObject("error").put("type", result.errorType()).put("reason", result.errorReason());
            }
            else {
                item.put("result", "delete".equals(action.operation()) ? (result.status() == 404 ? "not_found" : "deleted") : "created");
            }
        }
        root.put("errors", errors);
        return StubResponse.ok(root.toString());
    }

    public static List<BulkAction> parseBulk(String body) {
        final List<BulkAction> actions = new ArrayList<>();
        final String[] lines = body.split("\n");
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].isBlank()) {
                continue;
            }
            try {
                final JsonNode actionLine = JSON.readTree(lines[i]);
                final String operation = actionLine.fieldNames().next();
                final JsonNode meta = actionLine.get(operation);
                final String source = "delete".equals(operation) ? null : lines[++i];
                actions.add(new BulkAction(operation, meta.path("_index").asText(null), meta.path("_id").asText(null), source));
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Malformed bulk line: " + lines[i], e);
            }
        }
        return actions;
    }

    private void handle(HttpExchange exchange) throws IOException {
        final RecordedRequest request = record(exchange);
        final StubResponse response = respond(request);
        if (response.drop()) {
            exchange.close();
            return;
        }
        final byte[] body = response.body() == null ? new byte[0] : response.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("X-Elastic-Product", "Elasticsearch");
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        if ("HEAD".equalsIgnoreCase(request.method()) || body.length == 0) {
            exchange.sendResponseHeaders(response.status(), -1);
        }
        else {
            exchange.sendResponseHeaders(response.status(), body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
        exchange.close();
    }

    private RecordedRequest record(HttpExchange exchange) throws IOException {
        final Map<String, List<String>> headers = new LinkedHashMap<>();
        exchange.getRequestHeaders().forEach((k, v) -> headers.put(k.toLowerCase(), List.copyOf(v)));
        byte[] bytes = exchange.getRequestBody().readAllBytes();
        if (headers.getOrDefault("content-encoding", List.of()).contains("gzip")) {
            bytes = new GZIPInputStream(new java.io.ByteArrayInputStream(bytes)).readAllBytes();
        }
        final RecordedRequest request = new RecordedRequest(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                exchange.getRequestURI().getRawQuery(), headers, new String(bytes, StandardCharsets.UTF_8));
        requests.add(request);
        return request;
    }

    private StubResponse respond(RecordedRequest request) {
        final String routePath = routePath(request.path());
        final Deque<Function<RecordedRequest, StubResponse>> queue = scripted.get(key(request.method(), routePath));
        if (queue != null && !queue.isEmpty()) {
            return queue.poll().apply(request);
        }
        for (Route route : defaults) {
            if (route.method().equalsIgnoreCase(request.method()) && route.path().test(routePath)) {
                return route.handler().apply(request);
            }
        }
        return StubResponse.error(404, "resource_not_found_exception", "no stub route for " + request.method() + " " + request.path());
    }

    private String routePath(String path) {
        if (pathPrefix.isEmpty() || !path.startsWith(pathPrefix)) {
            return path;
        }
        final String stripped = path.substring(pathPrefix.length());
        return stripped.isEmpty() ? "/" : stripped;
    }

    private static String key(String method, String path) {
        return method.toUpperCase() + " " + path;
    }
}
