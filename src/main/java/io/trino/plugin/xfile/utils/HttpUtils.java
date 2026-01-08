package io.trino.plugin.xfile.utils;

import io.trino.plugin.xfile.XFileConnector;
import io.trino.plugin.xfile.XFileInternalColumn;

import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.util.stream.Collectors;

public class HttpUtils {

    static Map<String, HttpClient> httpClients = new HashMap<>();

    public static InputStream submitHttpRequest(
            String url,
            String method,
            Map<String, String> headers,
            Map<String, String> params,
            String body,
            String proxyHost,
            int proxyPort) throws Exception {

        HttpClient client;
        if(proxyHost != null && proxyPort > 0) {
            client = httpClients.putIfAbsent(proxyHost + proxyPort, HttpClient.newBuilder()
                    .proxy(ProxySelector.of(new InetSocketAddress(proxyHost, proxyPort)))
                    .version(HttpClient.Version.HTTP_2)
                    .build());
        } else {
            client = httpClients.putIfAbsent(proxyHost + proxyPort, HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .build());
        }

        HttpRequest.BodyPublisher bodyPublisher = (body == null || body.isEmpty())
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .method(method.toUpperCase(), bodyPublisher);

        if (headers != null) {
            headers.forEach(requestBuilder::header);
        }

        HttpResponse<InputStream> response = client.send(
                requestBuilder.build(),
                HttpResponse.BodyHandlers.ofInputStream()
        );

        return response.body();
    }

    public static InputStream submitHttpRequest(Map<String, Object> properties) {
        try {
            return HttpUtils.submitHttpRequest(
                    (String) properties.getOrDefault(XFileInternalColumn.HTTP_URL.getName(), properties.get(XFileConnector.TABLE_PROP_HTTP_URL)),
                    (String) properties.getOrDefault(XFileInternalColumn.HTTP_METHOD.getName(), properties.getOrDefault(XFileConnector.TABLE_PROP_HTTP_METHOD, "GET")),
                    HttpUtils.parseQuery((String) properties.getOrDefault(XFileInternalColumn.HTTP_HEADERS.getName(), properties.get(XFileConnector.TABLE_PROP_HTTP_BODY))),
                    HttpUtils.parseQuery((String) properties.getOrDefault(XFileInternalColumn.HTTP_PARAMS.getName(), properties.get(XFileConnector.TABLE_PROP_HTTP_PARAMS))),
                    (String) properties.getOrDefault(XFileInternalColumn.HTTP_BODY.getName(), properties.get(XFileConnector.TABLE_PROP_HTTP_BODY)),
                    (String) properties.getOrDefault(XFileInternalColumn.HTTP_PROXY_HOST.getName(), properties.getOrDefault(XFileConnector.TABLE_PROP_HTTP_PROXY_HOST, "GET")),
                    Integer.parseInt((String) properties.getOrDefault(XFileInternalColumn.HTTP_PROXY_PORT.getName(), properties.getOrDefault(XFileConnector.TABLE_PROP_HTTP_PROXY_PORT,"80")))
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static Map<String, String> parseQuery(String query) {
        if (query == null || query.isEmpty()) return Collections.emptyMap();

        return Arrays.stream(query.split("&"))
                .map(param -> param.split("=", 2))
                .collect(Collectors.toMap(
                        pair -> URLDecoder.decode(pair[0], StandardCharsets.UTF_8), // Key
                        pair -> pair.length > 1 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "", // Value
                        (oldValue, newValue) -> oldValue // Handle duplicate keys: keep first
                ));
    }
}
