package com.luisurdaneta.kv.adapters.peer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class PeerHttp {
    public static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    private PeerHttp() {}

    public static HttpResponse<byte[]> getBytes(String url, Duration timeout) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .GET()
                .build();
        return CLIENT.send(req, HttpResponse.BodyHandlers.ofByteArray());
    }

    public static HttpResponse<byte[]> putJson(String url, byte[] jsonBody, Duration timeout) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(jsonBody))
                .build();
        return CLIENT.send(req, HttpResponse.BodyHandlers.ofByteArray());
    }
}
