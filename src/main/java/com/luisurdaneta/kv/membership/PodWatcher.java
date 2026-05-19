package com.luisurdaneta.kv.membership;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luisurdaneta.kv.http.Node;
import com.luisurdaneta.kv.http.NodeConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-node Kubernetes API watch for Pod readiness.
 *
 * Design:
 *  - NO leader. Every node runs this independently and watches the same resource.
 *  - Initial list to seed state, then watch from that resourceVersion.
 *  - On 410 Gone / timeout / error: re-list and resume. A dropped watch never crashes
 *    the node or wedges membership.
 *  - Uses raw java.net.http.HttpClient + cluster service-account credentials.
 *    No extra dependency beyond Jackson (already present).
 */
public final class PodWatcher {

    private static final System.Logger LOG = System.getLogger(PodWatcher.class.getName());
    private static final String SA_PATH  = "/var/run/secrets/kubernetes.io/serviceaccount/";
    private static final String API_BASE = "https://kubernetes.default.svc";
    private static final long   BACKOFF_MS = 5_000;

    private final MembershipView membershipView;
    private final NodeConfig     config;
    private final ObjectMapper   mapper;

    // Local map: pod name → Node (only ready pods present)
    private final Map<String, Node> readyPods = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private HttpClient httpClient; // built lazily (needs cluster certs at runtime)

    public PodWatcher(MembershipView membershipView, NodeConfig config, ObjectMapper mapper) {
        this.membershipView = membershipView;
        this.config         = config;
        this.mapper         = mapper;
    }

    /** Starts the watch loop on a daemon thread. Returns immediately. */
    public void start() {
        Thread t = new Thread(this::watchLoop, "pod-watcher");
        t.setDaemon(true);
        t.start();
    }

    public void stop() {
        running = false;
    }

    // ── watch loop ──────────────────────────────────────────────────────────

    private void watchLoop() {
        while (running) {
            try {
                ensureHttpClient();
                String resourceVersion = listAndSeed();
                watchStream(resourceVersion);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING,
                        "PodWatcher error; will re-list in " + BACKOFF_MS + "ms: " + e.getMessage());
                try { Thread.sleep(BACKOFF_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    // ── list (seed) ──────────────────────────────────────────────────────────

    private String listAndSeed() throws Exception {
        String url = API_BASE + "/api/v1/namespaces/"
                + URLEncoder.encode(config.namespace(), StandardCharsets.UTF_8)
                + "/pods?labelSelector="
                + URLEncoder.encode(config.labelSelector(), StandardCharsets.UTF_8);

        JsonNode body = getJson(url);

        String rv = body.path("metadata").path("resourceVersion").asText("");
        readyPods.clear();
        for (JsonNode item : body.path("items")) {
            processItem(item);
        }
        publishMembers();
        return rv;
    }

    // ── watch stream ──────────────────────────────────────────────────────────

    private void watchStream(String resourceVersion) throws Exception {
        String url = API_BASE + "/api/v1/namespaces/"
                + URLEncoder.encode(config.namespace(), StandardCharsets.UTF_8)
                + "/pods?watch=true"
                + "&resourceVersion=" + URLEncoder.encode(resourceVersion, StandardCharsets.UTF_8)
                + "&labelSelector=" + URLEncoder.encode(config.labelSelector(), StandardCharsets.UTF_8);

        HttpRequest req = buildRequest(url, Duration.ofMinutes(5));
        HttpResponse<InputStream> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofInputStream());

        if (resp.statusCode() == 410) {
            // Gone — resourceVersion too old; caller will re-list
            LOG.log(System.Logger.Level.INFO, "Watch returned 410 Gone; re-listing");
            return;
        }
        if (resp.statusCode() != 200) {
            throw new IOException("Watch stream returned HTTP " + resp.statusCode());
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null && running) {
                if (line.isBlank()) continue;
                processWatchLine(line);
            }
        }
        // Stream ended (server closed it) — re-list on next iteration
    }

    private void processWatchLine(String line) throws IOException {
        JsonNode event = mapper.readTree(line);
        String type = event.path("type").asText();
        JsonNode object = event.path("object");

        if ("ERROR".equals(type)) {
            int code = object.path("code").asInt(0);
            if (code == 410) throw new RuntimeException("Watch error: 410 Gone");
            LOG.log(System.Logger.Level.WARNING, "Watch ERROR event: " + object.path("message").asText());
            return;
        }

        if ("DELETED".equals(type)) {
            String podName = object.path("metadata").path("name").asText();
            readyPods.remove(podName);
        } else {
            // ADDED or MODIFIED
            processItem(object);
        }
        publishMembers();
    }

    // ── pod processing ────────────────────────────────────────────────────────

    private void processItem(JsonNode pod) {
        JsonNode meta = pod.path("metadata");
        String podName = meta.path("name").asText();

        // Treat pods with a deletionTimestamp as not ready
        if (!meta.path("deletionTimestamp").isMissingNode()) {
            readyPods.remove(podName);
            return;
        }

        boolean ready = isReady(pod.path("status"));
        if (ready) {
            readyPods.put(podName, podToNode(podName));
        } else {
            readyPods.remove(podName);
        }
    }

    private boolean isReady(JsonNode status) {
        for (JsonNode cond : status.path("conditions")) {
            if ("Ready".equals(cond.path("type").asText())
                    && "True".equals(cond.path("status").asText())) {
                return true;
            }
        }
        return false;
    }

    private Node podToNode(String podName) {
        String host = podName + "." + config.headlessService()
                + "." + config.namespace() + ".svc.cluster.local";
        return new Node(podName, "http://" + host + ":" + config.port());
    }

    private void publishMembers() {
        membershipView.updateFromWatch(new HashSet<>(readyPods.values()));
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    private JsonNode getJson(String url) throws Exception {
        HttpRequest req = buildRequest(url, Duration.ofSeconds(15));
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("List pods returned HTTP " + resp.statusCode());
        }
        return mapper.readTree(resp.body());
    }

    private HttpRequest buildRequest(String url, Duration timeout) throws Exception {
        String token = Files.readString(Path.of(SA_PATH + "token")).trim();
        return HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .timeout(timeout)
                .GET()
                .build();
    }

    // ── SSL setup ────────────────────────────────────────────────────────────

    private void ensureHttpClient() throws Exception {
        if (httpClient != null) return;
        httpClient = HttpClient.newBuilder()
                .sslContext(buildSslContext())
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    private static SSLContext buildSslContext() throws Exception {
        Path caCertPath = Path.of(SA_PATH + "ca.crt");
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate caCert;
        try (InputStream is = Files.newInputStream(caCertPath)) {
            caCert = (X509Certificate) cf.generateCertificate(is);
        }

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        ks.setCertificateEntry("k8s-ca", caCert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(null, tmf.getTrustManagers(), null);
        return ssl;
    }
}
