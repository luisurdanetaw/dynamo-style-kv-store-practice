package com.luisurdaneta.kv.http;

import java.util.ArrayList;
import java.util.List;

public final class PeerDiscovery {
    private PeerDiscovery() {}

    public static List<Node> computePeers(String podName, int replicas, String headless, String namespace, int port) {
        String baseName = podName.contains("-")
                ? podName.substring(0, podName.lastIndexOf('-'))
                : podName;

        List<Node> peers = new ArrayList<>(replicas);
        for (int i = 0; i < replicas; i++) {
            String id = baseName + "-" + i;
            String host = id + "." + headless + "." + namespace + ".svc.cluster.local";
            String url = "http://" + host + ":" + port;
            peers.add(new Node(id, url));
        }
        return peers;
    }
}
