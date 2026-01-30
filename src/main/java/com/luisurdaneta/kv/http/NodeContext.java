package com.luisurdaneta.kv.http;

import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.service.KvService;
import com.luisurdaneta.kv.core.service.ReplicaKvService;

import java.util.List;

public record NodeContext(
        NodeConfig config,
        List<Node> peers,
        ConsistentHashRing ring,
        KvService kvService,
        ReplicaKvService replicaKvService
) {}
