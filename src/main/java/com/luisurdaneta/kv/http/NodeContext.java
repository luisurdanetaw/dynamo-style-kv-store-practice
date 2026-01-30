package com.luisurdaneta.kv.http;

import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.service.ReadCoordinatorService;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.core.service.WriteCoordinatorService;

import java.util.List;

public record NodeContext(
        NodeConfig config,
        List<Node> peers,
        ConsistentHashRing ring,
        ReplicaKvService replicaKvService,
        WriteCoordinatorService writeCoordinatorService,
        ReadCoordinatorService readCoordinatorService,
        PeerClient peerClient
) {}
