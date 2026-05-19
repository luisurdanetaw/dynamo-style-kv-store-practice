package com.luisurdaneta.kv.http;

import com.luisurdaneta.kv.core.ports.KvStore;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.ring.RingManager;
import com.luisurdaneta.kv.core.service.ReadCoordinatorService;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.core.service.WriteCoordinatorService;
import com.luisurdaneta.kv.membership.MembershipView;

import java.util.List;

public record NodeContext(
        NodeConfig config,
        MembershipView membershipView,
        RingManager ringManager,
        ReplicaKvService replicaKvService,
        WriteCoordinatorService writeCoordinatorService,
        ReadCoordinatorService readCoordinatorService,
        PeerClient peerClient,
        KvStore kvStore
) {
    /** Current ring snapshot — convenience for callers that don't need the manager. */
    public ConsistentHashRing ring() { return ringManager.currentRing(); }

    /** Current member set as a list — convenience for existing callers (e.g. WhoamiHandler). */
    public List<Node> peers() { return List.copyOf(membershipView.currentMembers()); }
}
