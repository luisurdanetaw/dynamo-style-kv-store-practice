# kv-node

A distributed key-value store inspired by Amazon Dynamo, built on RocksDB and deployed as a Kubernetes StatefulSet. It prioritizes availability and partition tolerance over strong consistency, using quorum-based replication, last-write-wins conflict resolution, and decentralized cluster membership via per-node Kubernetes API watches.

---

## What this is

- A **leaderless, AP-style key-value store** — every node can serve reads and writes independently.
- A learning artifact that implements, end-to-end in a single codebase, the core mechanisms from the Dynamo paper: consistent hashing, quorum replication, LWW conflict resolution, read repair, and incremental data rebalancing on membership changes.
- Deployable into a real Kubernetes cluster via the included Helm chart (RBAC, PVCs, probes, headless service).

## What this is not

- **Not strongly consistent.** There is no linearizability guarantee. Concurrent writes to the same key from different coordinators can race under clock skew or network partitions.
- **No gossip protocol.** Membership is sourced from the Kubernetes API. There is no SWIM or Phi-accrual failure detection.
- **No anti-entropy / Merkle-tree reconciliation.** Stale replicas are only healed by read repair (on-path) or by the rebalancer (on ring change). A replica that misses writes and is never read will stay stale indefinitely.
- **Tombstones are not garbage-collected** on a stable cluster. They accumulate until a rebalance drops the key's arc from a node.
- **No authentication** on the public HTTP API. The server is designed to run inside a private Kubernetes network.

---

## Architecture

```
 Client
   │  PUT /kv/{key}   GET /kv/{key}   DELETE /kv/{key}
   ▼
┌──────────────────────────────────────────────────────────────────┐
│  Any kv-node  (all nodes are equal — no primaries, no leaders)  │
│                                                                  │
│  WriteCoordinatorService         ReadCoordinatorService          │
│  ├─ hash key → RF=3 replicas     ├─ hash key → RF=3 replicas    │
│  ├─ fan out (virtual threads)    ├─ fan out (virtual threads)    │
│  ├─ wait for W=2 acks            ├─ wait for R=2 acks            │
│  └─ cancel stragglers            ├─ LWW pick winner              │
│                                  └─ async read repair            │
│                                                                  │
│  RingManager ─── AtomicReference<ConsistentHashRing>            │
│  ├─ bootstrap ring (StatefulSet DNS) → serves immediately        │
│  └─ watch ring (K8s API, debounced) → replaces bootstrap         │
│                                                                  │
│  MembershipView                  Rebalancer (background thread)  │
│  ├─ initFromSeeds  (provisional) ├─ Phase 1: pull gained arcs   │
│  └─ updateFromWatch (authritative) └─ Phase 2: push+drop stale  │
│                                                                  │
│  RocksDB  (LZ4, persistent volume, WAL-backed)                  │
└──────────────────────────────────────────────────────────────────┘
         │ /internal/replica/kv          │ /internal/transfer/range
         │  (quorum replication)         │  (rebalancing)
         ▼                               ▼
     Other kv-nodes                Other kv-nodes
```

---

## Data model

### VersionedValue

Every stored record is a `VersionedValue`:

| Field       | Type      | Description                                                         |
|-------------|-----------|---------------------------------------------------------------------|
| `ts`        | `long`    | Wall-clock milliseconds at the coordinating node at write time      |
| `nodeId`    | `String`  | ID of the node that originated the write (LWW tiebreaker)           |
| `tombstone` | `boolean` | `true` for soft-deletes                                             |
| `payload`   | `byte[]`  | Raw binary value (any encoding, null on tombstone)                  |

**Binary layout** (stored in RocksDB):
```
[ts: 8 bytes][tombstone: 1 byte][nodeIdLen: 4 bytes][nodeId][payloadLen: 4 bytes][payload]
```

### Last-Write-Wins ordering

`isNewerThan(other)` defines a total order:
1. Higher `ts` wins.
2. On equal `ts`, higher `nodeId` lexicographically wins.

This comparison drives `putIfNewer` at the storage layer. Because LWW is commutative and associative, records can be merged in any order — including duplicate delivery — without diverging.

---

## Consistency model

| Parameter | Value | Meaning                                        |
|-----------|-------|------------------------------------------------|
| RF        | 3     | Every key is stored on 3 physical nodes        |
| W         | 2     | Write succeeds when 2 of 3 replicas ack        |
| R         | 2     | Read succeeds when 2 of 3 replicas respond     |
| W + R     | 4 > RF | Quorum overlap: at least one reader always holds the latest write |

### Guarantees

- **Quorum overlap.** W + R = 4 > RF = 3, so the read and write quorums always share at least one node on a healthy cluster. A successful quorum read always sees the latest successful quorum write.
- **Eventual convergence.** Read repair and rebalancing propagate the winning value to stale replicas over time.
- **Durability.** A successful write (W=2 acks) is on at least 2 nodes' WALs. Losing 1 node does not lose data.

### Non-guarantees

- **Not linearizable.** Two concurrent writers can each reach a disjoint W=2 quorum if the cluster is partitioned. The higher-timestamp write eventually wins everywhere, but there is no global ordering constraint during the partition.
- **No session consistency across nodes.** A client reading from two different coordinators in sequence has no monotonic read guarantee.
- **No causal consistency.** There are no vector clocks; a write that causally depends on a prior read is not guaranteed to be observed after it.
- **Clock skew can cause lost updates.** LWW relies on wall-clock timestamps. A node with a clock skewed backward can write records that are immediately beaten by LWW.

### Read repair

When a quorum read collects disagreeing values, the winning `VersionedValue` is asynchronously pushed back to lagging replicas via `putReplica`. This is fire-and-forget — the client response is not delayed. A replica that is never read will never be repaired by this mechanism alone.

### Tombstones

`DELETE /kv/{key}` writes a tombstone (same quorum path as a PUT). The tombstone propagates to all replicas through quorum replication and read repair. `GET /kv/{key}` returns `404` for tombstoned keys; the internal replica API exposes tombstones so they propagate correctly. Tombstone records are physically deleted from a node only when that node loses the key's arc during a rebalance.

---

## Storage

**Engine:** RocksDB 9.x (Java JNI)

| Property       | Value                               |
|----------------|-------------------------------------|
| Compression    | LZ4 per SST file                    |
| Durability     | WAL-backed (RocksDB default)        |
| Key encoding   | UTF-8 bytes, lexicographically ordered |
| Value encoding | Custom binary (see above)           |
| Deletes        | Physical (`db.delete(key)`) — no tombstone at storage level |
| Data path      | `/var/lib/kv/rocksdb` (configurable via `DB_PATH`) |

The `KvStore` interface (`get`, `putIfNewer`, `scan`, `drop`) isolates RocksDB from the rest of the system.

`scan` opens a `RocksIterator` inside a single call, invokes a `BiPredicate<String, VersionedValue>` visitor for each entry (supports early-stop via `false` return), and closes the iterator before returning. No iterator is ever held open across a network call.

---

## Consistent hashing

### Ring construction

Each physical node is placed at **64 virtual node (vnode) positions** on a 64-bit hash ring:

```
position(node N, vnode i) = first 8 bytes of SHA-256("N#i"), interpreted as signed int64
```

The ring is stored as a sorted `NavigableMap<Long, Node>`, giving O(log n) successor lookup.

### Key placement

For a key `k`, the replica set is the first RF unique physical nodes encountered walking clockwise from `hash64(k)`. `hash64` is deterministic and uses SHA-256, so all nodes independently compute identical replica sets for any key.

### Replica arcs

`replicaArcs(nodeId, rf)` returns all half-open token ranges `(start, end]` where `nodeId` appears among the top-RF replicas. These arcs define the keyspace a node is responsible for. `TokenRange.contains(hash)` handles ring wrap-around (when `start > end`).

### Monotonicity

Consistent hashing is monotone on node removal: when node X is removed, remaining nodes can only gain key assignments, never lose them. Removing X from the clockwise traversal cannot displace a node Y that was already within the first RF positions for any given key. The inverse is also true: a node sheds keys only when new nodes are added, as its arc fraction shrinks from `RF/old_n` to `RF/new_n`.

This property is the reason the rebalancer's `findKeyLostBy` logic tests the add-node direction and why the skipped-test scenario (removing a node and expecting n0 to shed keys) is structurally impossible.

### Load distribution

With 64 vnodes and SHA-256, load imbalance stays within ±10–15% for clusters up to ~20 nodes. The vnode count is configurable.

---

## Replication

### Write path

```
PUT /kv/{key}
  1. ts  = clock.nowMillis(), nodeId = this node
  2. replicas = ring.replicasForKey(key, RF=3)
  3. Submit putReplica() to each replica on a virtual thread
  4. Wait for W=2 acks via CompletionService (overall timeout 800ms)
  5. Cancel stragglers once quorum is reached
  6. Return 200 {ok, key, acks, required, ts, nodeId}
     or 503 if quorum not reached in time
```

`putReplica` sends `{ts, nodeId, tombstone, payloadBase64}` to `/internal/replica/kv/{key}` on the target node. The receiver stores it via `putIfNewer` (LWW, idempotent, concurrent-safe).

### Read path

```
GET /kv/{key}
  1. replicas = ring.replicasForKey(key, RF=3)
  2. Submit getReplica() to each replica on a virtual thread
  3. Wait for R=2 responses (overall timeout 800ms)
  4. Winner = max(ts), break ties by max(nodeId)
  5. Async read repair to replicas holding older values
  6. Return 200 binary body + X-KV-Ts / X-KV-NodeId headers
     or 404 if winner is tombstone or not found
```

---

## Cluster membership

### Bootstrap phase

1. Compute seed peers from StatefulSet DNS: `kv-{0..N-1}.kv-headless.{namespace}.svc.cluster.local`.
2. Build a **bootstrap ring** from seeds and start serving immediately. Membership version = 0. The bootstrap ring is provisional — it may list pods that are not yet ready.
3. Start `PodWatcher` as a daemon thread.

The bootstrap ring allows the node to accept traffic within milliseconds of starting, before the Kubernetes API watch is established.

### Watch phase (authoritative)

`PodWatcher` independently watches the Kubernetes API on each node — there is no central watcher or leader:

1. **List**: `GET /api/v1/namespaces/{ns}/pods?labelSelector={selector}` → extract ready pods (Ready condition = True, no `deletionTimestamp`) → call `membershipView.updateFromWatch()`.
2. **Watch**: Stream NDJSON events from the returned `resourceVersion`. Handle `ADDED`, `MODIFIED`, `DELETED`, `BOOKMARK`.
3. On HTTP 410 Gone or stream timeout: re-list from scratch.
4. On any error: sleep 5 seconds, retry.

Auth: in-cluster service account token + CA cert at `/var/run/secrets/kubernetes.io/serviceaccount/`.

### MembershipView semantics

| Method              | Version | Listeners | Used for        |
|---------------------|---------|-----------|-----------------|
| `initFromSeeds()`   | stays 0 | not fired | provisional bootstrap |
| `updateFromWatch()` | +1      | fired     | authoritative watch |

Once `updateFromWatch` fires, the seed list is abandoned and never re-consulted. The membership version is a monotone counter used to stamp rebalance operations.

### Debounce

`RingManager` debounces membership changes by 3 seconds. A rolling update that flips 3 pods from NotReady → Ready over 2 seconds produces one ring rebuild and one rebalancer invocation, not three. The rebuilt ring is published atomically via `AtomicReference` — reads of `currentRing()` are a single `get()` with no locking.

`previousRing` in `RingManager` is `null` until the first watch sync, triggering the restart re-pull strategy below.

---

## Rebalancing

`Rebalancer.onRingChanged(oldRing, newRing, version)` is enqueued on a single background thread. Both coordinator paths see an updated ring immediately; the rebalancer catches up asynchronously.

### Phase 1 — Pull gained arcs

1. Compute gained arcs: token ranges where this node is now among the top-RF replicas but was not in `oldRing` (or all arcs if `oldRing == null`).
2. For each gained arc: identify a source peer (old primary for the arc) and pull all keys via paginated `pullRangePage` calls (500 keys/page by default).
3. Each page is written via `putReplica` (LWW-safe, idempotent) before the next page is requested — memory is bounded regardless of dataset size.

### Phase 2 — Push and drop stale keys

1. Scan local store in batches of 100 keys.
2. For each key where this node is no longer in the new ring's replica set: push the key to all new owners via `putReplica`.
3. **Confirm-before-delete**: drop the key locally only after every push succeeds. On any failure, retain the key. It will be pushed again on the next membership change.

### Restart re-pull (oldRing == null)

On first watch sync, Phase 1 re-pulls all current replica arcs from peers. Since `putIfNewer` is idempotent, re-pulling data already present is a no-op. Phase 2 is skipped on restart — nothing can be stale yet. This strategy needs no persistent progress log.

---

## HTTP API

### Public API

| Method     | Path           | Request body | Success response |
|------------|----------------|--------------|------------------|
| `PUT`      | `/kv/{key}`    | raw bytes    | `200 {"ok":true,"key":"…","acks":2,"required":2,"ts":…,"nodeId":"…"}` |
| `GET`      | `/kv/{key}`    | —            | `200` raw bytes + `X-KV-Ts`, `X-KV-NodeId` headers |
| `DELETE`   | `/kv/{key}`    | —            | `200 {"ok":true,"deleted":true,"key":"…","acks":2,"required":2,"ts":…}` |

`GET` returns `404` when the key is absent or tombstoned. `PUT`/`DELETE` return `503` when fewer than W acks are collected within the timeout.

### Internal replication API

| Method | Path                            | Description |
|--------|---------------------------------|-------------|
| `PUT`  | `/internal/replica/kv/{key}`   | Accept a replica write. Body: `{ts, nodeId, tombstone, payloadB64}` |
| `GET`  | `/internal/replica/kv/{key}`   | Fetch a replica record (tombstones visible to peers) |

### Rebalancing transfer API

| Method | Path                            | Description |
|--------|---------------------------------|-------------|
| `GET`  | `/internal/transfer/range`      | Paginated range scan for a token arc |

Query params: `start` (long), `end` (long), `cursor` (last key from prior page, optional), `pageSize`, `membershipVersion` (informational — logged but does not gate access).

Response: `{"entries":[{"key":"…","record":{…}}…],"nextCursor":"lastKey"|null}`

### Debug / admin

| Path                        | Description                                        |
|-----------------------------|----------------------------------------------------|
| `GET /healthz`              | `{"ok":true,"ts":"…"}` — liveness and readiness    |
| `GET /whoami`               | Node identity, config, current peer list           |
| `GET /debug/replicas?key=X` | Lists the RF replica nodes responsible for key X   |

---

## Configuration

| Environment variable | Default                           | Description |
|----------------------|-----------------------------------|-------------|
| `PORT`               | `8080`                            | HTTP listen port |
| `POD_NAME`           | —                                 | Injected by downward API (`metadata.name`) |
| `POD_NAMESPACE`      | `default`                         | Pod namespace |
| `HEADLESS_SERVICE`   | `kv-headless`                     | Headless service name for seed DNS resolution |
| `REPLICAS`           | `3`                               | StatefulSet replica count (seed list size) |
| `LABEL_SELECTOR`     | `app.kubernetes.io/name=kv-node`  | Pod label filter for Kubernetes API watch |
| `DB_PATH`            | `/var/lib/kv/rocksdb`             | RocksDB data directory |

Hard-coded tuning (in `Main.java` — edit to change):

| Constant            | Value     | Description |
|---------------------|-----------|-------------|
| RF                  | 3         | Replication factor |
| W                   | 2         | Write quorum |
| R                   | 2         | Read quorum |
| perRequestTimeout   | 300 ms    | Per-peer RPC deadline |
| overallTimeout      | 800 ms    | Total quorum budget per request |
| debounceMs          | 3 000 ms  | Ring rebuild debounce window |
| vnodes              | 64        | Virtual nodes per physical node |
| CLEANUP_BATCH_SIZE  | 100       | Keys per scan batch in rebalance Phase 2 |
| TRANSFER_TIMEOUT    | 30 s      | Per-page timeout in rebalance Phase 1 pull |
| PUSH_TIMEOUT        | 5 s       | Per-peer timeout in rebalance Phase 2 push |

---

## Kubernetes deployment

```
helm/
├── Chart.yaml
├── values.yaml
└── templates/
    ├── statefulset.yaml       3-replica StatefulSet, PVC, downward-API env injection
    ├── service.yaml           ClusterIP service for client traffic (port 8080)
    ├── service-headless.yaml  Headless service for peer DNS (kv-{N}.kv-headless.…)
    └── rbac.yaml              ServiceAccount + Role (pods: get/list/watch) + RoleBinding
```

### Quick start with kind

```bash
# Build
mvn package -DskipTests
docker build -t kv-node:0.1.0 .

# Load into kind cluster
kind load docker-image kv-node:0.1.0 --name k8s

# Deploy
helm upgrade --install kv ./helm \
  --set image.repository=kv-node \
  --set image.tag=0.1.0 \
  --set image.pullPolicy=IfNotPresent \
  --set replicaCount=3

# Watch pods come up
kubectl get pods -l app.kubernetes.io/name=kv-node -w
```

### Key `values.yaml` options

```yaml
replicaCount: 3
image:
  repository: kv-node
  tag: "0.1.0"
  pullPolicy: IfNotPresent
service:
  port: 8080
persistence:
  size: 1Gi
  db:
    path: /var/lib/kv/rocksdb
```

---

## Building and running locally

Requires JDK 25.

```bash
# Build
mvn package

# Single-node smoke test (no Kubernetes; no watch; serves from bootstrap ring only)
POD_NAME=kv-0 REPLICAS=1 java -jar target/kv-node-0.1.0.jar

# Tests (25 tests, ~5 seconds)
mvn test
```

Test suites:

| Suite                    | Tests | What it covers |
|--------------------------|-------|----------------|
| `ConsistentHashRingTest` | 8     | Hash determinism, replica selection, arc coverage, wrap-around |
| `MembershipViewTest`     | 7     | Seed vs. watch semantics, version bumps, listener notification |
| `RebalancerTest`         | 6     | Restart re-pull, LWW protection, lost-key push+drop, push-confirm-before-drop, failure retention |
| `BootstrapTest`          | 4     | Provisional serving, watch transition, seed abandonment, debounce |

---

## Operational notes

**Rolling updates:** The 3-second debounce absorbs a 3-pod rolling update into a single rebalance pass. Pods excluded from the ring until their readiness probe passes.

**Data safety during rebalance:** A key is never dropped locally until all new owners confirm receipt (confirm-before-delete). A failed push retains the key in place; it will be retried on the next membership change.

**Stale replicas:** Without anti-entropy, a replica that misses writes heals only via read repair (if the key is read) or via Phase 1 re-pull (if the ring changes). A key that is never read and whose ring assignment never changes will remain stale indefinitely on that replica.

**Tombstone growth:** In workloads with heavy deletes on a stable cluster topology, tombstone records accumulate on all replicas without bound. They are only physically removed from a node when that node loses the key's arc in a rebalance.

**Clock skew:** LWW correctness assumes reasonably synchronized clocks. Nodes with clocks skewed backward can produce writes that lose LWW races to older data. NTP or equivalent time synchronization is assumed.
