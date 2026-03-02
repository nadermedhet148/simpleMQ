package io.dist.cluster;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.dist.model.RaftMetadata;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.CDI;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages the Raft consensus layer for the simpleMQ cluster.
 *
 * <p>This service initializes and manages the Apache Ratis {@link RaftServer}
 * and a shared {@link RaftClient}. It provides methods to:</p>
 * <ul>
 *   <li>Check leadership status and retrieve the current leader ID</li>
 *   <li>Replicate broker commands (publish, poll, ack, nack, exchange/queue
 *       management) through the Raft log</li>
 *   <li>Dynamically add or remove cluster peers</li>
 * </ul>
 *
 * <h3>Lifecycle</h3>
 * <p>On startup ({@link #onStart}), the Raft server is configured and started.
 * A periodic timer syncs Raft metadata (term, votedFor) to SQLite and logs
 * the current leadership state. On shutdown ({@link #onStop}), the server and
 * client are closed and temporary storage is cleaned up.</p>
 *
 * <h3>Command Replication</h3>
 * <p>All {@code replicate*} methods encode their parameters into a
 * pipe-delimited string, send it through the Raft client, and return a
 * {@link Uni<Boolean>} indicating success. The command is applied on every
 * node by the {@link SimpleStateMachine}.</p>
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code simplemq.cluster.node-id} – unique identifier for this node</li>
 *   <li>{@code simplemq.cluster.address} – Raft gRPC address (host:port)</li>
 *   <li>{@code simplemq.cluster.peers} – comma-separated list of id=address pairs</li>
 *   <li>{@code simplemq.persistence.enabled} – whether SQLite persistence is active</li>
 * </ul>
 *
 * @see SimpleStateMachine
 * @see RaftMetadata
 */
@ApplicationScoped
public class RaftService {
    private static final Logger LOG = Logger.getLogger(RaftService.class);

    static {
        // Disable DNS negative caching to help recover from transient name
        // resolution failures in Docker environments.
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
    }

    /** Unique identifier for this cluster node. */
    @ConfigProperty(name = "simplemq.cluster.node-id")
    String nodeId;

    /** Raft gRPC address for this node (host:port). */
    @ConfigProperty(name = "simplemq.cluster.address")
    String address;

    /** Comma-separated list of cluster peers in "id=address" format. */
    @ConfigProperty(name = "simplemq.cluster.peers")
    String peers;

    /** Whether SQLite persistence is enabled. */
    @ConfigProperty(name = "simplemq.persistence.enabled", defaultValue = "true")
    boolean persistenceEnabled;

    /** The underlying Apache Ratis server instance. */
    private RaftServer raftServer;

    /** Raft group ID derived from the cluster name "simpleMQ-cluster". */
    private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes("simpleMQ-cluster".getBytes()));

    /** The current Raft group (peers may change dynamically). */
    private RaftGroup raftGroup;

    /** Shared Raft client used for sending commands to the cluster. */
    private RaftClient sharedClient;

    /** The storage directory used by this node's Raft server. */
    private File currentStorageDir;

    /**
     * Initializes and starts the Raft server on application startup.
     *
     * <p>Parses the peer configuration, sets up storage (persistent under
     * {@code /data} in Docker, or a temporary directory locally), and starts
     * the Raft server and shared client.</p>
     *
     * @param ev the Quarkus startup event
     * @throws IOException if the Raft server fails to start
     */
    void onStart(@Observes StartupEvent ev) throws IOException {
        LOG.infof("Starting Raft Node: %s at %s", nodeId, address);

        RaftPeerId peerId = RaftPeerId.valueOf(nodeId);
        
        // Parse the comma-separated peers configuration into RaftPeer objects
        List<RaftPeer> raftPeers = Arrays.stream(peers.split(","))
                .map(p -> {
                    String[] parts = p.split("=");
                    return RaftPeer.newBuilder()
                            .setId(parts[0])
                            .setAddress(parts[1])
                            .build();
                })
                .collect(Collectors.toList());

        raftGroup = RaftGroup.valueOf(groupId, raftPeers);

        RaftProperties properties = new RaftProperties();
        
        // Use in-memory Raft logs to avoid file storage for logs
        RaftServerConfigKeys.Log.setUseMemory(properties, true);
        
        final int port = extractPort(address);
        GrpcConfigKeys.Server.setPort(properties, port);
        
        // Determine storage directory: /data in Docker, temp dir locally
        File storageDir;
        File dataDir = new File("/data");
        
        if (dataDir.exists() && dataDir.canWrite()) {
            // Docker environment: use persistent /data volume
            storageDir = new File(dataDir, "raft-" + nodeId);
        } else {
            // Local/test environment: use a stable temp directory.
            // Clean any leftover content first so FORMAT always starts fresh.
            storageDir = new File(System.getProperty("java.io.tmpdir"), "smq-raft-" + nodeId);
            if (storageDir.exists()) {
                LOG.infof("Cleaning stale Raft temp dir before FORMAT: %s", storageDir.getAbsolutePath());
                deleteDirectory(storageDir);
            }
        }

        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }

        this.currentStorageDir = storageDir;
        LOG.infof("Using Raft storage directory: %s", storageDir.getAbsolutePath());

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        // Use RECOVER for production (/data), otherwise FORMAT (tests/local)
        RaftStorage.StartupOption startupOption = (dataDir.exists() && dataDir.canWrite()) ? 
                RaftStorage.StartupOption.RECOVER : RaftStorage.StartupOption.FORMAT;

        raftServer = RaftServer.newBuilder()
                .setServerId(peerId)
                .setGroup(raftGroup)
                .setProperties(properties)
                .setStateMachineRegistry(gid -> {
                    LOG.infof("Creating StateMachine for group %s", gid);
                    return new SimpleStateMachine();
                })
                .setOption(startupOption)
                .build();

        LOG.infof("Starting Raft server %s...", nodeId);
        System.out.println("[RAFT_DEBUG] About to start Raft server " + nodeId);
        raftServer.start();
        System.out.println("[RAFT_DEBUG] Raft server started successfully: " + nodeId);
        LOG.infof("Raft server %s started successfully", nodeId);

        this.sharedClient = buildClient(raftGroup);
        LOG.infof("Raft shared client initialized for node %s", nodeId);
        
        // Start periodic metadata sync and leadership logging
        startSyncAndLogging();
    }

    /**
     * Starts a periodic timer that syncs Raft metadata (term, votedFor) to
     * SQLite and logs the current leadership state every 10 seconds.
     */
    private void startSyncAndLogging() {
        new Timer(true).scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    RaftServer.Division division = raftServer.getDivision(groupId);
                    
                    // Sync current term and votedFor to SQLite for durability
                    long term = division.getInfo().getCurrentTerm();
                    RaftPeerId votedFor = division.getRaftStorage().getMetadataFile().getMetadata().getVotedFor();
                    String votedForStr = votedFor != null ? votedFor.toString() : null;

                    QuarkusTransaction.requiringNew().run(() -> {
                        RaftMetadata metadata = RaftMetadata.findById(nodeId);
                        if (metadata == null) {
                            metadata = new RaftMetadata(nodeId, term, votedForStr, -1L, -1L);
                            metadata.persist();
                        } else {
                            boolean changed = false;
                            if (metadata.currentTerm < term) {
                                metadata.currentTerm = term;
                                changed = true;
                            }
                            if (!Objects.equals(metadata.votedFor, votedForStr)) {
                                metadata.votedFor = votedForStr;
                                changed = true;
                            }
                            // lastAppliedIndex/Term are updated in SimpleStateMachine
                        }
                    });

                    // Log current leadership state
                    if (division.getInfo().isLeader()) {
                        LOG.info(">>> I am the LEADER (" + nodeId + ")");
                    } else if (division.getInfo().isFollower()) {
                        LOG.info("I am a follower (" + nodeId + "). Leader is: " + division.getInfo().getLeaderId());
                    } else if (division.getInfo().isCandidate()) {
                        LOG.info("I am a candidate (" + nodeId + ")");
                    }
                } catch (Exception e) {
                    // Ignore if division not ready or other transient errors
                }
            }
        }, 5000, 10000);
    }


    /**
     * Gracefully shuts down the Raft server and client on application stop.
     * Cleans up temporary storage directories in non-Docker environments.
     *
     * @param ev the Quarkus shutdown event
     * @throws IOException if closing the server or client fails
     */
    void onStop(@Observes ShutdownEvent ev) throws IOException {
        LOG.infof("Stopping Raft Node: %s", nodeId);
        if (sharedClient != null) {
            sharedClient.close();
        }
        if (raftServer != null) {
            raftServer.close();
        }

        // In Docker, /data is mounted and uses RECOVER mode — preserve it.
        // In tests/local, /tmp is used with FORMAT mode — clean up so the next
        // run can FORMAT a fresh directory without "existing directories found" errors.
        File dataDir = new File("/data");
        if (currentStorageDir != null && currentStorageDir.exists()
                && !(dataDir.exists() && dataDir.canWrite())) {
            try {
                LOG.infof("Cleaning up temporary Raft storage directory: %s", currentStorageDir.getAbsolutePath());
                deleteDirectory(currentStorageDir);
            } catch (Exception e) {
                LOG.error("Failed to clean up temporary storage directory", e);
            }
        }
    }

    /**
     * Returns whether SQLite persistence is enabled.
     *
     * @return {@code true} if persistence is enabled
     */
    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * Recursively deletes a directory and all its contents.
     *
     * @param directoryToBeDeleted the directory to delete
     */
    private void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directoryToBeDeleted.delete();
    }

    /**
     * Extracts the port number from a "host:port" address string.
     *
     * @param address the address string
     * @return the port number, or 9851 as default
     */
    private int extractPort(String address) {
        int lastColon = address.lastIndexOf(':');
        if (lastColon >= 0) {
            try {
                return Integer.parseInt(address.substring(lastColon + 1));
            } catch (NumberFormatException e) {
                LOG.errorf("Invalid port in address: %s", address);
            }
        }
        return 9851;
    }

    // ======================== Cluster State Queries ========================

    /**
     * Blocks until a leader is elected for the Raft group, polling every 500ms.
     * This ensures the node is ready to accept commands before the application
     * starts serving requests.
     *
     * @param timeoutMs maximum time to wait in milliseconds
     */
    private void waitForLeader(long timeoutMs) {
        System.out.println("[RAFT_DEBUG] waitForLeader called, timeout=" + timeoutMs);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                var leaderId = raftServer.getDivision(groupId).getInfo().getLeaderId();
                System.out.println("[RAFT_DEBUG] Leader check: " + leaderId);
                if (leaderId != null) {
                    LOG.infof("Leader elected: %s", leaderId);
                    return;
                }
            } catch (Exception e) {
                System.out.println("[RAFT_DEBUG] waitForLeader exception: " + e.getMessage());
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        System.out.println("[RAFT_DEBUG] TIMED OUT waiting for leader");
        LOG.warn("Timed out waiting for Raft leader election");
    }

    /**
     * Returns this node's unique identifier.
     *
     * @return the node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Checks whether this node is currently the Raft leader.
     *
     * @return {@code true} if this node is the leader
     */
    public boolean isLeader() {
        if (raftServer == null) return false;
        try {
            return raftServer.getDivision(groupId).getInfo().isLeader();
        } catch (IOException e) {
            return false;
        }
    }
    
    /**
     * Returns the ID of the current Raft leader.
     *
     * @return the leader's peer ID, or {@code null} if unknown
     */
    public RaftPeerId getLeaderId() {
        if (raftServer == null) return null;
        try {
            return raftServer.getDivision(groupId).getInfo().getLeaderId();
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Returns the list of all known cluster peers.
     *
     * @return list of Raft peers
     */
    public List<RaftPeer> getPeers() {
        return new ArrayList<>(raftGroup.getPeers());
    }

    // ======================== Dynamic Membership ========================

    /**
     * Adds a new peer to the Raft cluster. Must be called on the leader.
     *
     * @param id      the new peer's identifier
     * @param address the new peer's Raft gRPC address
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> addPeer(String id, String address) {
        if (!isLeader()) {
            return Uni.createFrom().item(false);
        }

        RaftPeer newPeer = RaftPeer.newBuilder()
                .setId(id)
                .setAddress(address)
                .build();

        List<RaftPeer> currentPeers = new ArrayList<>(raftGroup.getPeers());
        if (currentPeers.stream().anyMatch(p -> p.getId().toString().equals(id))) {
            return Uni.createFrom().item(true); // Already joined
        }
        currentPeers.add(newPeer);

        return setConfiguration(currentPeers);
    }

    /**
     * Removes a peer from the Raft cluster. Must be called on the leader.
     *
     * @param id the peer identifier to remove
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> removePeer(String id) {
        if (!isLeader()) {
            return Uni.createFrom().item(false);
        }

        List<RaftPeer> currentPeers = new ArrayList<>(raftGroup.getPeers());
        List<RaftPeer> newPeers = currentPeers.stream()
                .filter(p -> !p.getId().toString().equals(id))
                .collect(Collectors.toList());

        if (currentPeers.size() == newPeers.size()) {
            return Uni.createFrom().item(true); // Already gone
        }

        return setConfiguration(newPeers);
    }

    /**
     * Applies a new cluster configuration (peer list) through the Raft protocol.
     * Rebuilds the shared client after a successful configuration change.
     *
     * @param newPeers the new list of cluster peers
     * @return a Uni emitting {@code true} on success
     */
    private Uni<Boolean> setConfiguration(List<RaftPeer> newPeers) {
        if (sharedClient == null) {
            LOG.error("Raft shared client is not initialized");
            return Uni.createFrom().item(false);
        }
        LOG.infof("Setting new Raft configuration: %s", newPeers);
        return Uni.createFrom().item(() -> {
            try {
                RaftClientReply reply = sharedClient.admin().setConfiguration(newPeers);
                LOG.infof("Raft setConfiguration reply: %s", reply);
                if (reply.isSuccess()) {
                    this.raftGroup = RaftGroup.valueOf(groupId, newPeers);
                    // Rebuild the shared client with the updated group
                    try {
                        this.sharedClient.close();
                    } catch (IOException e) {
                        LOG.error("Failed to close client", e);
                    }
                    this.sharedClient = buildClient(raftGroup);
                    return true;
                }
                return false;
            } catch (IOException e) {
                LOG.error("Failed to update cluster configuration", e);
                return false;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // ======================== Command Replication ========================

    /**
     * Replicates a PUBLISH command through the Raft log.
     * Payload and routing key are Base64-encoded to safely handle pipe characters.
     *
     * @param msg the message to replicate
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateMessage(io.dist.model.Message msg) {
        String encodedPayload = Base64.getEncoder().encodeToString(msg.payload.getBytes(StandardCharsets.UTF_8));
        String encodedRoutingKey = msg.routingKey == null ? "null" : Base64.getEncoder().encodeToString(msg.routingKey.getBytes(StandardCharsets.UTF_8));
        return sendCommand(String.format("PUBLISH|%s|%s|%s|%s|%s|%s",
                msg.id, encodedPayload, encodedRoutingKey, msg.exchange, msg.queueName, msg.timestamp.toString()));
    }

    /**
     * Replicates a POLL command through the Raft log.
     *
     * @param queueName the queue being polled
     * @param messageId the ID of the dequeued message
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicatePoll(String queueName, String messageId) {
        return sendCommand(String.format("POLL|%s|%s", queueName, messageId));
    }

    /**
     * Replicates an ACK command through the Raft log.
     *
     * @param messageId the ID of the message to acknowledge
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateAck(String messageId) {
        return sendCommand(String.format("ACK|%s", messageId));
    }

    /**
     * Replicates a NACK command through the Raft log.
     *
     * @param messageId the ID of the message to nack
     * @param requeue   whether to requeue the message
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateNack(String messageId, boolean requeue) {
        return sendCommand(String.format("NACK|%s|%b", messageId, requeue));
    }

    /**
     * Replicates a CREATE_EXCHANGE command through the Raft log.
     *
     * @param name    exchange name
     * @param type    exchange type (DIRECT or FANOUT)
     * @param durable durability flag
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateCreateExchange(String name, String type, boolean durable) {
        return sendCommand(String.format("CREATE_EXCHANGE|%s|%s|%b", name, type, durable));
    }

    /**
     * Replicates a DELETE_EXCHANGE command through the Raft log.
     *
     * @param name the exchange name to delete
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateDeleteExchange(String name) {
        return sendCommand(String.format("DELETE_EXCHANGE|%s", name));
    }

    /**
     * Replicates a CREATE_QUEUE command through the Raft log.
     *
     * @param name       queue name
     * @param group      logical group
     * @param durable    durability flag
     * @param autoDelete auto-delete flag
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateCreateQueue(String name, String group, boolean durable, boolean autoDelete) {
        return sendCommand(String.format("CREATE_QUEUE|%s|%s|%b|%b", name, group, durable, autoDelete));
    }

    /**
     * Replicates a DELETE_QUEUE command through the Raft log.
     *
     * @param name the queue name to delete
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateDeleteQueue(String name) {
        return sendCommand(String.format("DELETE_QUEUE|%s", name));
    }

    /**
     * Replicates a BIND command through the Raft log.
     * The routing key is Base64-encoded to safely handle special characters.
     *
     * @param exchangeName exchange name
     * @param queueName    queue name
     * @param routingKey   routing key (may be {@code null})
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateBind(String exchangeName, String queueName, String routingKey) {
        String encodedRoutingKey = routingKey == null ? "null" : Base64.getEncoder().encodeToString(routingKey.getBytes(StandardCharsets.UTF_8));
        return sendCommand(String.format("BIND|%s|%s|%s", exchangeName, queueName, encodedRoutingKey));
    }

    /**
     * Replicates an UNBIND command through the Raft log.
     * The routing key is Base64-encoded to safely handle special characters.
     *
     * @param exchangeName exchange name
     * @param queueName    queue name
     * @param routingKey   routing key (may be {@code null})
     * @return a Uni emitting {@code true} on success
     */
    public Uni<Boolean> replicateUnbind(String exchangeName, String queueName, String routingKey) {
        String encodedRoutingKey = routingKey == null ? "null" : Base64.getEncoder().encodeToString(routingKey.getBytes(StandardCharsets.UTF_8));
        return sendCommand(String.format("UNBIND|%s|%s|%s", exchangeName, queueName, encodedRoutingKey));
    }

    /**
     * Sends a command string through the Raft client to be replicated across
     * the cluster. The command is applied by the {@link SimpleStateMachine}
     * on every node once committed.
     *
     * <p>Uses the blocking {@code io()} API wrapped in the Mutiny worker pool
     * so Quarkus reactive threads are never blocked.</p>
     *
     * @param command the pipe-delimited command string
     * @return a Uni emitting {@code true} if the command was successfully
     *         replicated and applied, {@code false} otherwise
     */
    private Uni<Boolean> sendCommand(String command) {
        if (sharedClient == null) {
            LOG.error("Raft shared client is not initialized");
            return Uni.createFrom().item(false);
        }
        return Uni.createFrom().item(() -> {
            // Wait for leader election before sending the command.
            // In a single-node cluster this completes in a few seconds;
            // in multi-node clusters it depends on election timeout.
            waitForLeader(30_000);
            try {
                RaftClientReply reply = sharedClient.io().send(
                        org.apache.ratis.protocol.Message.valueOf(command));
                if (!reply.isSuccess()) {
                    LOG.errorf("Raft command failed: %s, reply: %s", command, reply);
                    return false;
                }
                String response = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (!response.equals("SUCCESS")) {
                    LOG.errorf("Raft state-machine rejected command: %s, response: %s", command, response);
                    return false;
                }
                return true;
            } catch (IOException e) {
                LOG.error("Failed to send Raft command: " + command, e);
                return false;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Builds a new Raft client for the given group with a bounded retry policy.
     * Retries up to 10 times with 1-second sleep between attempts (~10s total)
     * to prevent worker-pool saturation during leader elections.
     *
     * @param group the Raft group to connect to
     * @return a configured Raft client
     */
    private RaftClient buildClient(RaftGroup group) {
        return RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(group)
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        10, TimeDuration.valueOf(1, java.util.concurrent.TimeUnit.SECONDS)))
                .build();
    }
}
