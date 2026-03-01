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

@ApplicationScoped
public class RaftService {
    private static final Logger LOG = Logger.getLogger(RaftService.class);

    static {
        // Disable DNS negative caching to help recover from transient name resolution failures in Docker
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
    }

    @ConfigProperty(name = "simplemq.cluster.node-id")
    String nodeId;

    @ConfigProperty(name = "simplemq.cluster.address")
    String address;

    @ConfigProperty(name = "simplemq.cluster.peers")
    String peers;

    @ConfigProperty(name = "simplemq.persistence.enabled", defaultValue = "true")
    boolean persistenceEnabled;

    private RaftServer raftServer;
    private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes("simpleMQ-cluster".getBytes()));
    private RaftGroup raftGroup;
    private RaftClient sharedClient;
    private File currentStorageDir;

    void onStart(@Observes StartupEvent ev) throws IOException {
        LOG.infof("Starting Raft Node: %s at %s", nodeId, address);

        RaftPeerId peerId = RaftPeerId.valueOf(nodeId);
        
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
        
        // Use persistent directory for Ratis internal files (metadata)
        File storageDir;
        File dataDir = new File("/data");
        
        if (dataDir.exists() && dataDir.canWrite()) {
            storageDir = new File(dataDir, "raft-" + nodeId);
        } else {
            // Use a stable temporary directory for the node.
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
        raftServer.start();
        LOG.infof("Raft server %s started successfully", nodeId);

        this.sharedClient = buildClient(raftGroup);
        LOG.infof("Raft shared client initialized for node %s", nodeId);
        
        // Start periodic metadata sync and leadership logging
        startSyncAndLogging();
    }

    private void startSyncAndLogging() {
        new Timer(true).scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    RaftServer.Division division = raftServer.getDivision(groupId);
                    
                    // Metadata Sync
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

                    // Leadership Logging
                    if (division.getInfo().isLeader()) {
                        LOG.info(">>> I am the LEADER (" + nodeId + ")");
                    } else if (division.getInfo().isFollower()) {
                        LOG.info("I am a follower (" + nodeId + "). Leader is: " + division.getInfo().getLeaderId());
                    } else if (division.getInfo().isCandidate()) {
                        LOG.info("I am a candidate (" + nodeId + ")");
                    }
                } catch (Exception e) {
                    // Ignore if division not ready or other errors
                }
            }
        }, 5000, 10000);
    }


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

    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    private void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directoryToBeDeleted.delete();
    }

    private int extractPort(String address) {
        int lastColon = address.lastIndexOf(':');
        if (lastColon >= 0) {
            try {
                return Integer.parseInt(address.substring(lastColon + 1));
            } catch (NumberFormatException e) {
                LOG.errorf("Invalid port in address: %s", address);
            }
        }
        return 9851; // Default port
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isLeader() {
        try {
            return raftServer.getDivision(groupId).getInfo().isLeader();
        } catch (IOException e) {
            return false;
        }
    }
    
    public RaftPeerId getLeaderId() {
        try {
            return raftServer.getDivision(groupId).getInfo().getLeaderId();
        } catch (IOException e) {
            return null;
        }
    }

    public List<RaftPeer> getPeers() {
        return new ArrayList<>(raftGroup.getPeers());
    }

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
                    // Update shared client with new group
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

    public Uni<Boolean> replicateMessage(io.dist.model.Message msg) {
        String encodedPayload = Base64.getEncoder().encodeToString(msg.payload.getBytes(StandardCharsets.UTF_8));
        String encodedRoutingKey = msg.routingKey == null ? "null" : Base64.getEncoder().encodeToString(msg.routingKey.getBytes(StandardCharsets.UTF_8));
        return sendCommand(String.format("PUBLISH|%s|%s|%s|%s|%s|%s",
                msg.id, encodedPayload, encodedRoutingKey, msg.exchange, msg.queueName, msg.timestamp.toString()));
    }

    public Uni<Boolean> replicatePoll(String queueName, String messageId) {
        return sendCommand(String.format("POLL|%s|%s", queueName, messageId));
    }

    public Uni<Boolean> replicateAck(String messageId) {
        return sendCommand(String.format("ACK|%s", messageId));
    }

    public Uni<Boolean> replicateNack(String messageId, boolean requeue) {
        return sendCommand(String.format("NACK|%s|%b", messageId, requeue));
    }

    public Uni<Boolean> replicateCreateExchange(String name, String type, boolean durable) {
        return sendCommand(String.format("CREATE_EXCHANGE|%s|%s|%b", name, type, durable));
    }

    public Uni<Boolean> replicateDeleteExchange(String name) {
        return sendCommand(String.format("DELETE_EXCHANGE|%s", name));
    }

    public Uni<Boolean> replicateCreateQueue(String name, String group, boolean durable, boolean autoDelete) {
        return sendCommand(String.format("CREATE_QUEUE|%s|%s|%b|%b", name, group, durable, autoDelete));
    }

    public Uni<Boolean> replicateDeleteQueue(String name) {
        return sendCommand(String.format("DELETE_QUEUE|%s", name));
    }

    public Uni<Boolean> replicateBind(String exchangeName, String queueName, String routingKey) {
        String encodedRoutingKey = routingKey == null ? "null" : Base64.getEncoder().encodeToString(routingKey.getBytes(StandardCharsets.UTF_8));
        return sendCommand(String.format("BIND|%s|%s|%s", exchangeName, queueName, encodedRoutingKey));
    }

    public Uni<Boolean> replicateUnbind(String exchangeName, String queueName, String routingKey) {
        String encodedRoutingKey = routingKey == null ? "null" : Base64.getEncoder().encodeToString(routingKey.getBytes(StandardCharsets.UTF_8));
        return sendCommand(String.format("UNBIND|%s|%s|%s", exchangeName, queueName, encodedRoutingKey));
    }

    private Uni<Boolean> sendCommand(String command) {
        if (sharedClient == null) {
            LOG.error("Raft shared client is not initialized");
            return Uni.createFrom().item(false);
        }
        // Use the blocking io() API (which handles leader-watch / retry internally)
        // wrapped in the worker pool so Quarkus reactive threads are never blocked.
        return Uni.createFrom().item(() -> {
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

    private RaftClient buildClient(RaftGroup group) {
        // Retry up to 10 times with 1s sleep → gives up after ~10s instead of retrying forever.
        // This prevents worker-pool saturation when a command gets stuck after a leader election.
        return RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(group)
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        10, TimeDuration.valueOf(1, java.util.concurrent.TimeUnit.SECONDS)))
                .build();
    }
}
