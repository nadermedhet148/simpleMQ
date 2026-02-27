package io.dist.cluster;

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
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
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
        
        // Use temporary directory for Ratis internal files (metadata)
        // Prefer /data (if writable) or /dev/shm (in-memory) or system temp
        File storageDir;
        File dataDir = new File("/data");
        File shmDir = new File("/dev/shm");
        
        try {
            if (dataDir.exists() && dataDir.canWrite()) {
                storageDir = Files.createTempDirectory(dataDir.toPath(), "smq-raft-" + nodeId + "-").toFile();
            } else if (shmDir.exists() && shmDir.canWrite()) {
                storageDir = Files.createTempDirectory(shmDir.toPath(), "smq-raft-" + nodeId + "-").toFile();
            } else {
                storageDir = Files.createTempDirectory("smq-raft-" + nodeId + "-").toFile();
            }
        } catch (IOException e) {
            LOG.error("Failed to create temporary Raft storage directory", e);
            // Last resort fallback
            storageDir = new File("/tmp/smq-raft-" + nodeId + "-" + System.currentTimeMillis());
            storageDir.mkdirs();
        }

        this.currentStorageDir = storageDir;
        LOG.infof("Using Raft storage directory: %s", storageDir.getAbsolutePath());

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        // Restore metadata from SQLite if it exists
        restoreMetadataFromSQLite(storageDir, peerId);

        // Use RECOVER to pick up restored metadata, or it will behave like FORMAT if empty
        RaftStorage.StartupOption startupOption = RaftStorage.StartupOption.RECOVER;

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

        this.sharedClient = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(raftGroup)
                .build();
        
        LOG.infof("Raft shared client initialized for node %s", nodeId);
        
        // Start periodic metadata sync and leadership logging
        startSyncAndLogging();
    }

    private void restoreMetadataFromSQLite(File storageDir, RaftPeerId peerId) {
        try {
            QuarkusTransaction.requiringNew().run(() -> {
                RaftMetadata metadata = RaftMetadata.findById(nodeId);
                if (metadata != null) {
                    LOG.infof("Restoring Raft metadata from SQLite: term=%d, votedFor=%s", metadata.currentTerm, metadata.votedFor);
                    File groupDir = new File(storageDir, groupId.getUuid().toString());
                    File currentDir = new File(groupDir, "current");
                    if (!currentDir.exists()) {
                        currentDir.mkdirs();
                    }
                    File metaFile = new File(currentDir, "raft-meta");
                    try (PrintWriter writer = new PrintWriter(metaFile)) {
                        writer.println("term=" + metadata.currentTerm);
                        writer.println("votedFor=" + (metadata.votedFor != null ? metadata.votedFor : ""));
                    } catch (IOException e) {
                        LOG.error("Failed to restore raft-meta file", e);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("Failed to restore metadata from SQLite", e);
        }
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
        
        if (currentStorageDir != null && currentStorageDir.exists()) {
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

    public boolean addPeer(String id, String address) {
        if (!isLeader()) {
            return false;
        }

        RaftPeer newPeer = RaftPeer.newBuilder()
                .setId(id)
                .setAddress(address)
                .build();

        List<RaftPeer> currentPeers = new ArrayList<>(raftGroup.getPeers());
        if (currentPeers.stream().anyMatch(p -> p.getId().toString().equals(id))) {
            return true; // Already joined
        }
        currentPeers.add(newPeer);

        return setConfiguration(currentPeers);
    }

    public boolean removePeer(String id) {
        if (!isLeader()) {
            return false;
        }

        List<RaftPeer> currentPeers = new ArrayList<>(raftGroup.getPeers());
        List<RaftPeer> newPeers = currentPeers.stream()
                .filter(p -> !p.getId().toString().equals(id))
                .collect(Collectors.toList());

        if (currentPeers.size() == newPeers.size()) {
            return true; // Already gone
        }

        return setConfiguration(newPeers);
    }

    private boolean setConfiguration(List<RaftPeer> newPeers) {
        if (sharedClient == null) {
            LOG.error("Raft shared client is not initialized");
            return false;
        }
        LOG.infof("Setting new Raft configuration: %s", newPeers);
        try {
            RaftClientReply reply = sharedClient.admin().setConfiguration(newPeers);
            LOG.infof("Raft setConfiguration reply: %s", reply);
            if (reply.isSuccess()) {
                this.raftGroup = RaftGroup.valueOf(groupId, newPeers);
                // Update shared client with new group
                this.sharedClient.close();
                this.sharedClient = RaftClient.newBuilder()
                        .setProperties(new RaftProperties())
                        .setRaftGroup(raftGroup)
                        .build();
                return true;
            }
            return false;
        } catch (IOException e) {
            LOG.error("Failed to update cluster configuration", e);
            return false;
        }
    }

    public boolean replicateMessage(io.dist.model.Message msg) {
        if (sharedClient == null) {
            LOG.error("Raft shared client is not initialized");
            return false;
        }
        String command = String.format("PUBLISH|%s|%s|%s|%s|%s|%s",
                msg.id, msg.payload, msg.routingKey, msg.exchange, msg.queueName, msg.timestamp.toString());

        try {
            RaftClientReply reply = sharedClient.io().send(org.apache.ratis.protocol.Message.valueOf(command));
            return reply.isSuccess();
        } catch (IOException e) {
            LOG.error("Failed to replicate message", e);
            return false;
        }
    }
}
