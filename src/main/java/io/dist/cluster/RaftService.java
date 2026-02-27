package io.dist.cluster;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class RaftService {
    private static final Logger LOG = Logger.getLogger(RaftService.class);

    @ConfigProperty(name = "simplemq.cluster.node-id")
    String nodeId;

    @ConfigProperty(name = "simplemq.cluster.address")
    String address;

    @ConfigProperty(name = "simplemq.cluster.peers")
    String peers;

    private RaftServer raftServer;
    private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes("simpleMQ-cluster".getBytes()));
    private RaftGroup raftGroup;

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
        
        final int port = NetUtils.createSocketAddr(address).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);
        
        File storageDir = new File("raft-data-" + System.currentTimeMillis() + "/" + nodeId);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        raftServer = RaftServer.newBuilder()
                .setServerId(peerId)
                .setGroup(raftGroup)
                .setProperties(properties)
                .setStateMachine(new SimpleStateMachine())
                .build();

        raftServer.start();
        
        LOG.infof("Raft Node %s started", nodeId);
        
        // Timer to check leadership status periodically for logging
        new Timer(true).scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    RaftServer.Division division = raftServer.getDivision(groupId);
                    if (division.getInfo().isLeader()) {
                        LOG.info(">>> I am the LEADER (" + nodeId + ")");
                    } else if (division.getInfo().isFollower()) {
                        LOG.info("I am a follower (" + nodeId + "). Leader is: " + division.getInfo().getLeaderId());
                    } else if (division.getInfo().isCandidate()) {
                        LOG.info("I am a candidate (" + nodeId + ")");
                    }
                } catch (IOException e) {
                    LOG.error("Error checking Raft status", e);
                }
            }
        }, 5000, 10000);
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
        LOG.infof("Setting new Raft configuration: %s", newPeers);
        try (RaftClient client = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(raftGroup)
                .build()) {

            RaftClientReply reply = client.admin().setConfiguration(newPeers);
            LOG.infof("Raft setConfiguration reply: %s", reply);
            if (reply.isSuccess()) {
                this.raftGroup = RaftGroup.valueOf(groupId, newPeers);
                return true;
            }
            return false;
        } catch (IOException e) {
            LOG.error("Failed to update cluster configuration", e);
            return false;
        }
    }

    public boolean replicateMessage(io.dist.model.Message msg) {
        String command = String.format("PUBLISH|%s|%s|%s|%s|%s|%s",
                msg.id, msg.payload, msg.routingKey, msg.exchange, msg.queueName, msg.timestamp.toString());

        try (RaftClient client = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(raftGroup)
                .build()) {
            
            RaftClientReply reply = client.io().send(org.apache.ratis.protocol.Message.valueOf(command));
            return reply.isSuccess();
        } catch (IOException e) {
            LOG.error("Failed to replicate message", e);
            return false;
        }
    }
}
