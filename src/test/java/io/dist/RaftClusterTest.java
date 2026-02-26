package io.dist;

import io.dist.cluster.RaftService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class RaftClusterTest {

    @Inject
    RaftService raftService;

    @Test
    void testSingleNodeLeaderElection() {
        // In a single-node setup (default for tests), the node should become the leader fairly quickly.
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .until(() -> raftService.isLeader());
        
        assertTrue(raftService.isLeader(), "The single node should be the LEADER");
        org.apache.ratis.protocol.RaftPeerId leaderId = raftService.getLeaderId();
        org.junit.jupiter.api.Assertions.assertNotNull(leaderId);
        org.junit.jupiter.api.Assertions.assertEquals("node1", leaderId.toString());
    }
}
