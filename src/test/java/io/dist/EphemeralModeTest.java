package io.dist;

import io.dist.cluster.RaftService;
import io.dist.service.MessagingEngine;
import io.dist.storage.PersistenceManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;

@QuarkusTest
@TestProfile(EphemeralModeTest.EphemeralProfile.class)
public class EphemeralModeTest {

    public static class EphemeralProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "simplemq.persistence.enabled", "false",
                "quarkus.datasource.jdbc.url", "jdbc:sqlite::memory:",
                // Use distinct ports so this profile doesn't conflict with:
                // - default test profile (Raft 19851, HTTP 18080)
                // - Docker cluster (Raft 9851-9853, HTTP 8081-8083, NGINX 8080)
                "quarkus.http.port", "18081",
                "quarkus.http.test-port", "18081",
                "simplemq.cluster.address", "localhost:19852",
                "simplemq.cluster.peers", "node1=localhost:19852"
            );
        }
    }

    @Inject
    RaftService raftService;

    @Inject
    PersistenceManager persistenceManager;

    @Test
    void testEphemeralModeConfiguration() {
        assertFalse(raftService.isPersistenceEnabled(), "Raft persistence should be disabled");
        assertFalse(persistenceManager.isPersistenceEnabled(), "Database persistence flag should be false");
    }
}
