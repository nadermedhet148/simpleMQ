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
                "quarkus.datasource.jdbc.url", "jdbc:sqlite::memory:"
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
