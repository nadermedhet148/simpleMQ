package io.dist.storage;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class StorageService {
    private final Map<String, InMemoryBuffer> buffers = new ConcurrentHashMap<>();

    public InMemoryBuffer getBuffer(String queueName) {
        return buffers.computeIfAbsent(queueName, k -> new InMemoryBuffer());
    }

    public void deleteBuffer(String queueName) {
        buffers.remove(queueName);
    }
}
