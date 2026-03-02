package io.dist.storage;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central registry for all in-memory queue buffers.
 *
 * <p>Each named queue in simpleMQ is backed by its own {@link InMemoryBuffer}.
 * This service lazily creates buffers on first access and provides methods to
 * retrieve, delete, or iterate over all buffers.</p>
 *
 * <p>Thread-safe: the internal map is a {@link ConcurrentHashMap} and buffers
 * are created atomically via {@code computeIfAbsent}.</p>
 *
 * @see InMemoryBuffer
 * @see io.dist.service.MessagingEngine
 */
@ApplicationScoped
public class StorageService {

    /** Map of queue name → in-memory buffer. Created lazily on first access. */
    private final Map<String, InMemoryBuffer> buffers = new ConcurrentHashMap<>();

    /**
     * Returns the in-memory buffer for the given queue, creating one if it
     * does not already exist.
     *
     * @param queueName the queue name
     * @return the buffer associated with the queue
     */
    public InMemoryBuffer getBuffer(String queueName) {
        return buffers.computeIfAbsent(queueName, k -> new InMemoryBuffer());
    }

    /**
     * Removes and discards the buffer for the given queue.
     * Any messages still in the buffer are lost.
     *
     * @param queueName the queue name whose buffer should be deleted
     */
    public void deleteBuffer(String queueName) {
        buffers.remove(queueName);
    }

    /**
     * Returns a read-only view of all queue buffers.
     * Used by the visibility timeout scheduler to iterate over every queue.
     *
     * @return map of queue name → buffer
     */
    public Map<String, InMemoryBuffer> getAllBuffers() {
        return buffers;
    }

    /**
     * Removes all buffers. Typically used in tests to reset state between runs.
     */
    public void clear() {
        buffers.clear();
    }
}
