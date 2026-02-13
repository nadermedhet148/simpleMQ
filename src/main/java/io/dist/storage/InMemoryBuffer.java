package io.dist.storage;

import io.dist.model.Message;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;

public class InMemoryBuffer {
    private final Queue<Message> messages = new ConcurrentLinkedQueue<>();

    public void enqueue(Message message) {
        messages.offer(message);
    }

    public Message dequeue() {
        return messages.poll();
    }

    public int size() {
        return messages.size();
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
