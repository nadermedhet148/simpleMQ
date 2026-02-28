package io.dist.storage;

import io.dist.model.Message;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryBuffer {
    private final Queue<Message> messages = new ConcurrentLinkedQueue<>();
    private final Set<String> messageIds = ConcurrentHashMap.newKeySet();

    public void enqueue(Message message) {
        if (message.id != null) {
            if (messageIds.add(message.id)) {
                messages.offer(message);
            }
        } else {
            messages.offer(message);
        }
    }

    public Message dequeue() {
        Message msg = messages.poll();
        if (msg != null && msg.id != null) {
            messageIds.remove(msg.id);
        }
        return msg;
    }

    public void dequeueById(String id) {
        if (id == null) return;
        messages.removeIf(m -> m.id != null && m.id.equals(id));
        messageIds.remove(id);
    }

    public int size() {
        return messages.size();
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
