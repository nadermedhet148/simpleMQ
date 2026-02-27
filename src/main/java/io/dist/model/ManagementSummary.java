package io.dist.model;

import java.util.List;
import java.util.Map;

public class ManagementSummary {
    public String nodeId;
    public String leaderId;
    public boolean isLeader;
    public List<String> peers;
    public List<QueueInfo> queues;
    public List<Exchange> exchanges;
    public SystemMetrics metrics;

    public static class QueueInfo {
        public String name;
        public String group;
        public int messageCount;
        public boolean durable;

        public QueueInfo(String name, String group, int messageCount, boolean durable) {
            this.name = name;
            this.group = group;
            this.messageCount = messageCount;
            this.durable = durable;
        }
    }
}
