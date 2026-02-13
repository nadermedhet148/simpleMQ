package io.dist.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Column;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "messages")
public class Message extends PanacheEntityBase {
    @Id
    public String id;
    public String payload;
    public String routingKey;
    public String exchange;
    public LocalDateTime timestamp;
    public int deliveryCount;

    @ElementCollection
    @CollectionTable(name = "message_headers", joinColumns = @JoinColumn(name = "message_id"))
    @MapKeyColumn(name = "header_key")
    @Column(name = "header_value")
    public Map<String, String> headers = new HashMap<>();

    public Message() {}

    public Message(String payload, String routingKey, String exchange) {
        this.id = UUID.randomUUID().toString();
        this.payload = payload;
        this.routingKey = routingKey;
        this.exchange = exchange;
        this.timestamp = LocalDateTime.now();
        this.deliveryCount = 0;
    }
}
