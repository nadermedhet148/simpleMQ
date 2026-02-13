package io.dist.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

@Entity
@Table(name = "queues")
public class Queue extends PanacheEntityBase {
    @Id
    public String name;
    public String queueGroup; // changed from 'group' as it might be a reserved keyword
    public boolean durable;
    public boolean autoDelete;

    public Queue() {}

    public Queue(String name, String queueGroup, boolean durable, boolean autoDelete) {
        this.name = name;
        this.queueGroup = queueGroup;
        this.durable = durable;
        this.autoDelete = autoDelete;
    }
}
