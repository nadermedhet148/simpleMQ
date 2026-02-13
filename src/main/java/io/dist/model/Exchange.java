package io.dist.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

@Entity
@Table(name = "exchanges")
public class Exchange extends PanacheEntityBase {
    @Id
    public String name;
    public ExchangeType type;
    public boolean durable;

    public Exchange() {}

    public Exchange(String name, ExchangeType type, boolean durable) {
        this.name = name;
        this.type = type;
        this.durable = durable;
    }
}
