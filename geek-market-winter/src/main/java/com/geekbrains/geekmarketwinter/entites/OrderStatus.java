package com.geekbrains.geekmarketwinter.entites;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Data
@NoArgsConstructor
@Table(name = "orders_statuses")
public class OrderStatus {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "title")
    private String title;

    @Override
    public String toString() {
        return "Role{" + "id=" + id + ", title='" + title + '\'' + '}';
    }
}
