package com.geekbrains.geekmarketwinter.services;

import com.geekbrains.geekmarketwinter.entites.OrderStatus;
import com.geekbrains.geekmarketwinter.repositories.OrderRepository;
import com.geekbrains.geekmarketwinter.repositories.OrderStatusRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OrderStatusService {
    private OrderStatusRepository orderStatusRepository;

    @Autowired
    public void setOrderStatusRepository(OrderStatusRepository orderStatusRepository) {
        this.orderStatusRepository = orderStatusRepository;
    }

    public OrderStatus getStatusById(Long id) {
        return orderStatusRepository.findById(id).orElse(null);
    }
}
