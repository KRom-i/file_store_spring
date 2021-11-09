package com.geekbrains.geekmarketwinter.services;

import com.geekbrains.geekmarketwinter.entites.Order;
import com.geekbrains.geekmarketwinter.entites.OrderItem;
import com.geekbrains.geekmarketwinter.entites.OrderStatus;
import com.geekbrains.geekmarketwinter.entites.User;
import com.geekbrains.geekmarketwinter.repositories.OrderRepository;
import com.geekbrains.geekmarketwinter.utils.ShoppingCart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;

@Service
public class OrderService {
    private OrderRepository orderRepository;

    private OrderStatusService orderStatusService;

    @Autowired
    public void setOrderRepository(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Autowired
    public void setOrderStatusService(OrderStatusService orderStatusService) {
        this.orderStatusService = orderStatusService;
    }

    @Transactional
    public Order makeOrder(ShoppingCart cart, User user) {
        Order order = new Order();
        order.setId(0L);
        order.setUser(user);
        order.setStatus(orderStatusService.getStatusById(1L));
        order.setPrice(cart.getTotalCost());
        order.setOrderItems(new ArrayList<>(cart.getItems()));
        for (OrderItem o : cart.getItems()) {
            o.setOrder(order);
        }
        return order;
    }

    public List<Order> getAllOrders() {
        return (List<Order>) orderRepository.findAll();
    }

    public Order findById(Long id) {
        return orderRepository.findById(id).get();
    }

    public Order saveOrder(Order order) {
        Order orderOut = orderRepository.save(order);
        orderOut.setConfirmed(true);
        return orderOut;
    }

    public Order changeOrderStatus(Order order, Long statusId) {
        order.setStatus(orderStatusService.getStatusById(statusId));
        return saveOrder(order);
    }
}
