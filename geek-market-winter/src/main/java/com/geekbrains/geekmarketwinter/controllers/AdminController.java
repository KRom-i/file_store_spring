package com.geekbrains.geekmarketwinter.controllers;

import com.geekbrains.geekmarketwinter.entites.Order;
import com.geekbrains.geekmarketwinter.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

@Controller
@RequestMapping("/admin")
public class AdminController {
    private OrderService orderService;

    @Autowired
    public void setOrderService(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping
    public String showAdminDashboard() {
        return "admin-panel";
    }

    @GetMapping("/orders")
    public String showOrders(Model model) {
        List<Order> orders = orderService.getAllOrders();
        model.addAttribute("orders", orders);
        return "orders-page";
    }

    @GetMapping("/orders/ready/{id}")
    public void orderReady(HttpServletRequest request, HttpServletResponse response, @PathVariable("id") Long id) throws Exception {
        Order order = orderService.findById(id);
        orderService.changeOrderStatus(order, 2L);
        response.sendRedirect(request.getHeader("referer"));
    }
}
