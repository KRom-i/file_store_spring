package com.geekbrains.geekmarketwinter.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sql2o.Connection;
import org.sql2o.Sql2o;
import com.geekbrains.geekmarketwinter.entites.User;

//@Component
public class UserRepositorySlqO2 {
//        private final Sql2o sql2o;
//
//    private static final String SELECT_USER_QUERY = "select id, username, password, first_name, last_name, email from users" +
//        " where username = :u_name";
//
//    public UserRepositorySlqO2(@Autowired Sql2o sql2o) {
//        this.sql2o = sql2o;
//    }
//
//    public User getUser(String userName) {
//        try (Connection connection = sql2o.open()) {
//            return connection.createQuery(SELECT_USER_QUERY, false)
//                    .addParameter("u_name", userName)
//                    .setColumnMappings(User.COLUMN_MAPPINGS)
//                    .executeAndFetchFirst(User.class);
//        }
//    }
}