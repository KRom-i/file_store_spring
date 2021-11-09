package com.geekbrains.geekmarketwinter;

import com.geekbrains.geekmarketwinter.entites.ProductDTO;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toCollection;

@SpringBootApplication
public class GeekMarketWinterApplication {
	public static void main(String[] args) {
		SpringApplication.run(GeekMarketWinterApplication.class, args);
	}


}