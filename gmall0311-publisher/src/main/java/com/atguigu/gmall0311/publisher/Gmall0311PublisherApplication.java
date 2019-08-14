package com.atguigu.gmall0311.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall0311.publisher.mapper")
public class Gmall0311PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0311PublisherApplication.class, args);
    }

}
