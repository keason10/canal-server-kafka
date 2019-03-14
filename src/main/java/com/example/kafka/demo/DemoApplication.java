package com.example.kafka.demo;

import com.example.kafka.demo.util.kafka.AbstractKafkaTest;
import com.example.kafka.demo.util.kafka.CanalKafkaClientExample;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoApplication extends SpringBootServletInitializer {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public CanalKafkaClientExample canalKafkaClientExample() {
        return new CanalKafkaClientExample(
                AbstractKafkaTest.zkServers,
                AbstractKafkaTest.servers,
                AbstractKafkaTest.topic,
                AbstractKafkaTest.partition,
                AbstractKafkaTest.groupId);
    }

}
