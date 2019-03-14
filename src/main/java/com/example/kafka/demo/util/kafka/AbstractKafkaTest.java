package com.example.kafka.demo.util.kafka;



/**
 * Kafka 测试基类
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public abstract class AbstractKafkaTest {

    public static String  topic     = "example_v112";
    public static Integer partition = null;
    public static String  groupId   = "g4";
    public static String  servers   = "192.168.64.128:9092";
    public static String  zkServers = "192.168.64.128:2181";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }
}
