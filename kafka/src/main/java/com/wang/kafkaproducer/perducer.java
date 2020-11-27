package com.wang.kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author 王继昌
 * @create 2020-09-27 18:27
 */
public class perducer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        Properties props = new Properties();

        //主机名：9092
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //ack -1 all
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,1);
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间

        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);


        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");




        //重试次数

        //批次大小

        //等待时间

        //RecordAccumulator缓冲区大小


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String,String>("atguigu","wangjiyou--->"+i));
        }

        producer.close();
    }
}
