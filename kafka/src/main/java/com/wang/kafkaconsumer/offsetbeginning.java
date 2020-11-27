package com.wang.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author 王继昌
 * @create 2020-09-27 20:55
 */
public class offsetbeginning {

    public static void main(String[] args) {
        Properties properties = new Properties();
        Properties props = new Properties();

        //kafka连接端口
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        //自定义组名
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"g3");
        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        props.put("auto.commit.interval.ms", "1000");
        //每次提交offset的时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

        //开启offset重置位置earliest
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add("atguigu");
        consumer.subscribe(arrayList);
        while (true) {

            //poll是设置惩罚时间
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records)

                System.out.println(record.topic()+" : "+record.offset()+"  -  "+record.partition());
        }
    }
}
