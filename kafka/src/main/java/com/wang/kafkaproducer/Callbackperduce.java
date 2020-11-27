package com.wang.kafkaproducer;

import com.sun.org.apache.bcel.internal.generic.IFNE;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author 王继昌
 * @create 2020-09-27 20:22
 */
public class Callbackperduce {
    public static void main(String[] args) {

        Properties properties = new Properties();


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
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,mypartition.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            if (i <30) {
            producer.send(new ProducerRecord<String, String>("atguigu", "wang","wangjiyou--->" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println(e.getMessage());
                    }else{
                        System.out.println(recordMetadata.topic()+" : "+recordMetadata.offset()+"  -  "+recordMetadata.partition());
                    }
                }
            });

            }else{
                producer.send(new ProducerRecord<String, String>("atguigu", "ji","wangjiyou--->" + i), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                System.out.println(e.getMessage());
                            }else{
                                System.out.println(recordMetadata.topic()+" : "+recordMetadata.offset()+"  -  "+recordMetadata.partition());
                            }
                        }
                    });

            }
        }

        producer.close();
    }
}
