package com.pandora.www.kafka_interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * flink的kafka配置，可以配置这个拦截器，可以进行实时分拣数据
 */
public class MyKafkaInterceptor implements ProducerInterceptor<byte[], byte[]> {

    @Override
    public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> producerRecord) {
        String topic = producerRecord.topic();
        Integer partition = producerRecord.partition();
        Long timestamp = producerRecord.timestamp();
        byte[] key = producerRecord.key();
        byte[] value = producerRecord.value();

        /**
         * 这个拦截器这里的逻辑
         * 将value为null的数据认为是脏数据，重新给一个topic名，写到一个单独的topic
         * value不为null的数据保持写到原本要写入的topic
         */
        if (value == null) {
            topic = "dirty-data";
            return new ProducerRecord<byte[], byte[]>(topic, partition, timestamp, key, value);
        } else {
            return producerRecord;
        }

    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
