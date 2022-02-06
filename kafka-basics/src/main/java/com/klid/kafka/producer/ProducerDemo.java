package com.klid.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Ivan Kaptue
 */
public class ProducerDemo {

    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        // create producer properties
        var configs = new Properties();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // create producer
        var producer = new KafkaProducer<String, String>(configs);

        // create producer record
        var record = new ProducerRecord<String, String>("third_topic", "Hello Ivan !!!");

        // send data
        producer.send(record);
        producer.flush();
        producer.close();
    }

}
