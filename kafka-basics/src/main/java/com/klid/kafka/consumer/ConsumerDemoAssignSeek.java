package com.klid.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author Ivan Kaptue
 */
public class ConsumerDemoAssignSeek {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        String topic = "third_topic";

        // create consumer properties
        var configs = new Properties();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        var consumer = new KafkaConsumer<String, String>(configs);

        // assign and seek are mostly used to replay data or fetch a specific message
        // assign
        var topicToReadFrom = new TopicPartition(topic, 0);
        var offsetToReadFrom = 5L;
        consumer.assign(Collections.singleton(topicToReadFrom));
        //seek
        consumer.seek(topicToReadFrom, offsetToReadFrom);

        // poll new data
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;
        while (keepOnReading) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (var r : records) {
                var key = r.key();
                var value = r.value();
                numberOfMessagesReadSoFar++;
                logger.info("key : {}, Value : {}", key, value);
                logger.info("Partition : {}, Offset : {}", r.partition(), r.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false; // exit while loop
                    break;
                }
            }
        }

        consumer.close();
        logger.info("Exit the application");
    }
}
