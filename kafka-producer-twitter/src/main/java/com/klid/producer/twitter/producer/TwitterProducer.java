package com.klid.producer.twitter.producer;

import com.klid.producer.twitter.config.KafkaConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * @author Ivan Kaptue
 */
public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static Producer<String, String> producer = null;

    private TwitterProducer() {
    }

    private static synchronized Producer<String, String> getProducer() {
        if (producer == null) {
            var configs = new Properties();
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKERS);
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            // safe producer
            configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            configs.put(ProducerConfig.ACKS_CONFIG, "all");
            configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

            logger.info("Init producer with params {}", configs);

            producer = new KafkaProducer<>(configs);
        }
        return producer;
    }

    public static void send(String message) {
        var uuid = UUID.randomUUID().toString();
        var payload = "{\"uuid\": \"" + uuid + "\", \"message\": \"" + message + "\"}";

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KafkaConfig.TOPIC, payload);
        getProducer().send(producerRecord, TwitterProducer::producerCallback);
        getProducer().flush();
    }

    private static void producerCallback(RecordMetadata metadata, Exception e) {
        if (e != null) {
            logger.error("Error", e);
            return;
        }

        logger.info("Partition : {}", metadata.partition());
        logger.info("Timestamp : {}", new Date(metadata.timestamp()));
    }

}
