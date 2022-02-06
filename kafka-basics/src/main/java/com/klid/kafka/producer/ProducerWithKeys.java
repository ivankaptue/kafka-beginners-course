package com.klid.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author Ivan Kaptue
 */
public class ProducerWithKeys {

    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create producer properties
        var configs = new Properties();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // create producer
        var producer = new KafkaProducer<String, String>(configs);

        for (int i = 0; i < 12; i++) {
            // create producer record
            String topic = "third_topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            var record = new ProducerRecord<>(topic, key, value);

            logger.info("Key : " + key);

            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time record is successfully sent or an exception is thrown
                    if (exception == null) {
                        var data = new StringBuilder();
                        data.append("Received new metadata.");
                        data.append("\n");
                        data.append("Topic : " + metadata.topic());
                        data.append("\n");
                        data.append("Partition : " + metadata.partition());
                        data.append("\n");
                        data.append("Offset : " + metadata.offset());
                        data.append("\n");
                        data.append("Timestamp : " + metadata.timestamp());
                        data.append("\n");

                        logger.info(data.toString());
                    } else {
                        logger.error("Error occur", exception);
                    }
                }
            }).get(); // block the .send() to make it synchronous;
        }

        producer.flush();
        producer.close();
    }

}
