package com.klid.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * @author Ivan Kaptue
 */
public class StreamsFilterTweets {
    public static void main(String[] args) {
        // create properties
        var properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.APP_ID);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // create topology
        var streamBuilder = new StreamsBuilder();
        streamBuilder.<String, String>stream(KafkaConfig.INPUT_TOPIC)
                .filter((k, v) -> v.length() > 100)
                .to(KafkaConfig.RESULT_TOPIC);

        // build topology
        var kafkaStreams = new KafkaStreams(streamBuilder.build(), properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        // start application
        try {
            kafkaStreams.start();
        } catch (RuntimeException ex) {
            System.exit(0);
        }
    }
}
