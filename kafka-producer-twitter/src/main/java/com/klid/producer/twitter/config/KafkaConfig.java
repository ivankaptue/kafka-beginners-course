package com.klid.producer.twitter.config;

/**
 * @author Ivan Kaptue
 */
public interface KafkaConfig {
    public static final String BROKERS = "127.0.0.1:9092";
    String TOPIC = "com.klid.twitter-elastic-search";
    String GROUP_ID = "com.klid.twitter-elastic-search.group";
}
