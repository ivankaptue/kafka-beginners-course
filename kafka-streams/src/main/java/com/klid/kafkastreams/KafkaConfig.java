package com.klid.kafkastreams;

/**
 * @author Ivan Kaptue
 */
public interface KafkaConfig {
    public static final String BROKERS = "127.0.0.1:9092";
    String INPUT_TOPIC = "com.klid.twitter-elastic-search";
    String RESULT_TOPIC = "com.klid.twitter_important_tweets";
    String GROUP_ID = "com.klid.twitter-elastic-search.group";
    String APP_ID = "demo-kafka-streams";
}
