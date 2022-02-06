package com.klid.consumer.elasticsearch;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * @author Ivan Kaptue
 */
public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    public static final String TWITTER_INDEX = "twitter";
    public static final String TWITTER_TYPE = "tweets";

    private static Consumer<String, String> consumer;
    private static RestHighLevelClient elasticSearchClient;

    private ElasticSearchConsumer() {
    }

    private static Consumer<String, String> getConsumer() {
        if (consumer == null) {
            var configs = new Properties();
            configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKERS);
            configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.GROUP_ID);
            configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // disable auto commit of offset
            configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 15);

            consumer = new KafkaConsumer<>(configs);
        }
        return consumer;
    }

    private static synchronized RestHighLevelClient getElasticSearchClient() {
        if (elasticSearchClient == null) {
            var hostname = "kafka-course-7994283481.us-east-1.bonsaisearch.net";
            var username = "fydzsdgpon";
            var password = "c9gitkbu0s";

            var credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

            var builder =
                    RestClient.builder(new HttpHost(hostname, 443, "https"))
                            .setHttpClientConfigCallback(httpClientBuilder ->
                                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

            elasticSearchClient = new RestHighLevelClient(builder);
        }
        return elasticSearchClient;
    }

    private static void insertIndex(String id, String data) throws IOException {
        var indexRequest =
                new IndexRequest(TWITTER_INDEX, TWITTER_TYPE, id) // pass id to make consumer idempotent
                        .source(data, XContentType.JSON);

        var indexResponse = getElasticSearchClient()
                .index(indexRequest, RequestOptions.DEFAULT);

        logger.info("index response id : " + indexResponse.getId());
    }

    private static IndexRequest buildIndexRequest(String id, String data) {
        return new IndexRequest(TWITTER_INDEX, TWITTER_TYPE, id) // pass id to make consumer idempotent
                .source(data, XContentType.JSON);
    }

    private static Optional<String> getIdFromPayload(String data) {
        logger.info("json data : {}", data);

        String id = null;
        try {
            id = JsonParser.parseString(data)
                    .getAsJsonObject()
                    .get("uuid")
                    .getAsString();
        } catch (JsonSyntaxException ignored) {
        }
        return Optional.ofNullable(id);
    }

    public static void start() throws IOException {
        var consumer = getConsumer();
        consumer.subscribe(List.of(KafkaConfig.TOPIC));

        Optional<String> id;
        String data;
        BulkRequest bulk;
        IndexRequest indexRequest;
        while (true) {
            var consumerRecords = consumer.poll(Duration.ofMillis(500));

            int count = consumerRecords.count();
            logger.info("Received : " + count);

            if (count > 0) {
                bulk = new BulkRequest();
                for (var consumerRecord : consumerRecords) {
                    data = consumerRecord.value();

                    // to strategies to generate id
                    // 1 : kafka generated id
                    // id = consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset();

                    // twitter specific id
                    id = getIdFromPayload(data);

                    if (id.isPresent()) {
                        // insertIndex(id.get(), data);
                        indexRequest = buildIndexRequest(id.get(), data);
                        bulk.add(indexRequest);
                    }
                }

                if (bulk.hasIndexRequestsWithPipelines()) {
                    getElasticSearchClient().bulk(bulk, RequestOptions.DEFAULT);
                }

                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                sleep();
            }
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
