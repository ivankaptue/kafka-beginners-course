package com.klid.producer.twitter.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Ivan Kaptue
 */
public class TwitterClient {
    public static final String CONSUMER_KEY = "";
    public static final String CONSUMER_SECRET = "";
    public static final String ACCESS_TOKEN = "";
    public static final String ACCESS_TOKEN_SECRET = "";

    private TwitterClient() {
    }

    private static final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
    //    private static final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);
    private static Client client;

    public static void start() {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        client = new ClientBuilder()
                .name("Kafka-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(auth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
//                .eventMessageQueue(eventQueue)
                .build();

        client.connect();
    }

    public static BlockingQueue<String> getMessageQueue() {
        return msgQueue;
    }

//    public static BlockingQueue<Event> getEventQueue() {
//        return eventQueue;
//    }

    public static Client getClient() {
        if (client == null) {
            start();
        }
        return client;
    }
}
