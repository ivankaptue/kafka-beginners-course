package com.klid.producer.twitter;

import com.github.javafaker.Faker;
import com.klid.producer.twitter.producer.TwitterProducer;
import com.klid.producer.twitter.twitter.TwitterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.klid.producer.twitter.twitter.TwitterClient.getClient;
import static com.klid.producer.twitter.twitter.TwitterClient.getMessageQueue;

/**
 * @author Ivan Kaptue
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        initProducerStream();

        logger.info("End");
    }

    private static void initProducerStream() {
        Faker faker = new Faker();

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.scheduleAtFixedRate(
                () -> {
                    var message = faker.lorem().paragraph(3);
                    logger.info("Message {}", message);
                    TwitterProducer.send(message);
                }, 0, 500, TimeUnit.MILLISECONDS);
    }

    private static void initTwitterClient() {
        TwitterClient.start();

        // on a different thread, or multiple different threads....
        while (!getClient().isDone()) {
            String msg = null;
            try {
                msg = getMessageQueue().poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                getClient().stop();
                e.printStackTrace();
            }
            logger.info("twitter message : {}", msg);
            // TwitterProducer.send(msg);
        }
    }
}
