package com.petro;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
@RequiredArgsConstructor
public class Consumer {

    private final String bootstrapServer;
    private final String groupId;
    private final String topic;

    public void run() {
        log.info("Creating consumer thread");

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            await(latch);

            log.info("Application is shutdown");
        }));

        await(latch);
    }

    private void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }
    }

    private class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            Properties properties = consumerProperties(bootstrapServer, groupId);
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topic));
            this.latch = latch;
        }

        private Properties consumerProperties(String bootstrapServer, String groupId) {
            String deserializer = StringDeserializer.class.getName();

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            return properties;
        }

        @Override
        public void run() {
            try {
                do {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    records.forEach(record -> log.info("Key: {}, Value: {}\n" +
                            "Partition: {}, Offset: {}",
                            record.key(), record.value(), record.partition(), record.offset()));
                } while (true);
            } catch (WakeupException e) {
                log.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        String serverBroker = "localhost:9092";
        String groupId = "cool_group1";
        String topic = "users";

        new Consumer(serverBroker, groupId, topic).run();
    }
}
