package com.petro;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Producer {
    private final KafkaProducer<String, String> kafkaProducer;

    Producer(String bootstrapServer) {
        Properties properties = producerProperties(bootstrapServer);

        kafkaProducer = new KafkaProducer<>(properties);

        log.info("Producer initialized");
    }

    /**
     * @param bootstrapServer an address of our Kafka broker
     * @return properties for our producer
     */
    private Properties producerProperties(String bootstrapServer) {
        String serializer = StringSerializer.class.getName();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);

        return properties;
    }

    public void put(String topic, String key, String value) throws ExecutionException, InterruptedException {

        log.info("Put value {} for key {}", value, key);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        kafkaProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.error("Error while producing", e);
            }

            log.info(String.format("Received ne metadata.\n" +
                    "Topic: %s\n" +
                    "Partition: %d\n" +
                    "Offset: %d\n" +
                    "Timestamp: %d",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()
            ));

        }).get();
    }

    public void close() {
        log.info("Closing producer`s connection");
        kafkaProducer.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String serverBroker = "localhost:9092";
        String topic = "users";

        Producer producer = new Producer(serverBroker);
        producer.put(topic, "user1", "Petro");
        producer.put(topic, "user2", "Yarik");
        producer.put(topic, "user3", "Vadim");
        producer.close();
    }

}
