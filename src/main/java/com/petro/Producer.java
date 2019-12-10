package com.petro;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class Producer {
    private final KafkaProducer<String, String> kafkaProducer;

    Producer(String bootstrapServer) {
        Properties properties = producerProperties(bootstrapServer);

        kafkaProducer = new KafkaProducer<>(properties);

        log.info("Producer initialized");
    }

    /**
     * @param bootstrapServer an IP of our Kafka broker
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

}
