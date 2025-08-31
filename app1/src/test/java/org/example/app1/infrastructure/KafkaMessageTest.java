package org.example.app1.infrastructure;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class KafkaMessageTest {

    @Test
    void kafkaMessageTest() {
        DockerImageName image = DockerImageName.parse("apache/kafka:3.9.1");

        try (KafkaContainer kafka = new KafkaContainer(image)) {
            kafka.start();
            String bootstrap = kafka.getBootstrapServers();
            String topic = "test-" + UUID.randomUUID();

            // Producer
            Properties producerProps = new Properties();
            producerProps.put("bootstrap.servers", bootstrap);
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                producer.send(new ProducerRecord<>(topic, "key1", "hello"));
                producer.flush();
            }

            // Consumer
            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", bootstrap);
            consumerProps.put("group.id", "test-group");
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("auto.offset.reset", "earliest");

            try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(topic));

                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    assertThat(records).isNotEmpty();
                    assertThat(records.iterator().next().value()).isEqualTo("hello");
                });
            }
        }
    }
}
