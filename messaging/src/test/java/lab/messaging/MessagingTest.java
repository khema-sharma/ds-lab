package lab.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

public class MessagingTest {

    private static KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka-native:3.8.0");

    @BeforeAll
    public static void beforeAll() {
        kafkaContainer.setPortBindings(Collections.singletonList("9092:9092"));
        kafkaContainer.start();
    }

    @Test
    public void acceptsRecordsFromSamePartition() throws InterruptedException, ExecutionException {
        String topic = "ds-lab";
        String key = "default-key";

        try (AdminClient adminClient = newAdminClient()) {
            adminClient.createTopics(Collections.singleton(newTopic(topic, 3)));

            try (Producer<String, String> kafkaProducer = newKafkaProducer()) {
                // Producers wait until server acknowledges that atleast one
                // node in the kafka cluster has received the messages.
                //
                // The get() call at the end blocks until server confirms.
                //
                // See "acks" producer property in newKafkaProducer method.

                kafkaProducer
                        .send(new ProducerRecord<>(topic, key, "value-1"))
                        .get();

                kafkaProducer
                        .send(new ProducerRecord<>(topic, key, "value-2"))
                        .get();

                try (Consumer<String, String> kafkaConsumer = newKafkaConsumer()) {
                    kafkaConsumer.subscribe(Collections.singletonList(topic));

                    int counter = 0;
                    Set<TopicPartition> partitions = new HashSet<>();

                    // Consumers poll for new messages at regular intervals

                    while (counter < 2) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                        counter += records.count();

                        partitions.addAll(records.partitions());
                    }

                    assertEquals(1, partitions.size());
                }
            }
        }
    }

    @Test
    public void acceptsUnprocessedRecordsOnRebalancing() throws InterruptedException, ExecutionException {
        String topic = "ds-lab";
        String key = "default-key";

        try (AdminClient adminClient = newAdminClient()) {
            adminClient.createTopics(Collections.singleton(newTopic(topic, 1)));
            
            try (Producer<String, String> kafkaProducer = newKafkaProducer()) {
                kafkaProducer.send(new ProducerRecord<>(topic, key, "value-1"));
                kafkaProducer.send(new ProducerRecord<>(topic, key, "value-2"));
                kafkaProducer.send(new ProducerRecord<>(topic, key, "value-3"));

                Map<String, Long> inboxTable = new HashMap<>();

                Consumer<String, String> kafkaConsumer = newKafkaConsumer();
                Consumer<String, String> kafkaConsumer2 = newKafkaConsumer();

                kafkaConsumer.subscribe(Collections.singletonList(topic));
                kafkaConsumer2.subscribe(Collections.singletonList(topic));

                int counter = 0;
                while (counter < 2) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    Iterator<ConsumerRecord<String, String>> recordItr = records.iterator();

                    while (recordItr.hasNext()) {
                        // Mimic a crash by stopping after processing 2 out of 3 messages
                        if (counter == 2) {
                            break;
                        }

                        ConsumerRecord<String, String> record = recordItr.next();
                        inboxTable.put(record.key(), record.offset());

                        counter++;
                    }
                }

                // Consumer closes without committing any of the proccessed records
                kafkaConsumer.close();

                AtomicInteger applyCounter = new AtomicInteger(0);
                counter = 0;

                while (counter < 3) {
                    ConsumerRecords<String, String> records = kafkaConsumer2.poll(Duration.ofMillis(100));

                    records.forEach(record -> {
                        if (inboxTable.get(record.key()) >= record.offset()) {
                            return;
                        }

                        inboxTable.put(record.key(), record.offset());
                        applyCounter.incrementAndGet();
                    });

                    counter += records.count();
                }

                assertEquals(1, applyCounter.get());

                kafkaConsumer2.close();
            }
        }
    }

    @AfterAll
    public static void afterAll() {
        kafkaContainer.stop();
    }

    private NewTopic newTopic(String topic, int numOfPartitions) {
        return new NewTopic(topic, Optional.of(numOfPartitions), Optional.empty());
    }

    private AdminClient newAdminClient() {
        return AdminClient.create(newCommonProperties());
    }

    private Consumer<String, String> newKafkaConsumer() {
        Properties consumerProperties = newCommonProperties();

        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "ds-lab-consumers");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(consumerProperties);
    }

    private Producer<String, String> newKafkaProducer() {
        Properties producerProperties = newCommonProperties();

        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");

        return new KafkaProducer<>(producerProperties);
    }

    private Properties newCommonProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        return properties;
    }

}
