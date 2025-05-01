package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) {
        log.info("Starting Kafka Consumer POC");

        String bootstrapServers = System.getenv("KAFKA_BROKERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            log.error("KAFKA_BROKERS environment variable not set.");
            System.exit(1);
        }
        String groupId = System.getenv("GROUP_ID");
        if (groupId == null || groupId.isEmpty()) {
            log.error("GROUP_ID environment variable not set.");
            System.exit(1);
        }
        String topicsEnv = System.getenv("TOPICS");
        if (topicsEnv == null || topicsEnv.isEmpty()) {
            log.error("TOPICS environment variable not set.");
            System.exit(1);
        }
        List<String> topics = Arrays.stream(topicsEnv.split(","))
                                      .map(String::trim)
                                      .filter(s -> !s.isEmpty())
                                      .collect(Collectors.toList());

        log.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        log.info("Consumer Group ID: {}", groupId);
        log.info("Subscribing to Topics: {}", topics);


        // 1. Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Options: "earliest" (read from beginning), "latest" (read only new messages), "none" (throw error if no offset)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Optional: Disable auto-commit for more control (manual commit needed then)
        // properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 2. Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown, calling consumer.wakeup()...");
            // Ask the consumer polling loop to exit cleanly
            consumer.wakeup();

            // Join the main thread to allow graceful shutdown in the loop
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for main thread to finish.", e);
                Thread.currentThread().interrupt(); // Re-interrupt thread
            } finally {
                log.info("Consumer closed cleanly after shutdown hook.");
            }
        }));

        try {
            // 3. Subscribe consumer to our topic(s)
            consumer.subscribe(topics);

            // 4. Poll for new data
            while (true) {
                // Poll with a timeout. If no records after timeout, returns an empty collection.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // Wait up to 1 second

                if (records.isEmpty()){
                    // Optional: log if needed, but can be noisy
                    // log.trace("No records received in this poll interval for group {}", groupId);
                    continue; // Go back to polling
                }

                log.info("Received {} records for group {}", records.count(), groupId);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Group: {}, Key: {}, Value: {}, Partition: {}, Offset: {}, Topic: {}",
                            groupId, record.key(), record.value(), record.partition(), record.offset(), record.topic());
                }

                // Optional: Manual commit if auto-commit is disabled
                // consumer.commitAsync((offsets, exception) -> {
                //     if (exception != null) {
                //         log.error("Error committing offsets for group {}: {}", groupId, offsets, exception);
                //     } else {
                //         log.trace("Offsets committed successfully for group {}: {}", groupId, offsets);
                //     }
                // });
            }
        } catch (WakeupException e) {
            // We expect this when shutting down gracefully via consumer.wakeup()
            log.info("Consumer for group {} is starting to shut down (WakeupException caught)", groupId);
        } catch (Exception e) {
            log.error("Unexpected exception in consumer for group {}: ", groupId, e);
        } finally {
            // Close the consumer. This also commits offsets if auto-commit is enabled.
            log.info("Closing consumer for group {}...", groupId);
            consumer.close();
            log.info("Consumer for group {} closed.", groupId);
        }
    }
}