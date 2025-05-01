package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting Kafka Producer POC");

        String bootstrapServers = System.getenv("KAFKA_BROKERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            log.error("KAFKA_BROKERS environment variable not set.");
            System.exit(1);
        }
        String topicA = System.getenv("TOPIC_A");
        if (topicA == null || topicA.isEmpty()) {
            topicA = "topic-a"; // Default if not set
            log.warn("TOPIC_A environment variable not set, using default: {}", topicA);
        }
        String topicB = System.getenv("TOPIC_B");
        if (topicB == null || topicB.isEmpty()) {
            topicB = "topic-b"; // Default if not set
            log.warn("TOPIC_B environment variable not set, using default: {}", topicB);
        }


        log.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        log.info("Topic A: {}", topicA);
        log.info("Topic B: {}", topicB);

        // 1. Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Optional: Add reliability settings for POC if desired
        // properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // Strongest guarantee
        // properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Prevents duplicates from producer retries


        // 2. Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown, closing producer...");
            producer.flush(); // Flush any buffered records
            producer.close(); // Close the producer
            log.info("Producer closed.");
        }));

        int counter = 0;
        // 3. Send data - asynchronous
        while (true) {
            String key = "id_" + counter;
            String valueA = "Message " + counter + " for Topic A";
            String valueB = "Message " + counter + " for Topic B";

            // Send to Topic A
            ProducerRecord<String, String> recordA = new ProducerRecord<>(topicA, key, valueA);
            producer.send(recordA, (metadata, exception) -> {
                // Executes every time a record is successfully sent or an exception is thrown
                if (exception == null) {
                    log.info("Sent to Topic A -> Key: {}, Partition: {}, Offset: {}, Timestamp: {}",
                            key, metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    log.error("Error while producing to Topic A: ", exception);
                }
            });

            // Send to Topic B
            ProducerRecord<String, String> recordB = new ProducerRecord<>(topicB, key, valueB);
            producer.send(recordB, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Sent to Topic B -> Key: {}, Partition: {}, Offset: {}, Timestamp: {}",
                            key, metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    log.error("Error while producing to Topic B: ", exception);
                }
            });

            counter++;
            Thread.sleep(2000); // Wait 2 seconds between sends
            if (counter > 10000) { // Limit messages for POC run
                log.info("Reached message limit, stopping producer.");
                break;
            }
        }

        // 4. Flush and close producer (will also be called by shutdown hook)
        log.info("Flushing remaining messages...");
        producer.flush();
        log.info("Closing producer...");
        producer.close();
        log.info("Producer finished.");
    }
}