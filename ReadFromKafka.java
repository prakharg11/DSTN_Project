package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class ReadFromKafka {
    public static void main(String[] args) throws Exception {
        // Check if running in Docker by looking for /.dockerenv file
        boolean inDocker = new File("/.dockerenv").exists();

        // Set Kafka bootstrap servers based on Docker environment
        String bootstrapServers = inDocker ? "kafka:9092" : "localhost:9093"; // Internal Docker Kafka or Localhost for external access

        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "flights-topic"; // Kafka topic name

        // Configure the Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers) // Use the determined bootstrap server address
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a data stream from Kafka
        DataStream<String> messageStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Print the messages from Kafka to stdout
        messageStream.print();

        // Execute the Flink job
        env.execute("Flink Kafka Source Example");
    }
}
