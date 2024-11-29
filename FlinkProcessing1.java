package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import com.google.gson.Gson;


import java.io.File;
import java.io.StringReader;
import java.util.Collections;
import java.util.Properties;

public class ReadFromKafka {
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        // Check if running in Docker by looking for /.dockerenv file
        boolean inDocker = new File("/.dockerenv").exists();
        // Set Kafka bootstrap servers based on Docker environment
        String bootstrapServers = inDocker ? "kafka:9092" : "localhost:9093"; // CHANGE SECOND IP TO COMMUNICATE WITH OTHER LAPTOPS

        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputTopic = "flights-topic"; // Kafka topic name

        // Configure the Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // Create a data stream from Kafka
        DataStream<String> messageStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );
        messageStream.print();
//        System.out.println(messageStream);
        // Process each message
        messageStream.map(line -> {
            try {
                // Deserialize the JSON line into a Flight object using Gson
                Flight flight = gson.fromJson(line, Flight.class);
                System.out.println("Parsed Flight Object: " + flight.toString());

                // Write the flight data to a topic based on the carrier
                writeToTopic(bootstrapServers, flight.carrier, line);

                return "Message written to topic: " + flight.carrier;
            } catch (Exception e) {
                return "Error processing line: " + line + " - " + e.getMessage();
            }
        }).print();

        // Execute the Flink job
        env.execute("Kafka to Flink to Kafka Example");
    }

    private static Flight parseCSVLine(String line) throws Exception {
        try (StringReader reader = new StringReader(line);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            // Extract the first (and only) record from the CSV parser
            return parser.getRecords().stream()
                    .findFirst()
                    .map(record -> new Flight(new String[]{
                            record.get("id"),
                            record.get("year"),
                            record.get("month"),
                            record.get("day"),
                            record.get("dep_time"),
                            record.get("sched_dep_time"),
                            record.get("dep_delay"),
                            record.get("arr_time"),
                            record.get("sched_arr_time"),
                            record.get("arr_delay"),
                            record.get("carrier"),
                            record.get("flight"),
                            record.get("tailnum"),
                            record.get("origin"),
                            record.get("dest"),
                            record.get("air_time"),
                            record.get("distance"),
                            record.get("hour"),
                            record.get("minute"),
                            record.get("time_hour"),
                            record.get("name")
                    }))
                    .orElseThrow(() -> new IllegalArgumentException("Invalid CSV data: " + line));
        }
    }

    private static void writeToTopic(String bootstrapServers, String carrier, String message) {
        // Configure Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "flights-" + carrier; // Topic name based on carrier

        // Create the topic if it doesn't exist
        createTopicIfNotExists(bootstrapServers, topic);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Create a record to send to the carrier-specific topic
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

            // Send the record
            producer.send(record);
        } catch (Exception e) {
            System.err.println("Error writing to topic: " + e.getMessage());
        }
    }

    private static void createTopicIfNotExists(String bootstrapServers, String topic) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Check if the topic exists
            boolean topicExists = adminClient.listTopics().names().get().contains(topic);

            if (!topicExists) {
                // Create the topic with 1 partition and replication factor of 1
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("Created topic: " + topic);
            }
        } catch (Exception e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }
    }
}
