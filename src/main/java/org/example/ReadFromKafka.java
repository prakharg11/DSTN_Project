package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ReadFromKafka {
    private static final Gson gson = new Gson();
    private static final String masterHost = "10.30.55.88";  // Master Node IP
    private static final int masterPort = 5000;              // Master Node Port
    private static final Map<String, Integer> carrierBlockMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        // Check if running in Docker by looking for /.dockerenv file
        boolean inDocker = new File("/.dockerenv").exists();
        String bootstrapServers = inDocker ? "kafka:9092" : "localhost:9093";

        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputTopic = "flights-topic";

        // Configure Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> messageStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        messageStream.map(line -> {
            try {
                Flight flight = gson.fromJson(line, Flight.class);
                writeFlightToDistributedFS(flight);
                return "Data written for carrier: " + flight.carrier;
            } catch (Exception e) {
                return "Error processing line: " + line + " - " + e.getMessage();
            }
        }).print();

        env.execute("Kafka to Flink to Distributed FS Example");
    }

    private static void writeFlightToDistributedFS(Flight flight) {
        String carrier = flight.carrier;
        int currentBlock = carrierBlockMap.getOrDefault(carrier, 1);
        String blockName = "block" + currentBlock;
        String fileName = carrier + ".txt";  // File named by carrier

        // Serialize flight data to JSON
        String flightData = gson.toJson(flight);

        // Construct the request as a Map
        Map<String, Object> writeRequestMap = new HashMap<>();
        writeRequestMap.put("action", "write");
        writeRequestMap.put("file_name", fileName);
        writeRequestMap.put("blocks", Collections.singletonList(blockName));

        // Create block data map and put it into the main request
        Map<String, String> blockData = new HashMap<>();
        blockData.put(blockName, flightData);
        writeRequestMap.put("block_data", blockData);

        // Serialize the entire write request to JSON
        String writeRequest = gson.toJson(writeRequestMap);

        try (Socket socket = new Socket(masterHost, masterPort);
             OutputStream outputStream = socket.getOutputStream();
             PrintWriter writer = new PrintWriter(outputStream, true)) {

            writer.println(writeRequest);
            System.out.println("Sent data to master: " + writeRequest);

            // Increment the block number for this carrier
            carrierBlockMap.put(carrier, currentBlock + 1);

        } catch (Exception e) {
            System.err.println("Error writing to distributed FS: " + e.getMessage());
        }
    }

}
