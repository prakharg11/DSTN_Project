package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class ReadFromKafka {
    private static final Gson gson = new Gson();
    private static final String masterHost = "10.30.55.88";  // Master Node IP
    private static final int masterPort = 5000;              // Master Node Port
    private static final Map<String, Integer> carrierBlockMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        boolean inDocker = new File("/.dockerenv").exists();
        String bootstrapServers = inDocker ? "kafka:9092" : "10.30.68.61:9093";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputTopic = "flights-topic";
//        carrierBlockMap.put("UA", 10 + 1);
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

//         Query logic after processing
        while(true){
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter the airline carrier code (e.g., 'AA'): ");
            String airline = scanner.nextLine();
            queryFlightData(airline);
        }
    }

    private static void writeFlightToDistributedFS(Flight flight) {
        String carrier = flight.carrier;
        int currentBlock = carrierBlockMap.getOrDefault(carrier, 1);
        String blockName = "block" + currentBlock;
        String fileName = carrier + ".txt";

        String flightData = gson.toJson(flight);
        Map<String, Object> writeRequestMap = new HashMap<>();
        writeRequestMap.put("action", "write");
        writeRequestMap.put("file_name", fileName);
        writeRequestMap.put("blocks", Collections.singletonList(blockName));

        Map<String, String> blockData = new HashMap<>();
        blockData.put(blockName, flightData);
        writeRequestMap.put("block_data", blockData);

        String writeRequest = gson.toJson(writeRequestMap);

        try (Socket socket = new Socket(masterHost, masterPort);
             OutputStream outputStream = socket.getOutputStream();
             PrintWriter writer = new PrintWriter(outputStream, true)) {

            writer.println(writeRequest);
            carrierBlockMap.put(carrier, currentBlock + 1);

        } catch (Exception e) {
            System.err.println("Error writing to distributed FS: " + e.getMessage());
        }
    }

    private static void queryFlightData(String airline) {
        String fileName = airline + ".txt";

        try (Socket socket = new Socket(masterHost, masterPort);
             OutputStream outputStream = socket.getOutputStream();
             PrintWriter writer = new PrintWriter(outputStream, true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send the read request to the master node
            Map<String, String> readRequest = new HashMap<>();
            readRequest.put("action", "read");
            readRequest.put("file_name", fileName);

            writer.println(gson.toJson(readRequest));

            // Read the entire response from the file system
            String response = reader.readLine();
            if (response == null || response.isEmpty()) {
                System.out.println("No data found for airline: " + airline);
                return;
            }

            Map<String, Object> data = gson.fromJson(response, Map.class);

            // Variables to calculate averages
            double totalDepDelay = 0, totalArrDelay = 0, totalAirTime = 0;
            int depCount = 0, arrCount = 0, airTimeCount = 0;

            // Iterate over all flight entries in the response
            for (Object blockData : data.values()) {
                if (blockData instanceof String) {
                    Flight flight = gson.fromJson((String) blockData, Flight.class);
                    System.out.println(flight.toString());
                    double depDelay = parseDouble(flight.dep_delay);
                    double arrDelay = parseDouble(flight.arr_delay);
                    double airTime = parseDouble(flight.air_time);

                    if (depDelay > 0) {
                        totalDepDelay += depDelay;
                        depCount++;
                    }
                    if (arrDelay > 0) {
                        totalArrDelay += arrDelay;
                        arrCount++;
                    }
                    if (airTime > 0) {
                        totalAirTime += airTime;
                        airTimeCount++;
                    }
                }
            }

            // Display results
            System.out.println("Results for Airline: " + airline);
            System.out.printf("Average Departure Delay: %.2f minutes\n", depCount > 0 ? totalDepDelay / depCount : 0);
            System.out.printf("Average Arrival Delay: %.2f minutes\n", arrCount > 0 ? totalArrDelay / arrCount : 0);
            System.out.printf("Average Airtime: %.2f minutes\n", airTimeCount > 0 ? totalAirTime / airTimeCount : 0);

        } catch (Exception e) {
            System.err.println("Error querying flight data: " + e.getMessage());
        }
    }

        private static double parseDouble(String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
}
