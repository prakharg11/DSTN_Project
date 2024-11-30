package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.google.gson.Gson;

public class numFlightsAirport {
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "localhost:9093"; // Adjust for your environment
        String inputTopic = "flights-topic";

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

        // Process each message
        messageStream
                .map(line -> {
                    try {
                        Flight flight = gson.fromJson(line, Flight.class); // Deserialize JSON to Flight
                        return flight.toDelimitedString(); // Convert to delimited string
                    } catch (Exception e) {
                        return "Error processing line: " + line + " - " + e.getMessage(); // Handle errors
                    }
                })
                .filter(delimitedString -> {
                    try {
                        Flight flight = Flight.fromDelimitedString(delimitedString); // Deserialize from delimited string
                        return flight.origin != null && !flight.origin.isEmpty(); // Ensure valid origin
                    } catch (Exception e) {
                        return false; // Handle parsing errors
                    }
                })
                .map(delimitedString -> {
                    Flight flight = Flight.fromDelimitedString(delimitedString); // Deserialize again
                    return "origin:" + flight.origin; // Return the origin as a string
                })
                .keyBy(origin -> origin) // Key by origin airport
                .process(new CountFlightsByOrigin()) // Count flights per origin
                .print(); // Print the output

        // Execute the Flink job
        env.execute("Number of Flights per Origin Airport Graph");
    }

    // Custom KeyedProcessFunction to count flights per origin airport
    public static class CountFlightsByOrigin extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Integer> countState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("count", Types.INT);
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String origin, Context context, Collector<String> out) throws Exception {
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0;
            }
            currentCount++;
            countState.update(currentCount);

            // Extract origin from the string
            String airport = origin.split(":")[1];

            // Update the graph
            FlightGraph.updateGraph(airport, currentCount);

            // Emit the count
            out.collect("Origin Airport: " + airport + ", Total Flights: " + currentCount);
        }
    }
}
