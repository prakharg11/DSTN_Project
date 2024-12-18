package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.google.gson.Gson;

public class DelayedFlightsGraph {
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "10.30.68.61:9093"; // TODO: Change this ip address and port number
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
        messageStream.map(line -> {
                    try {
                        Flight flight = gson.fromJson(line, Flight.class);
//                        System.out.println("In first map: "+flight.toDelimitedString());// Deserialize JSON to Flight
                        return flight.toDelimitedString(); // Convert to delimited string
                    } catch (Exception e) {
                        return "Error processing line: " + line + " - " + e.getMessage(); // Handle errors
                    }
                })
                .filter(delimitedString -> {
                    try {
                        Flight flight = Flight.fromDelimitedString(delimitedString);
//                        System.out.println("In filter:"+flight.arr_delay+":");// Deserialize from delimited string
                        return Double.parseDouble(flight.arr_delay) > 0; // Filter flights with delay > 0
                    } catch (NumberFormatException e) {
                        return false; // Handle invalid arr_delay values
                    }
                })
                .map(delimitedString -> {
                    Flight flight = Flight.fromDelimitedString(delimitedString);
//                    System.out.println("In second map: "+flight.carrier); // Deserialize again
                    return flight.carrier; // Extract the carrier field
                })
                .keyBy(carrier -> carrier)
                .process(new UpdateGraphOnEvent())
                .print(); // Print results (optional)


        // Execute the Flink job
        env.execute("Delayed Flights vs Airlines Graph");
    }

    public static class UpdateGraphOnEvent extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("count", Types.INT);
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String carrier, Context context, Collector<String> out) throws Exception {
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0;
            }
            currentCount++;
            countState.update(currentCount);

            // Update the graph periodically or based on a condition (every 10 events as an example)
            if (currentCount % 10 == 0) {
                FlightGraph.updateGraph(carrier, currentCount);
            }
            out.collect("Airline: " + carrier + ", Delayed Flights: " + currentCount);
        }
    }
}
