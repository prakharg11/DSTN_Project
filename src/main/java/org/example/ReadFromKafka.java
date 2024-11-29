package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Simple example on how to read with a Kafka consumer
 */
public class ReadFromKafka {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set Kafka properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProperties.setProperty("zookeeper.connect", "zookeeper:2181");
        kafkaProperties.setProperty("group.id", "myGroup");

        // define Kafka topic
        String topic = "flights-topic";

        // create Kafka consumer
        DataStream<String> messageStream = env.addSource(
                new FlinkKafkaConsumer082<>(topic, new SimpleStringSchema(), kafkaProperties)
        );

        // process stream data
        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                return "Kafka and Flink says: " + value;
            }
        }).print();

        // execute Flink environment
        env.execute("Kafka to Flink Stream");
    }
}
