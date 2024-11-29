//package org.example;
//
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.flink.util.Collector;
//
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Properties;
//
//public class ReadromKafka {
//
//    public static void main(String[] args) throws Exception {
//        // Create execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Set Kafka properties
//        Properties kafkaProperties = new Properties();
//        kafkaProperties.setProperty("bootstrap.servers", "kafka:9092");
//        kafkaProperties.setProperty("group.id", "myGroup");
//
//        // Define Kafka topic
//        String topic = "flights-topic";
//
//        // Create Kafka consumer
//        DataStream<String> messageStream = env.addSource(
//                new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaProperties)
//        );
//
//        // Parse CSV messages into FlightData objects
//        DataStream<FlightData> flightDataStream = messageStream
//                .map(line -> {
//                    String[] fields = line.split(",");
//                    return new FlightData(
//                            fields[10], // carrier
//                            fields[6].isEmpty() ? 0 : Integer.parseInt(fields[6]) // dep_delay
//                    );
//                }).returns(FlightData.class);
//
//        // Key by carrier
//        KeyedStream<FlightData, String> keyedStream = flightDataStream.keyBy(flightData -> flightData.carrier);
//
//        // Aggregate to calculate reputation score
//        DataStream<ReputationScore> reputationScores = keyedStream.aggregate(new ReputationScoreAggregator());
//
//        // Collect all reputation scores and rank them
//        reputationScores.flatMap(new RankingFunction()).print();
//
//        // Execute Flink environment
//        env.execute("Kafka to Flink - Reputation Score Calculation");
//    }
//
//    // Flight data model
//    public static class FlightData {
//        public String carrier;
//        public int depDelay;
//
//        public FlightData() {}
//
//        public FlightData(String carrier, int depDelay) {
//            this.carrier = carrier;
//            this.depDelay = depDelay;
//        }
//    }
//
//    // Reputation score model
//    public static class ReputationScore {
//        public String carrier;
//        public double score;
//
//        public ReputationScore() {}
//
//        public ReputationScore(String carrier, double score) {
//            this.carrier = carrier;
//            this.score = score;
//        }
//
//        @Override
//        public String toString() {
//            return "ReputationScore{" +
//                    "carrier='" + carrier + '\'' +
//                    ", score=" + score +
//                    '}';
//        }
//    }
//
//    // Custom aggregator for calculating reputation score
//    public static class ReputationScoreAggregator implements AggregateFunction<FlightData, ScoreAccumulator, ReputationScore> {
//        @Override
//        public ScoreAccumulator createAccumulator() {
//            return new ScoreAccumulator();
//        }
//
//        @Override
//        public ScoreAccumulator add(FlightData value, ScoreAccumulator accumulator) {
//            if (value.depDelay > 0) {
//                accumulator.totalScore -= value.depDelay * 0.1; // Deduct score for delays
//            } else {
//                accumulator.totalScore += 1; // Reward on-time flights
//            }
//            accumulator.flightCount++;
//            accumulator.carrier = value.carrier;
//            return accumulator;
//        }
//
//        @Override
//        public ReputationScore getResult(ScoreAccumulator accumulator) {
//            return new ReputationScore(accumulator.carrier, accumulator.totalScore / accumulator.flightCount);
//        }
//
//        @Override
//        public ScoreAccumulator merge(ScoreAccumulator a, ScoreAccumulator b) {
//            a.totalScore += b.totalScore;
//            a.flightCount += b.flightCount;
//            return a;
//        }
//    }
//
//    // Accumulator for reputation score
//    public static class ScoreAccumulator {
//        public String carrier;
//        public double totalScore = 0;
//        public int flightCount = 0;
//    }
//
//    // Function to rank carriers by reputation score
//    public static class RankingFunction extends RichFlatMapFunction<ReputationScore, String> {
//        private transient ListState<ReputationScore> scoreState;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            ListStateDescriptor<ReputationScore> descriptor = new ListStateDescriptor<>(
//                    "scores",
//                    Types.POJO(ReputationScore.class)
//            );
//            scoreState = getRuntimeContext().getListState(descriptor);
//        }
//
//        @Override
//        public void flatMap(ReputationScore value, Collector<String> out) throws Exception {
//            // Add new value to state
//            scoreState.add(value);
//
//            // Collect all scores and sort them
//            List<ReputationScore> scores = new ArrayList<>();
//            for (ReputationScore score : scoreState.get()) {
//                scores.add(score);
//            }
//
//            // Sort by score descending
//            scores.sort(Comparator.comparingDouble(score -> -score.score));
//
//            // Format and print rankings
//            StringBuilder rankings = new StringBuilder("Airline Reputation Rankings:\n");
//            for (int i = 0; i < scores.size(); i++) {
//                ReputationScore score = scores.get(i);
//                rankings.append(String.format("%d. %s - Score: %.2f\n", i + 1, score.carrier, score.score));
//            }
//
//            out.collect(rankings.toString());
//        }
//}